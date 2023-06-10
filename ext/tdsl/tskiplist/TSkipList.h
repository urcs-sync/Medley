#pragma once

#include <optional>
#include "Utils.h"
#include "WriteSet.h"
#include "Node.h"
#include "GVC.h"
#include "Index.h"

namespace tdsl {

class SkipListTransaction
{
public:
    SkipListTransaction() {}

    virtual ~SkipListTransaction() {}

    void clear() {
        is_inside_txn = false;
        readVersion = 0;
        writeVersion = 0;
        readSet.clear();
        writeSet.clear();
        indexTodo.clear();
    }

    static GVC gvc;
    bool is_inside_txn;
    unsigned int readVersion;
    unsigned int writeVersion;
    std::vector<NodeBase *> readSet;
    WriteSet writeSet;
    std::vector<IndexOperation> indexTodo;

    NodeBase * getValidatedValue(NodeBase * node, bool * outDeleted = NULL){
        NodeBase * res = NULL;
        if (node->isLocked()) {
            throw AbortTransactionException();
        }

        if (!writeSet.getValue(node, res, outDeleted)) {
            res = node->next;

            if (outDeleted) {
                *outDeleted = node->deleted;
            }
        }

        if (node->version > readVersion) {
            throw AbortTransactionException();
        }

        if (node->isLocked()) {
            throw AbortTransactionException();
        }

        return res;
    }

    bool validateReadSet(){
        for (auto n : readSet) {
            if (!getValidatedValue(n)) {
                return false;
            }
        }
        return true;
    }

    void TXBegin(){
        clear();
        is_inside_txn = true;
        readVersion = gvc.read();
    }

    void TXCommit(){
        is_inside_txn = false;
        {
            SafeLockList locks;
            if (!writeSet.tryLock(locks)) {
                throw AbortTransactionException();
            }

            if (!validateReadSet()) {
                throw AbortTransactionException();
            }

            writeVersion = gvc.addAndFetch();
            writeSet.update(writeVersion);
        }
        for (auto & op : indexTodo) {
            op.node->update_index(op);
        }
    }
};

template<typename K, typename V>
class SkipList
{
public:
    SkipList() : index(SkipListTransaction::gvc.read()) {}

    virtual ~SkipList() {}

    bool contains(const K & k, SkipListTransaction & transaction){
        Node<K,V> * pred = NULL, *succ = NULL;
        traverseTo(k, transaction, pred, succ);
        return (succ != NULL && succ->key == k);
    }

    std::optional<V> get(const K & k, SkipListTransaction & transaction){
        std::optional<V> res = {};
        Node<K,V> * pred = NULL, *succ = NULL;
        traverseTo(k, transaction, pred, succ);

        if (succ != NULL && succ->key == k) 
            res = succ->val;

        return res;
    }

    bool insert(const K & k, const V & v, SkipListTransaction & transaction){
        Node<K,V> * pred = NULL, *succ = NULL;
        traverseTo(k, transaction, pred, succ);

        if (succ != NULL && succ->key == k) {
            return false;
        }

        Node<K,V> * newNode = new Node<K,V>(this, k, v, transaction.readVersion);
        newNode->next = succ;

        transaction.writeSet.addItem(pred, newNode, false);
        transaction.writeSet.addItem(newNode, NULL, false);
        transaction.indexTodo.push_back(IndexOperation(newNode, OperationType::INSERT));
        return true;
    }
    
    std::optional<V> put(const K & k, const V & v, SkipListTransaction & transaction){
        std::optional<V> res = {};
        Node<K,V> * pred = NULL, *succ = NULL;
        traverseTo(k, transaction, pred, succ);

        Node<K,V> * newNode = new Node<K,V>(this, k, v, transaction.readVersion);
        
        if (succ != NULL && succ->key == k) {
            res = succ->val;
            transaction.readSet.push_back(succ);
            newNode->next = transaction.getValidatedValue( succ);
            transaction.writeSet.addItem(succ, NULL, true);
            transaction.writeSet.addItem(pred, newNode, false);
            transaction.indexTodo.push_back(IndexOperation(succ, OperationType::REMOVE));
        } else {
            newNode->next = succ;
            transaction.writeSet.addItem(pred, newNode, false);
            transaction.writeSet.addItem(newNode, NULL, false);
        }
        transaction.indexTodo.push_back(IndexOperation(newNode, OperationType::INSERT));
        return res;
    }

    std::optional<V> remove_return(const K & k, SkipListTransaction & transaction){
        Node<K,V> * pred = NULL, *succ = NULL;
        std::optional<V> res = {};
        traverseTo(k, transaction, pred, succ);
        
        if (succ == NULL || succ->key != k) {
            return res;
        }
        res = succ->val;
        transaction.readSet.push_back(succ);

        transaction.writeSet.addItem(pred, transaction.getValidatedValue(succ), false);
        transaction.writeSet.addItem(succ, NULL, true);
        transaction.indexTodo.push_back(IndexOperation(succ, OperationType::REMOVE));
        return res;
    }

    bool remove(const K & k, SkipListTransaction & transaction) {
        return (remove_return(k, transaction).has_value());
    }
    
    void traverseTo(const K & k, SkipListTransaction & transaction,
                    Node<K,V> *& pred, Node<K,V> *& succ){
        Node<K,V> * startNode = index.getPrev(k);
        bool deleted = false;
        succ = dynamic_cast<Node<K,V>*>(transaction.getValidatedValue(startNode, &deleted));
        while (startNode->isLocked() || deleted) {
            startNode = index.getPrev(startNode->key);
            succ = dynamic_cast<Node<K,V>*>(transaction.getValidatedValue(startNode, &deleted));
        }

        pred = startNode;
        deleted = false;
        while (succ != NULL && (succ->key < k || deleted)) {
            pred = succ;
            succ = dynamic_cast<Node<K,V>*>(transaction.getValidatedValue(pred, &deleted));
        }
        transaction.readSet.push_back(pred);
    }

    Index<K,V> index;
};

    
} // namespace tdsl