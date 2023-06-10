#pragma once

#include <assert.h>
#include <iostream>

#include "Node.h"
#include "Utils.h"
#include "skiplist/skiplist.h"

namespace tdsl{

enum OperationType
{
    REMOVE,
    INSERT,
    CONTAINS,
    PUT,
    GET
};

class IndexOperation
{
public:
    IndexOperation(NodeBase * node, OperationType op) : node(node), op(op) {}

    NodeBase * node;
    OperationType op;
};

template<typename K, typename V>
static int NodeCmp(skiplist_node * a, skiplist_node * b, void *)
{
    typedef Node<K,V> NodeType;
    NodeType * aa, *bb;
    aa = _get_entry(a, NodeType, snode);
    bb = _get_entry(b, NodeType, snode);

    if (aa->type < bb->type) {
        return -1;
    } else if (aa->type > bb->type) {
        return 1;
    } else {
        assert(aa->type == NORMAL && bb->type == NORMAL);
        if (aa->key < bb->key) {
            return -1;
        }
        if (aa->key > bb->key) {
            return 1;
        }
        return 0;
    }
}

template<typename K, typename V>
class Index
{
public:
    Index(unsigned int version) : head(nullptr, K(), version, MIN) {
        skiplist_init(&sl, NodeCmp<K,V>);
        skiplist_insert(&sl, &head.snode);
    }

    void update(IndexOperation op){
        if (op.op == OperationType::REMOVE) {
            if (!remove(op.node)) {
                throw std::runtime_error("Failed during Index update!");
            }
        } else {
            if (!insert(op.node)) {
                throw std::runtime_error("Failed during Index update!");
            }
        }
    }

    bool insert(NodeBase * node){
        skiplist_insert(&sl, &node->snode);
        return true;
    }

    bool remove(NodeBase * node){
        skiplist_erase_node(&sl, &node->snode);
        return true;
    }

    Node<K,V> * getPrev(const K & k){
        typedef Node<K,V> NodeType;
        Node<K,V> query(nullptr, k, 0);
        skiplist_node * cursor = skiplist_find_smaller_or_equal(&sl, &query.snode);
        if (!cursor) {
            throw std::runtime_error("WTF");
        }

        return _get_entry(cursor, NodeType, snode);
    }

    // These methods are purely for test-purposes and are not meant to be used by TDSs.
    // long sum();
    long size(){
        long size = 0;
        Node<K,V> * n = head.next;
        while (n != NULL) {
            size++;
            n = n->next;
        }

        return size;
    }

private:
    Node<K,V> head;
    skiplist_raw sl;
};

}