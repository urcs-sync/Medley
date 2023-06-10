#ifndef MEDLEY_LF_HASHTABLE_P
#define MEDLEY_LF_HASHTABLE_P

// This is a transient version of txMontageLfHashTable, i.e.,
// transactional lf hash table built with txMontage but without
// persistent payloads.

#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

#include <iostream>
#include <atomic>
#include <algorithm>
#include <functional>
#include <vector>
#include <utility>

#include "HarnessUtils.hpp"
#include "ConcurrentPrimitives.hpp"
#include "RMap.hpp"
// #include "RCUTracker.hpp"
#include "CustomTypes.hpp"
#include "Recoverable.hpp"

template <class K, class V, int idxSize=1000000>
class MedleyLfHashTable : public RMap<K,V>, public Recoverable{
private:
    struct Node;

    struct MarkPtr{
        pds::atomic_lin_var<Node*> ptr;
        MarkPtr(Node* n):ptr(n){};
        MarkPtr():ptr(nullptr){};
    };

    struct Node{
        MedleyLfHashTable* ds;
        K key;
        V val;
        MarkPtr next;
        Node(MedleyLfHashTable* ds_, K k, V v, Node* n):
            ds(ds_),key(k),val(v),next(n){
            // assert(ds->epochs[pds::EpochSys::tid].ui == NULL_EPOCH);
            };
        ~Node(){
        }

    }__attribute__((aligned(CACHELINE_SIZE)));
    std::hash<K> hash_fn;
    padded<MarkPtr>* buckets=new padded<MarkPtr>[idxSize]{};
    bool findNode(MarkPtr* &prev, Node* &curr, Node* &next, K key, int tid);

    // RCUTracker tracker;
    // GlobalTestConfig* gtc;

    static constexpr uint64_t MARK_MASK = ~0x1;
    inline Node* getPtr(Node* d){
        return reinterpret_cast<Node*>((uint64_t)d & MARK_MASK);
    }
    inline bool getMark(Node* d){
        return (bool)((uint64_t)d & 1);
    }
    inline Node* mixPtrMark(Node* d, bool mk){
        return reinterpret_cast<Node*>((uint64_t)d | mk);
    }
    inline Node* setMark(Node* d){
        return reinterpret_cast<Node*>((uint64_t)d | 1);
    }
public:
    MedleyLfHashTable(GlobalTestConfig* gtc) : Recoverable(gtc){
        // tracker(gtc->task_num, 100, 1000, true), 
        // gtc(gtc) {
    };
    ~MedleyLfHashTable(){};

    void init_thread(GlobalTestConfig* gtc, LocalTestConfig* ltc){
        Recoverable::init_thread(gtc, ltc);
    }
    void clear(){
        //single-threaded; for recovery test only
        for (uint64_t i = 0; i < idxSize; i++){
            Node* curr = buckets[i].ui.ptr.load(this);
            Node* next = nullptr;
            while(curr){
                next = curr->next.ptr.load(this);
                delete curr;
                curr = next;
            }
            buckets[i].ui.ptr.store(this,nullptr);
        }
    }
    int recover(bool simulated){
        // TODO(Wentao): consider separate transient Medley from class
        // Recoverable, maybe Composable, and derive Recoverable from
        // Composable, so txMontage's support to transient and
        // persistent structures is more decoupled.
        assert(0&&"MedleyLfHashTable isn't recoverable!");
        return 0;
    }

    optional<V> get(K key, int tid);
    optional<V> put(K key, V val, int tid);
    bool insert(K key, V val, int tid);
    optional<V> remove(K key, int tid);
    optional<V> replace(K key, V val, int tid);
};

template <class T> 
class MedleyLfHashTableFactory : public RideableFactory{
    Rideable* build(GlobalTestConfig* gtc){
        return new MedleyLfHashTable<T,T>(gtc);
    }
};


//-------Definition----------
template <class K, class V, int idxSize> 
optional<V> MedleyLfHashTable<K,V,idxSize>::get(K key, int tid) {
    TX_OP_SEPARATOR();
    optional<V> res={};
    MarkPtr* prev=nullptr;
    Node* curr;
    Node* next;

    // tracker start and end implied in TX_OP_SEPARATOR constructor
    // and destructor
    // tracker.start_op(tid);
    // hold epoch from advancing so that the node we find won't be deleted
    if(findNode(prev,curr,next,key,tid)) {
        // MontageOpHolder _holder(this);
        res=curr->val;//never old see new as we find node before BEGIN_OP
    }
    addToReadSet(&(prev->ptr), curr);

    // tracker.end_op(tid);

    return res;
}

template <class K, class V, int idxSize> 
optional<V> MedleyLfHashTable<K,V,idxSize>::put(K key, V val, int tid) {
    TX_OP_SEPARATOR();

    optional<V> res={};
    Node* tmpNode = nullptr;
    MarkPtr* prev=nullptr;
    Node* curr;
    Node* next;
    tmpNode = tnew<Node>(this, key, val, nullptr);

    // tracker.start_op(tid);
    while(true) {
        if(findNode(prev,curr,next,key,tid)) {
            // exists; replace
            tmpNode->next.ptr.store(this,next); // this don't need undo
            // begin_op();
            res=curr->val;
            // insert tmpNode after cur and mark cur 
            if(curr->next.ptr.nbtc_CAS(this,next,setMark(tmpNode), true, true)) {
                // end_op();
                // detach cur
                auto cleanup = [=]()mutable{
                    if(prev->ptr.CAS(this,curr,tmpNode)) {
                        this->tretire(curr);
                    } else {
                        this->findNode(prev,curr,next,key,tid);
                    }
                };
                if (is_inside_txn()) {
                    addToCleanups(cleanup);
                } else {
                    cleanup();//execute cleanup in place
                }
                break;
            }
            // abort_op();
        }
        else {
            //does not exist; insert.
            res={};
            tmpNode->next.ptr.store(this,curr);// this don't need undo, so we use regular store
            // begin_op();
            if(prev->ptr.nbtc_CAS(this,curr,tmpNode,true,true)) {
                // end_op();
                break;
            }
            // abort_op();
        }
    }
    // tracker.end_op(tid);
    // assert(0&&"put isn't implemented");
    return res;
}

template <class K, class V, int idxSize> 
bool MedleyLfHashTable<K,V,idxSize>::insert(K key, V val, int tid){
    TX_OP_SEPARATOR();

    bool res=false;
    Node* tmpNode = nullptr;
    MarkPtr* prev=nullptr;
    Node* curr;
    Node* next;
    tmpNode = tnew<Node>(this, key, val, nullptr);

    // tracker.start_op(tid);
    while(true) {
        if(findNode(prev,curr,next,key,tid)) {
            addToReadSet(&(prev->ptr), curr);
            res=false;
            tdelete(tmpNode);
            break;
        }
        else {
            //does not exist, insert.
            tmpNode->next.ptr.store(this,curr);// this don't need undo, so we use regular store
            // begin_op();
            if(prev->ptr.nbtc_CAS(this,curr,tmpNode,true,true)) {
                // end_op();
                res=true;
                break;
            }
            // abort_op();
        }
    }
    // tracker.end_op(tid);

    return res;
}

template <class K, class V, int idxSize> 
optional<V> MedleyLfHashTable<K,V,idxSize>::remove(K key, int tid) {
    TX_OP_SEPARATOR();

    optional<V> res={};
    MarkPtr* prev=nullptr;
    Node* curr;
    Node* next;

    // tracker.start_op(tid);
    while(true) {
        if(!findNode(prev,curr,next,key,tid)) {
            addToReadSet(&(prev->ptr), curr);
            res={};
            break;
        }
        // begin_op();
        res=curr->val;
        if(!curr->next.ptr.nbtc_CAS(this,next,setMark(next),true,true)) {
            // abort_op();
            continue;
        }
        // end_op();
        auto cleanup = [=]()mutable{
            if(prev->ptr.CAS(this,curr,next)) {
                this->tretire(curr);
            } else {
                this->findNode(prev,curr,next,key,tid);
            }
        };
        if (is_inside_txn()) {
            addToCleanups(cleanup);
        } else {
            cleanup();//execute cleanup in place
        }

        break;
    }
    // tracker.end_op(tid);

    return res;
}

template <class K, class V, int idxSize> 
optional<V> MedleyLfHashTable<K,V,idxSize>::replace(K key, V val, int tid) {
    TX_OP_SEPARATOR();

    optional<V> res={};
    Node* tmpNode = nullptr;
    MarkPtr* prev=nullptr;
    Node* curr;
    Node* next;
    tmpNode = tnew<Node>(this, key, val, nullptr);

    // tracker.start_op(tid);
    while(true){
        if(findNode(prev,curr,next,key,tid)){
            tmpNode->next.ptr.store(this,next); // this don't need undo
            // begin_op();
            res=curr->val;
            // insert tmpNode after cur and mark cur 
            if(curr->next.ptr.nbtc_CAS(this,next,setMark(tmpNode), true, true)) {
                // end_op();
                // detach cur
                auto cleanup = [=]()mutable{
                    if(prev->ptr.CAS(this,curr,tmpNode)) {
                        this->tretire(curr);
                    } else {
                        this->findNode(prev,curr,next,key,tid);
                    }
                };
                if (is_inside_txn()) {
                    addToCleanups(cleanup);
                } else {
                    cleanup();//execute cleanup in place
                }
                break;
            }
            // abort_op();
        }
        else{//does not exist
            addToReadSet(&(prev->ptr), curr);
            res={};
            tdelete(tmpNode);
            break;
        }
    }
    // tracker.end_op(tid);
    // assert(0&&"replace isn't implemented");

    return res;
}

template <class K, class V, int idxSize> 
bool MedleyLfHashTable<K,V,idxSize>::findNode(MarkPtr* &prev, Node* &curr, Node* &next, K key, int tid){
    size_t idx=hash_fn(key)%idxSize;
    while(true){
        bool cmark=false;
        prev=&buckets[idx].ui;
        curr=prev->ptr.nbtc_load(this);

        while(true){
            if(getPtr(curr)==nullptr) {
                curr = getPtr(curr);
                next = getPtr(next);
                return false;
            }
            next=getPtr(curr)->next.ptr.nbtc_load(this);
            cmark=getMark(next);
            auto ckey=getPtr(curr)->key;
            if(prev->ptr.nbtc_load(this)!=getPtr(curr)) break;//retry
            if(!cmark) {
                if(ckey>=key) {
                    curr = getPtr(curr);
                    next = getPtr(next);
                    return ckey==key;
                }
                prev=&(getPtr(curr)->next);
            } else {
                int res = prev->ptr.nbtc_CAS(
                    this,
                    getPtr(curr),
                    getPtr(next), 
                    false, 
                    false);
                if(res == 0) {
                    break;//retry
                } else {
                    if (res == 1) // real succeeded CAS
                        tretire(getPtr(curr));
                    else // speculative succeeded CAS
                        txn_tretire(getPtr(curr));
                }
                // Wentao:
                // When we help remove the marked node and the
                // following next node is nullptr, the linearizing
                // load is no longer the load at prev, but
                // at the helping nbtc_CAS, and the read to be
                // validated should be prev and curr at the moment of
                // the CAS.
            }
            curr=next;
        }
    }
}


#endif
