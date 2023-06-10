#ifndef TXN_BOOSTING_LF_HASHTABLE_P
#define TXN_BOOSTING_LF_HASHTABLE_P

// This is a transient transactional boosting LfHashTable, using
// semantic locking running in Montage framework.

#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

#include <iostream>
#include <atomic>
#include <algorithm>
#include <functional>
#include <vector>
#include <utility>
#include <shared_mutex>

#include "HarnessUtils.hpp"
#include "ConcurrentPrimitives.hpp"
#include "RMap.hpp"
// #include "RCUTracker.hpp"
#include "CustomTypes.hpp"
#include "Recoverable.hpp"

template <class K, class V, int idxSize=1000000>
class TxnBoostingLfHashTable : public RMap<K,V>, public Recoverable{
private:
    struct Node;

    struct MarkPtr{
        std::atomic<Node*> ptr;
        MarkPtr(Node* n):ptr(n){};
        MarkPtr():ptr(nullptr){};
    };

    struct Node{
        MarkPtr next;
        K key;
        V val;
        Node(K k, V v, Node* n):
            next(n),key(k),val(v){
            // assert(ds->epochs[pds::EpochSys::tid].ui == NULL_EPOCH);
            };
        ~Node(){
        }

    }__attribute__((aligned(CACHELINE_SIZE)));
    struct LockBucket {
        std::unordered_map<K, std::shared_mutex> table;
        std::shared_mutex lk;

        bool try_lock(K k){
            auto& l = lock_prologue(k);
            lk.unlock_shared();
            bool ret = l.try_lock();
            return ret;
        }
        bool try_lock_shared(K k){
            auto& l = lock_prologue(k);
            lk.unlock_shared();
            bool ret = l.try_lock_shared();
            return ret;
        }
        void unlock(K k){
            lk.lock_shared();
            auto iter = table.find(k);
            assert (iter != table.end());
            iter->second.unlock();
            lk.unlock_shared();
        }
        void unlock_shared(K k){
            lk.lock_shared();
            auto iter = table.find(k);
            assert (iter != table.end());
            iter->second.unlock_shared();
            lk.unlock_shared();
        }
    private:
        std::shared_mutex& lock_prologue(K k){
            lk.lock_shared();
            auto iter = table.find(k);
            if (iter == table.end()) {
                lk.unlock_shared();
                lk.lock();
                (void)table[k];
                iter = table.find(k);
                lk.unlock();
                lk.lock_shared();
            }
            return iter->second;
        }
    }__attribute__((aligned(CACHELINE_SIZE)));

    std::hash<K> hash_fn;
    padded<MarkPtr>* buckets=new padded<MarkPtr>[idxSize]{};
    LockBucket* locks=new LockBucket[idxSize]{};
    bool findNode(MarkPtr* &prev, Node* &curr, Node* &next, K key, int tid);

    // RCUTracker tracker;
    GlobalTestConfig* gtc;

    const uint64_t MARK_MASK = ~0x1;
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
    TxnBoostingLfHashTable(GlobalTestConfig* gtc) : Recoverable(gtc),
        // tracker(gtc->task_num, 100, 1000, true), 
        gtc(gtc) {
    };
    ~TxnBoostingLfHashTable(){};

    void init_thread(GlobalTestConfig* gtc, LocalTestConfig* ltc){
        Recoverable::init_thread(gtc, ltc);
    }
    int recover(bool simulated){
        // TODO(Wentao): consider separate transient Medley from class
        // Recoverable, maybe Composable, and derive Recoverable from
        // Composable, so txnMontage's support to transient and
        // persistent structures is more decoupled.
        assert(0&&"TxnBoostingLfHashTable isn't recoverable!");
        return 0;
    }

    optional<V> get(K key, int tid);
    optional<V> put(K key, V val, int tid);
    bool insert(K key, V val, int tid);
    optional<V> remove(K key, int tid);
    optional<V> replace(K key, V val, int tid);
};

template <class T> 
class TxnBoostingLfHashTableFactory : public RideableFactory{
    Rideable* build(GlobalTestConfig* gtc){
        return new TxnBoostingLfHashTable<T,T>(gtc);
    }
};


//-------Definition----------
template <class K, class V, int idxSize> 
optional<V> TxnBoostingLfHashTable<K,V,idxSize>::get(K key, int tid) {
    TX_OP_SEPARATOR();
    optional<V> res={};
    MarkPtr* prev=nullptr;
    Node* curr;
    Node* next;

    if(is_inside_txn() && !is_during_abort()) {
        size_t idx=hash_fn(key)%idxSize;
        bool locked = locks[idx].try_lock_shared(key);
        if(!locked) {
            _esys->tx_abort();
        }
        addToUnlocks([=]()mutable{
            this->locks[idx].unlock_shared(key);
        });
    }

    // tracker start and end implied in TX_OP_SEPARATOR constructor
    // and destructor
    // tracker.start_op(tid);
    // hold epoch from advancing so that the node we find won't be deleted
    if(findNode(prev,curr,next,key,tid)) {
        // MontageOpHolder _holder(this);
        res=curr->val;//never old see new as we find node before BEGIN_OP
    }
    // tracker.end_op(tid);

    return res;
}

template <class K, class V, int idxSize> 
optional<V> TxnBoostingLfHashTable<K,V,idxSize>::put(K key, V val, int tid) {
    TX_OP_SEPARATOR();

    optional<V> res={};
    Node* tmpNode = nullptr;
    MarkPtr* prev=nullptr;
    Node* curr;
    Node* next;
    tmpNode = new Node(key, val, nullptr);

    if(is_inside_txn() && !is_during_abort()) {
        size_t idx=hash_fn(key)%idxSize;
        bool locked = locks[idx].try_lock(key);
        if(!locked) {
            _esys->tx_abort();
        }
        addToUnlocks([=]()mutable{
            this->locks[idx].unlock(key);
        });
    }
    // Otherwise, no need to do semantic locking

    // tracker.start_op(tid);
    while(true) {
        if(findNode(prev,curr,next,key,tid)) {
            // exists; replace
            tmpNode->next.ptr.store(next); // this don't need undo
            // begin_op();
            res=curr->val;
            // insert tmpNode after cur and mark cur 
            if(curr->next.ptr.compare_exchange_weak(next,setMark(tmpNode))) {
                // end_op();
                // detach cur
                if(prev->ptr.compare_exchange_strong(curr,tmpNode)) {
                    this->tretire(curr);
                } else {
                    findNode(prev,curr,next,key,tid);
                }
                if(is_inside_txn() && !is_during_abort()) {
                    addToUndos([=]() mutable{
                        this->put(key, res.value(), tid);
                    });
                }
                break;
            }
            // abort_op();
        }
        else {
            //does not exist; insert.
            res={};
            tmpNode->next.ptr.store(curr);// this don't need undo, so we use regular store
            // begin_op();
            if(prev->ptr.compare_exchange_weak(curr,tmpNode)) {
                // end_op();
                if(is_inside_txn() && !is_during_abort()) {
                    addToUndos([=]() mutable{
                        this->remove(key, tid);
                    });
                }
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
bool TxnBoostingLfHashTable<K,V,idxSize>::insert(K key, V val, int tid){
    TX_OP_SEPARATOR();

    bool res=false;
    Node* tmpNode = nullptr;
    MarkPtr* prev=nullptr;
    Node* curr;
    Node* next;
    tmpNode = new Node(key, val, nullptr);

    if(is_inside_txn() && !is_during_abort()) {
        size_t idx=hash_fn(key)%idxSize;
        bool locked = locks[idx].try_lock(key);
        if(!locked) {
            _esys->tx_abort();
        }
        addToUnlocks([=]()mutable{
            this->locks[idx].unlock(key);
        });
    }
    // Otherwise, no need to do semantic locking

    // tracker.start_op(tid);
    while(true) {
        if(findNode(prev,curr,next,key,tid)) {
            res=false;
            delete(tmpNode);
            break;
        }
        else {
            //does not exist, insert.
            tmpNode->next.ptr.store(curr);// this don't need undo, so we use regular store
            // begin_op();
            if(prev->ptr.compare_exchange_weak(curr,tmpNode)) {
                if(is_inside_txn() && !is_during_abort()) {
                    addToUndos([=]() mutable{
                        this->remove(key, tid);
                    });
                }
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
optional<V> TxnBoostingLfHashTable<K,V,idxSize>::remove(K key, int tid) {
    TX_OP_SEPARATOR();

    optional<V> res={};
    MarkPtr* prev=nullptr;
    Node* curr;
    Node* next;

    if(is_inside_txn() && !is_during_abort()) {
        size_t idx=hash_fn(key)%idxSize;
        bool locked = locks[idx].try_lock(key);
        if(!locked) {
            _esys->tx_abort();
        }
        addToUnlocks([=]()mutable{
            this->locks[idx].unlock(key);
        });
    }
    // Otherwise, no need to do semantic locking

    // tracker.start_op(tid);
    while(true) {
        if(!findNode(prev,curr,next,key,tid)) {
            res={};
            break;
        }
        // begin_op();
        res=curr->val;
        if(!curr->next.ptr.compare_exchange_weak(next,setMark(next))) {
            // abort_op();
            continue;
        }
        if(is_inside_txn() && !is_during_abort()) {
            addToUndos([=]() mutable{
                this->put(key, res.value(), tid);
            });
        }
        // end_op();
        if(prev->ptr.compare_exchange_strong(curr,next)) {
            this->tretire(curr);
        } else {
            findNode(prev,curr,next,key,tid);
        }

        break;
    }
    // tracker.end_op(tid);

    return res;
}

template <class K, class V, int idxSize> 
optional<V> TxnBoostingLfHashTable<K,V,idxSize>::replace(K key, V val, int tid) {
    TX_OP_SEPARATOR();

    optional<V> res={};
    Node* tmpNode = nullptr;
    MarkPtr* prev=nullptr;
    Node* curr;
    Node* next;
    tmpNode = new Node(key, val, nullptr);

    if(is_inside_txn() && !is_during_abort()) {
        size_t idx=hash_fn(key)%idxSize;
        bool locked = locks[idx].try_lock(key);
        if(!locked) {
            _esys->tx_abort();
        }
        addToUnlocks([=]()mutable{
            this->locks[idx].unlock(key);
        });
    }
    // Otherwise, no need to do semantic locking

    // tracker.start_op(tid);
    while(true){
        if(findNode(prev,curr,next,key,tid)){
            tmpNode->next.ptr.store(next); // this don't need undo
            // begin_op();
            res=curr->val;
            // insert tmpNode after cur and mark cur 
            if(curr->next.ptr.compare_exchange_weak(next,setMark(tmpNode))) {
                // end_op();
                // detach cur
                if(prev->ptr.compare_exchange_strong(curr,tmpNode)) {
                    this->tretire(curr);
                } else {
                    findNode(prev,curr,next,key,tid);
                }
                break;
            }
            // abort_op();
        }
        else{//does not exist
            res={};
            delete(tmpNode);
            break;
        }
    }
    // tracker.end_op(tid);
    // assert(0&&"replace isn't implemented");

    return res;
}

template <class K, class V, int idxSize> 
bool TxnBoostingLfHashTable<K,V,idxSize>::findNode(MarkPtr* &prev, Node* &curr, Node* &next, K key, int tid){
    size_t idx=hash_fn(key)%idxSize;
    while(true){
        bool cmark=false;
        prev=&buckets[idx].ui;
        curr=prev->ptr.load();

        while(true){
            if(getPtr(curr)==nullptr) {
                curr = getPtr(curr);
                next = getPtr(next);
                return false;
            }
            next=getPtr(curr)->next.ptr.load();
            cmark=getMark(next);
            auto ckey=getPtr(curr)->key;
            if(prev->ptr.load()!=getPtr(curr)) break;//retry
            if(!cmark) {
                if(ckey>=key) {
                    curr = getPtr(curr);
                    next = getPtr(next);
                    return ckey==key;
                }
                prev=&(getPtr(curr)->next);
            } else {
                Node* tmp = getPtr(curr);
                bool res = prev->ptr.compare_exchange_strong(
                    tmp,
                    getPtr(next));
                if(res == false) {
                    break;//retry
                } else {
                    tretire(getPtr(curr));
                }
                // Wentao:
                // When we help remove the marked node and the
                // following next node is nullptr, the linearizing
                // load is no longer the load at prev, but
                // at the helping compare_exchange_weak,the read to be
                // validated should be prev and curr at the moment of
                // the compare_exchange_strong.
            }
            curr=next;
        }
    }
}


#endif
