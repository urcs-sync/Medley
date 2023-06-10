#ifndef TXN_BOOSTING_FRASER_CAS_SKIPLIST
#define TXN_BOOSTING_FRASER_CAS_SKIPLIST
/******************************************************************************
 * Skip lists, allowing concurrent update by use of CAS primitives. 
 * 
 * Copyright (c) 2001-2003, K A Fraser
 * 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * * Redistributions of source code must retain the above copyright 
 * notice, this list of conditions and the following disclaimer.
 * 
 * * Redistributions in binary form must reproduce the above copyright 
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * 
 * * The name of the author may not be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR 
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE 
 * DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES 
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */

// The original C code comes from SynchBench at
// https://github.com/gramoli/synchrobench/blob/master/c-cpp/src/skiplists/fraser/skip_cas.c
// We transform it to C++.
// -- Wentao Cai

#include <cassert>
#include <random>
#include <shared_mutex>

#include "HarnessUtils.hpp"
#include "ConcurrentPrimitives.hpp"
#include "RMap.hpp"
#include "RCUTracker.hpp"

template <class K, class V>
class TxnBoostingFraserSkipList : public RMap<K,V>, public Recoverable{
private:
    static constexpr int LEVEL_MASK = 0x0ff;
    static constexpr int READY_FOR_FREE = 0x100;
    static constexpr int NUM_LEVELS = 20;
    static constexpr int idxSize = 1000000;
    enum KeyType { MIN, REAL, MAX };
    struct Node;
    struct NodePtr {
        std::atomic<Node *> ptr;
        NodePtr(Node *n) : ptr(n){};
        NodePtr() : ptr(nullptr){};
    };
    struct VPtr {
        std::atomic<V *> ptr;
        VPtr(V *n) : ptr(n){};
        VPtr() : ptr(nullptr){};
    };
    struct Node{
        std::atomic<int> level;
        KeyType key_type; // 0 min, 1 real val, 2 max
        K key;
        VPtr val;
        // Transient-to-transient pointers
        NodePtr next [NUM_LEVELS];
        Node(K k, V* v, Node *_next, int _level, KeyType _key_type) : 
            level(_level),
            key_type(_key_type), 
            key(k), 
            val(v),
            next{} 
        { 
            for(int i = 0; i < level; i++) 
                next[i].ptr.store(_next, std::memory_order_relaxed);
        }
        Node(Node *_next, int _level, KeyType _key_type) : 
            level(_level),
            key_type(_key_type),
            key(),
            val(nullptr),
            next{} 
        {
            for(int i = 0; i < level; i++) 
                next[i].ptr.store(_next, std::memory_order_relaxed);
        }
        ~Node(){ }
    };
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

    int get_level(int tid) {
        size_t r = rands[tid].ui();
        int l = 1;
        r = (r >> 4) & ((1 << (NUM_LEVELS-1)) - 1);
        while ( (r & 1) ) { l++; r >>= 1; }
        return l;
    }
    Node* get_marked_ref(Node* _p) {
        return ((Node *)(((size_t)(_p)) | 1ULL));
    }
    Node* get_unmarked_ref(Node* _p) {
        return ((Node *)(((size_t)(_p)) & ~1ULL));
    }
    bool is_marked_ref(Node* _p) {
        return (((size_t)(_p)) & 1);
    }

    std::hash<K> hash_fn;
    NodePtr head;
    // RCUTracker tracker;
    GlobalTestConfig* gtc;
    padded<std::mt19937>* rands;
    LockBucket* locks=new LockBucket[idxSize]{};
    Node* strong_search_predecessors(const K& key, Node** pa, Node** na);
    Node* weak_search_predecessors(const K& key, Node** pa, Node** na);
    void mark_deleted(Node* x, int level);
    int check_for_full_delete(Node* x);
    void do_full_delete(Node* x, int level, int tid);
    bool do_update(const K& key, V* val, int tid, optional<V>& res, bool overwrite);

public:
    TxnBoostingFraserSkipList(GlobalTestConfig* gtc) : Recoverable(gtc), 
        gtc(gtc) {
        rands = new padded<std::mt19937>[gtc->task_num];
        for(int i=0;i<gtc->task_num;i++){
            rands[i].ui.seed(i);
        }
        Node* tail = new Node(nullptr, NUM_LEVELS, MAX);
        head.ptr = new Node(tail, NUM_LEVELS, MIN);
    };
    void init_thread(GlobalTestConfig* gtc, LocalTestConfig* ltc){
        Recoverable::init_thread(gtc, ltc);
    }
    int recover(bool simulated){
        // TODO(Wentao): consider separate transient Medley from class
        // Recoverable, maybe Composable, and derive Recoverable from
        // Composable, so txnMontage's support to transient and
        // persistent structures is more decoupled.
        assert(0&&"TxnBoostingTxnBoostingFraserSkipList isn't recoverable!");
        return 0;
    }

    optional<V> get(K key, int tid);
    optional<V> remove(K key, int tid);
    optional<V> put(K key, V val, int tid);
    bool insert(K key, V val, int tid);
    optional<V> replace(K key, V val, int tid);
};

template<class K, class V>
typename TxnBoostingFraserSkipList<K,V>::Node* TxnBoostingFraserSkipList<K,V>::strong_search_predecessors(const K& key, TxnBoostingFraserSkipList<K,V>::Node** pa, TxnBoostingFraserSkipList<K,V>::Node** na)
{
    Node* x;
    Node* x_next;
    Node* y;
    Node* y_next;
    int        i;

 retry:
    x = head.ptr.load();
    for ( i = NUM_LEVELS - 1; i >= 0; i-- )
    {
        /* We start our search at previous level's unmarked predecessor. */
        x_next = x->next[i].ptr.load();
        /* If this pointer's marked, so is @pa[i+1]. May as well retry. */
        if ( is_marked_ref(x_next) ) goto retry;

        for ( y = x_next; ; y = y_next )
        {
            /* Shift over a sequence of marked nodes. */
            for ( ; ; )
            {
                y_next = y->next[i].ptr.load();
                if ( !is_marked_ref(y_next) ) break;
                y = get_unmarked_ref(y_next);
            }

            
            if ( y->key_type != MIN && ( y->key_type == MAX || y->key >= key) ) break;

            /* Update estimate of predecessor at this level. */
            x      = y;
            x_next = y_next;
        }

        /* Swing forward pointer over any marked nodes. */
        if ( x_next != y ) {
            if (!x->next[i].ptr.compare_exchange_strong(x_next, y)) 
                goto retry;
        }

        if ( pa ) pa[i] = x;
        if ( na ) na[i] = y;
    }

    return y;
}

template<class K, class V>
typename TxnBoostingFraserSkipList<K,V>::Node* TxnBoostingFraserSkipList<K,V>::weak_search_predecessors(const K& key, TxnBoostingFraserSkipList<K,V>::Node** pa, TxnBoostingFraserSkipList<K,V>::Node** na)
{
    Node* x;
    Node* x_next;
    int        i;

    x = head.ptr.load();
    for ( i = NUM_LEVELS - 1; i >= 0; i-- )
    {
        for ( ; ; )
        {
            x_next = x->next[i].ptr.load();
            x_next = get_unmarked_ref(x_next);

            if ( x_next->key_type != MIN && ( x_next->key_type == MAX || x_next->key >= key) ) break;

            x = x_next;
        }

        if ( pa ) pa[i] = x;
        if ( na ) na[i] = x_next;
    }

    return x_next;
}

template<class K, class V>
void TxnBoostingFraserSkipList<K,V>::mark_deleted(Node* x, int level)
{
    Node* x_next = nullptr;

    while ( --level >= 0 )
    {
        x_next = x->next[level].ptr.load();
        while ( !is_marked_ref(x_next) )
        {
            x->next[level].ptr.compare_exchange_strong(x_next, get_marked_ref(x_next));
        }
    }
}

template<class K, class V>
int TxnBoostingFraserSkipList<K,V>::check_for_full_delete(Node* x)
{
    int level = x->level.load();
    return ((level & READY_FOR_FREE) ||
            !x->level.compare_exchange_strong(level, level | READY_FOR_FREE));
}

template<class K, class V>
void TxnBoostingFraserSkipList<K,V>::do_full_delete(Node* x, int level, int tid)
{
    (void)strong_search_predecessors(x->key, nullptr, nullptr);
    this->tretire(x);
}

template<class K, class V>
bool TxnBoostingFraserSkipList<K,V>::do_update(const K& key, V* val, int tid, optional<V>& res, bool overwrite)
{
    V*  ov;
    Node* preds[NUM_LEVELS];
    Node* succs[NUM_LEVELS];
    Node* pred;
    Node* succ;
    Node* new_node = nullptr;
    Node* new_next;
    Node* old_next;
    int        i, level, retval;
    bool result = false;

    succ = weak_search_predecessors(key, preds, succs);

 retry:
    ov = nullptr;
    if ( succ->key_type == REAL && succ->key == key )
    {
        /* Already a @key node in the list: update its mapping. */
        ov = succ->val.ptr.load();
        do {
            if ( ov == nullptr )
            {
                /* Finish deleting the node, then retry. */
                level = succ->level.load();
                mark_deleted(succ, level & LEVEL_MASK);
                succ = strong_search_predecessors(key, preds, succs);
                goto retry;
            }
        } while ( overwrite && !succ->val.ptr.compare_exchange_strong(ov, val));

        if ( new_node != nullptr ) delete(new_node);
        res = *ov;
        if (overwrite) {
            result = true;
            if(is_inside_txn() && !is_during_abort()) {
                addToUndos([=]() mutable{
                    this->put(key, res.value(), tid);
                });
            }
            this->tretire(ov);
        } else {
            result = false;
        }
        // goto out;
    } else {
        /* Not in the list, so initialise a new_node node for insertion. */
        if ( new_node == nullptr )
            new_node    = new Node(key, val, nullptr, get_level(tid), REAL);
        level = new_node->level.load();

        /* If successors don't change, this saves us some CAS operations. */
        for ( i = 0; i < level; i++ )
        {
            new_node->next[i].ptr.store(succs[i], std::memory_order_relaxed);
        }

        /* We've committed when we've inserted at level 1. */
        if (!preds[0]->next[0].ptr.compare_exchange_strong(succ, new_node))
        {
            succ = strong_search_predecessors(key, preds, succs);
            goto retry;
        }
        result = true; // inserted
        if(is_inside_txn() && !is_during_abort()) {
            addToUndos([=]() mutable{
                this->remove(key, tid);
            });
        }
        /* Insert at each of the other levels in turn. */
        i = 1;
        while ( i < level )
        {
            pred = preds[i];
            succ = succs[i];

            /* Someone *can* delete @new_node under our feet! */
            new_next = new_node->next[i].ptr.load();
            if ( is_marked_ref(new_next) ) break; // goto success

            /* Ensure forward pointer of new_node node is up to date. */
            if ( new_next != succ )
            {
                old_next = new_next;
                new_node->next[i].ptr.compare_exchange_strong(old_next, succ);
                if ( is_marked_ref(old_next) ) break; // goto success
                assert(old_next == new_next);
            }

            /* Ensure we have unique key values at every level. */
            if ( succ->key_type == REAL && succ->key == key ) {
                (void)strong_search_predecessors(key, preds, succs);
                continue;
            }
            assert(
                (pred->key_type == MIN || (
                    pred->key_type == REAL && pred->key < key)) && 
                (succ->key_type == MAX || (
                    succ->key_type == REAL && succ->key > key)
                ));

            /* Replumb predecessor's forward pointer. */
            if (!pred->next[i].ptr.compare_exchange_strong(succ, new_node))
            {
                (void)strong_search_predecessors(key, preds, succs);
                continue;
            }

            /* Succeeded at this level. */
            i++;
        }

    //  success:
        /* Ensure node is visible at all levels before punting deletion. */
        if ( check_for_full_delete(new_node) )
        {
            do_full_delete(new_node, level - 1, tid);
        }
    }
// out:
    // tracker.end_op(tid);
    return result;
}


template<class K, class V>
optional<V> TxnBoostingFraserSkipList<K,V>::get(K key, int tid)
{
    TX_OP_SEPARATOR();
    optional<V> res = {};
    Node* x;
    V* v = nullptr;
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

    x = weak_search_predecessors(key, nullptr, nullptr);
    if ( x->key_type == REAL && x->key == key ) 
        v = x->val.ptr.load();

    if ( v != nullptr ) res = *v;

    return res;
}

template<class K, class V>
optional<V> TxnBoostingFraserSkipList<K,V>::remove(K key, int tid){
    TX_OP_SEPARATOR();
    optional<V> res = {};
    V* v = nullptr;
    Node* preds[NUM_LEVELS];
    Node* x;
    int level, i;

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

    x = weak_search_predecessors(key, preds, nullptr);

    if ( x->key_type != MIN && ( x->key_type == MAX || x->key > key) ) 
        goto out;
    level = x->level.load();
    level = level & LEVEL_MASK;

    /* Once we've marked the value field, the node is effectively deleted. */
    v = x->val.ptr.load();
    do {
        if ( v == nullptr ) 
            goto out;
    }
    while ( !x->val.ptr.compare_exchange_strong(v, nullptr) );
    res = *v;
    if(is_inside_txn() && !is_during_abort()) {
        addToUndos([=]() mutable{
            this->put(key, res.value(), tid);
        });
    }
    this->tretire(v);

    /* Committed to @x: mark lower-level forward pointers. */
    mark_deleted(x, level);

    /*
     * We must swing predecessors' pointers, or we can end up with
     * an unbounded number of marked but not fully deleted nodes.
     * Doing this creates a bound equal to number of threads in the system.
     * Furthermore, we can't legitimately call 'free_node' until all shared
     * references are gone.
     */
    for ( i = level - 1; i >= 0; i-- )
    {
        Node* tmp_x = x; // failed CAS would modify the first argument
        if (!preds[i]->next[i].ptr.compare_exchange_strong( 
                tmp_x, 
                get_unmarked_ref(x->next[i].ptr.load()))) 
        {
            if ( (i != (level - 1)) || check_for_full_delete(x) )
            {
                do_full_delete(x, i, tid);
            }
            goto out;
        }
    }

    this->tretire(x);

 out:
    // tracker.end_op(tid);
    return res;
}

template<class K, class V>
optional<V> TxnBoostingFraserSkipList<K,V>::put(K key, V val, int tid){
    TX_OP_SEPARATOR();
    optional<V> res = {};
    V* _v = new V(val);
    bool ret = do_update(key, _v, tid, res, true);
    return res;
}

template<class K, class V>
bool TxnBoostingFraserSkipList<K,V>::insert(K key, V val, int tid) {
    TX_OP_SEPARATOR();
    optional<V> res = {};
    V* _v = new V(val);
    if (do_update(key, _v, tid, res, false)){
        return true;
    } else {
        delete(_v);
        return false;
    }
}

template<class K, class V>
optional<V> TxnBoostingFraserSkipList<K,V>::replace(K key, V val, int tid) {
    TX_OP_SEPARATOR();
    optional<V> res = {};
    assert("replace not implemented!");
    return res;
}

template <class T> 
class TxnBoostingFraserSkipListFactory : public RideableFactory{
    Rideable* build(GlobalTestConfig* gtc){
        return new TxnBoostingFraserSkipList<T,T>(gtc);
    }
};
#endif