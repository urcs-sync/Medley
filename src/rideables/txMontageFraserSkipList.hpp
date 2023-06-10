#ifndef TX_MONTAGE_FRASER_CAS_SKIPLIST
#define TX_MONTAGE_FRASER_CAS_SKIPLIST
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
//
// We transform it to C++.
//
// This is the buffered persistent version that uses txMontage.
//
// -- Wentao Cai

#include <cassert>
#include <random>
#include <functional>

#include "HarnessUtils.hpp"
#include "ConcurrentPrimitives.hpp"
#include "RMap.hpp"
// #include "RCUTracker.hpp"
#include "Recoverable.hpp"

template <class K, class V>
class txMontageFraserSkipList : public RMap<K,V>, public Recoverable{
private:
    static constexpr int LEVEL_MASK = 0x0ff;
    static constexpr int READY_FOR_FREE = 0x100;
    static constexpr int NUM_LEVELS = 20;
    enum KeyType { MIN, REAL, MAX };

    class Payload 
    : public pds::PBlk
    {
        GENERATE_FIELD(K, key, Payload);
        GENERATE_FIELD(V, val, Payload);
    public:
        Payload(){}
        Payload(K x, V y): m_key(x), m_val(y){}
        Payload(const Payload& oth): 
            pds::PBlk(oth), 
            m_key(oth.m_key), m_val(oth.m_val){}
        void persist(){}
    }__attribute__((aligned(CACHELINE_SIZE)));

    struct Node;
    struct NodePtr {
        pds::atomic_lin_var<Node *> ptr;
        NodePtr(Node *n) : ptr(n){};
        NodePtr() : ptr(nullptr){};
    };
    struct PayloadPtr {
        pds::atomic_lin_var<Payload *> ptr;
        PayloadPtr(Payload *n) : ptr(n){};
        PayloadPtr() : ptr(nullptr){};
    };
    struct alignas(64) Node{
        std::atomic<int> level;
        KeyType key_type; // 0 min, 1 real val, 2 max
        txMontageFraserSkipList* ds;
        K key;
        PayloadPtr payload;
        // Transient-to-transient pointers
        NodePtr floor_next;
        std::atomic<Node*> next [NUM_LEVELS-1];
        Node(txMontageFraserSkipList* _ds, K k, Payload* _payload, Node *_next, int _level, KeyType _key_type) : 
            level(_level), 
            key_type(_key_type), 
            ds(_ds),
            key(k), 
            payload(_payload), 
            floor_next(),
            next{} 
        { 
            floor_next.ptr.store(ds, _next);
            for(int i = 0; i < level-1; i++) 
                next[i].store(_next);
        }
        Node(txMontageFraserSkipList* _ds, Node *_next, int _level, KeyType _key_type) : 
            level(_level), 
            key_type(_key_type), 
            ds(_ds),
            key(), 
            payload(nullptr), 
            floor_next(),
            next{} 
        {
            floor_next.ptr.store(ds, _next);
            for(int i = 0; i < level-1; i++) 
                next[i].store(_next);
        }
        ~Node(){ }
    };

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

    Node* strong_search_predecessors(const K& key, Node** pa, Node** na);
    Node* weak_search_predecessors(const K& key, Node** pa, Node** na);
    void mark_deleted(Node* x, int level);
    int check_for_full_delete(Node* x);
    void do_full_delete(Node* x, int level, int tid);
    bool do_update(const K& key, Payload* val, int tid, optional<V>& res, bool overwrite);

    GlobalTestConfig* gtc;
    padded<std::mt19937>* rands;
    alignas(64) NodePtr head;
public:
    txMontageFraserSkipList(GlobalTestConfig* gtc) : 
        Recoverable(gtc),
        gtc(gtc),
        head(new Node(this, new Node(this, nullptr, NUM_LEVELS, MAX), NUM_LEVELS, MIN)){ 
        rands = new padded<std::mt19937>[gtc->task_num];
        for(int i=0;i<gtc->task_num;i++){
            rands[i].ui.seed(i);
        }
    };
    void init_thread(GlobalTestConfig* gtc, LocalTestConfig* ltc){
        Recoverable::init_thread(gtc, ltc);
    }
    
    int recover(bool simulated){
        errexit("recover() not implemented!");
        return 0;
    }

    optional<V> get(K key, int tid);
    optional<V> remove(K key, int tid);
    optional<V> put(K key, V val, int tid);
    bool insert(K key, V val, int tid);
    optional<V> replace(K key, V val, int tid);
};

template<class K, class V>
typename txMontageFraserSkipList<K,V>::Node* txMontageFraserSkipList<K,V>::strong_search_predecessors(const K& key, txMontageFraserSkipList<K,V>::Node** pa, txMontageFraserSkipList<K,V>::Node** na)
{
    Node* x = nullptr;
    Node* x_next = nullptr;
    Node* y = nullptr;
    Node* y_next = nullptr;
    int i = 0;

 retry:
    x = head.ptr.nbtc_load(this);
    for ( i = NUM_LEVELS - 1; i >= 0; i-- )
    {
        /* We start our search at previous level's unmarked predecessor. */
        if(i==0)
            x_next = x->floor_next.ptr.nbtc_load(this);
        else 
            x_next = x->next[i-1].load();
        /* If this pointer's marked, so is @pa[i+1]. May as well retry. */
        if ( is_marked_ref(x_next) ) goto retry;

        for ( y = x_next; ; y = y_next )
        {
            /* Shift over a sequence of marked nodes. */
            for ( ; ; )
            {
                if(i==0)
                    y_next = y->floor_next.ptr.nbtc_load(this);
                else
                    y_next = y->next[i-1].load();
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
            if(i==0) {
                if (!x->floor_next.ptr.nbtc_CAS(this, x_next, y, false, false)) 
                    goto retry;
            } else {
                if (!x->next[i-1].compare_exchange_strong(x_next, y)) 
                    goto retry;
            }
        }

        if ( pa ) pa[i] = x;
        if ( na ) na[i] = y;
    }

    return y;
}

template<class K, class V>
typename txMontageFraserSkipList<K,V>::Node* txMontageFraserSkipList<K,V>::weak_search_predecessors(const K& key, txMontageFraserSkipList<K,V>::Node** pa, txMontageFraserSkipList<K,V>::Node** na)
{
    Node* x = nullptr;
    Node* x_next = nullptr;
    Node* ox_next = nullptr;
    int i = 0;

    x = head.ptr.nbtc_load(this);
    for ( i = NUM_LEVELS - 1; i >= 0; i-- )
    {
        for ( ; ; )
        {
            if(i==0)
                ox_next = x->floor_next.ptr.nbtc_load(this);
            else 
                ox_next = x->next[i-1].load();
            x_next = get_unmarked_ref(ox_next);

            if ( x_next->key_type != MIN && ( x_next->key_type == MAX || x_next->key >= key) ) break;

            x = x_next;
        }

        if ( pa ) pa[i] = x;
        if ( na ) na[i] = x_next;
    }

    return ox_next;
}

template<class K, class V>
void txMontageFraserSkipList<K,V>::mark_deleted(Node* x, int level)
{
    Node* x_next = nullptr;

    while ( --level >= 0 )
    {
        if (level == 0)
            x_next = x->floor_next.ptr.nbtc_load(this);
        else
            x_next = x->next[level-1].load();
        while ( !is_marked_ref(x_next) )
        {
            if (level == 0) {
                if (x->floor_next.ptr.nbtc_CAS(this, x_next, get_marked_ref(x_next), false, false)) break;
                x_next = x->floor_next.ptr.nbtc_load(this);
            } else {
                if (x->next[level-1].compare_exchange_strong(x_next, get_marked_ref(x_next))) break;
                x_next = x->next[level-1].load();
            }
        }
    }
}

template<class K, class V>
int txMontageFraserSkipList<K,V>::check_for_full_delete(Node* x)
{
    // This function is called only as a cleanup, so level field can
    // be plain std::atomic.
    int level = x->level.load();
    return ((level & READY_FOR_FREE) ||
            !x->level.compare_exchange_strong(level, level | READY_FOR_FREE));
}

template<class K, class V>
void txMontageFraserSkipList<K,V>::do_full_delete(Node* x, int level, int tid)
{
    (void)strong_search_predecessors(x->key, nullptr, nullptr);
    this->tretire(x);
}

template<class K, class V>
bool txMontageFraserSkipList<K,V>::do_update(const K& key, Payload* val, int tid, optional<V>& res, bool overwrite)
{
    Payload*  ov = nullptr;
    Node* preds[NUM_LEVELS] = {nullptr};
    Node* succs[NUM_LEVELS] = {nullptr};
    Node* pred = nullptr;
    Node* succ = nullptr;
    Node* osucc = nullptr;
    Node* new_node = nullptr;
    Node* new_next = nullptr;
    Node* old_next = nullptr;
    int i=0, level=0;
    bool result = false;
    int cas_ret = 0; // nbtc_CAS return value

    osucc = weak_search_predecessors(key, preds, succs);
    succ = get_unmarked_ref(osucc);
 retry:
    ov = nullptr;
    if ( succ->key_type == REAL && succ->key == key )
    {
        /* Already a @key node in the list: update its mapping. */
        do {
            ov = succ->payload.ptr.nbtc_load(this);
            if ( ov == nullptr )
            {
                /* Finish deleting the node, then retry. */
                level = succ->level.load();
                mark_deleted(succ, level & LEVEL_MASK);
                succ = strong_search_predecessors(key, preds, succs);
                goto retry;
            }
            if (overwrite) {
                if (!is_inside_txn()) this->pretire(ov);
                cas_ret = succ->payload.ptr.nbtc_CAS(this, ov, val, true, true); //lin cas if succeeds
            }
        } while ( overwrite && !cas_ret );

        if ( new_node != nullptr ) tdelete(new_node);
        res = ov->get_unsafe_val(this);
        if (overwrite) {
            result = true;
            if (is_inside_txn()) this->pretire(ov);
            if (cas_ret == 1)
                this->tretire(ov); // this will auto invoke preclaim
            else 
                this->txn_tretire(ov);
        } else {
            addToReadSet(&(succ->payload.ptr), ov);
            result = false;
        }
        // goto out;
    } else {
        /* Not in the list, so initialise a new_node node for insertion. */
        if ( new_node == nullptr )
            new_node    = tnew<Node>(this, key, val, nullptr, get_level(tid), REAL);
        level = new_node->level.load();

        /* If successors don't change, this saves us some CAS operations. */
        new_node->floor_next.ptr.store(this, succs[0]);
        for ( i = 0; i < level-1; i++ )
        {
            new_node->next[i].store(succs[i+1]);
        }

        /* We've committed when we've inserted at level 1. */
        if (!preds[0]->floor_next.ptr.nbtc_CAS(this, succ, new_node, true, true))//lin cas if succeeds
        {
            succ = strong_search_predecessors(key, preds, succs);
            goto retry;
        }
        result = true; // inserted

        /* Insert at each of the other levels in turn. */
        auto cleanup = [=]()mutable{
            i = 1;
            while ( i < level )
            {
                pred = preds[i];
                succ = succs[i];

                /* Someone *can* delete @new_node under our feet! */
                new_next = new_node->next[i-1].load();
                if ( is_marked_ref(new_next) ) break; // goto success

                /* Ensure forward pointer of new_node node is up to date. */
                if ( new_next != succ )
                {
                    old_next = new_next;
                    new_node->next[i-1].compare_exchange_strong(old_next, succ);
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
                if (!pred->next[i-1].compare_exchange_strong(succ, new_node))
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
        };
        if (is_inside_txn()){
            addToCleanups(cleanup); // if inside txn, cleanup only after lin CAS succeeds.
        } else {
            cleanup(); // execute cleanup in place
        }
    }
// out:
    return result;
}


template<class K, class V>
optional<V> txMontageFraserSkipList<K,V>::get(K key, int tid)
{
    TX_OP_SEPARATOR();
    optional<V> res = {};
    Node* x = nullptr;
    Node* ox = nullptr;
    Node* preds[NUM_LEVELS] = {nullptr};
    Payload* v = nullptr;

    ox = weak_search_predecessors(key, preds, nullptr);
    x = get_unmarked_ref(ox);
    if ( x->key_type == REAL && x->key == key ) {
        v = x->payload.ptr.nbtc_load(this);
        addToReadSet(&(x->payload.ptr), v);
    } else {
        addToReadSet(&(preds[0]->floor_next.ptr), ox);
    }

    if ( v != nullptr ) res = v->get_unsafe_val(this);

    return res;
}

template<class K, class V>
optional<V> txMontageFraserSkipList<K,V>::remove(K key, int tid){
    TX_OP_SEPARATOR();
    optional<V> res = {};
    Payload* v = nullptr;
    Node* preds[NUM_LEVELS] = {nullptr};
    Node* x = nullptr;
    Node* ox = nullptr;
    int level=0, i=0;

    ox = weak_search_predecessors(key, preds, nullptr);
    x = get_unmarked_ref(ox);

    if ( x->key_type != MIN && ( x->key_type == MAX || x->key > key) ) 
        return res;
    level = x->level.load();
    level = level & LEVEL_MASK;

    /* Once we've marked the value field, the node is effectively deleted. */
    do {
        v = x->payload.ptr.nbtc_load(this);
        if ( v == nullptr ) {
            // doesn't exist; previous load becomes lin point
            addToReadSet(&(x->payload.ptr), v);
            return res;
        }
    }
    while ( !x->payload.ptr.nbtc_CAS(this, v, nullptr, true, true) );//lin cas if succeeds
    res = v->get_unsafe_val(this);
    
    auto cleanup = [=]()mutable{
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
            bool ret = false;
            if(i==0) {
                ret = preds[i]->floor_next.ptr.CAS(this, 
                    tmp_x, 
                    get_unmarked_ref(x->floor_next.ptr.load(this)));
            } else {
                ret = preds[i]->next[i-1].compare_exchange_strong( 
                    tmp_x, 
                    get_unmarked_ref(x->next[i-1].load()));
            }
            if (!ret) 
            {
                if ( (i != (level - 1)) || check_for_full_delete(x) )
                {
                    do_full_delete(x, i, tid);
                }
                break;
            }
        }
        // retire only if we successfully detach @x from the lowest level
        if(i == -1)
            this->tretire(x);
    };
    return res;
}

template<class K, class V>
optional<V> txMontageFraserSkipList<K,V>::put(K key, V val, int tid){
    TX_OP_SEPARATOR();
    optional<V> res = {};
    // XXX(wentao): this tnew automatically detects whether the type
    // is derived from PBlk, and if yes it will invoke pnew/preclaim
    // instead of new/delete.
    Payload* _v = tnew<Payload>(key, val);
    bool ret = do_update(key, _v, tid, res, true);
    return res;
}

template<class K, class V>
bool txMontageFraserSkipList<K,V>::insert(K key, V val, int tid) {
    TX_OP_SEPARATOR();
    optional<V> res = {};
    Payload* _v = tnew<Payload>(key, val);
    if (do_update(key, _v, tid, res, false)){
        return true;
    } else {
        tdelete(_v);
        return false;
    }
}

template<class K, class V>
optional<V> txMontageFraserSkipList<K,V>::replace(K key, V val, int tid) {
    optional<V> res = {};
    assert("replace not implemented!");
    return res;
}

template <class T> 
class txMontageFraserSkipListFactory : public RideableFactory{
    Rideable* build(GlobalTestConfig* gtc){
        return new txMontageFraserSkipList<T,T>(gtc);
    }
};

/* Specialization for strings */
#include <string>
#include "InPlaceString.hpp"
template <>
class txMontageFraserSkipList<std::string, std::string>::Payload : public pds::PBlk{
    GENERATE_FIELD(pds::InPlaceString<TESTS_KEY_SIZE>, key, Payload);
    GENERATE_FIELD(pds::InPlaceString<TESTS_VAL_SIZE>, val, Payload);

public:
    Payload(std::string k, std::string v) : m_key(this, k), m_val(this, v){}
    Payload(const Payload& oth) : pds::PBlk(oth), m_key(this, oth.m_key), m_val(this, oth.m_val){}
    void persist(){}
};

#endif
