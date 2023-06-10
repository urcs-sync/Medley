/******************************************************************************
 * Skip lists, allowing concurrent update by use of the STM abstraction.
 * 
 * Copyright (c) 2003, K A Fraser
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

#ifndef ONEFILE_SKIPLIST
#define ONEFILE_SKIPLIST
// STM-based skiplist using Lock-free OneFile
// The source code was from Synchrobench, originally by Fraser
// -- Wentao

#include <cassert>
#include <random>

#include "HarnessUtils.hpp"
#include "ConcurrentPrimitives.hpp"
#include "RMap.hpp"
#include "RCUTracker.hpp"
#include "OneFile/OneFileLF.hpp"

template <class K, class V>
class OneFileSkipList : public RMap<K,V>{
private:
    static constexpr int LEVEL_MASK = 0x0ff;
    static constexpr int READY_FOR_FREE = 0x100;
    static constexpr int NUM_LEVELS = 20;
    enum KeyType { MIN, REAL, MAX };
    struct Node;
    struct NodePtr {
        oflf::tmtype<Node *> ptr;
        NodePtr(Node *n) : ptr(n){};
        NodePtr() : ptr(nullptr){};
    };
    struct Payload : public oflf::tmbase {
        V val;
        Payload() : val(){}
        Payload(const V& v) : val(v) {}
    };
    struct PayloadPtr {
        oflf::tmtype<Payload *> ptr;
        PayloadPtr(Payload *n) : ptr(n){};
        PayloadPtr() : ptr(nullptr){};
    };
    struct Node : public oflf::tmbase {
        oflf::tmtype<int> level;
        KeyType key_type; // 0 min, 1 real val, 2 max
        K key;
        PayloadPtr payload;
        // Transient-to-transient pointers
        NodePtr next [NUM_LEVELS];
        Node(K k, Payload* v, Node *_next, int _level, KeyType _key_type) : 
            level(_level),
            key_type(_key_type), 
            key(k), 
            payload(v),
            next{} 
        { 
            oflf::updateTx([&] () {
                for(int i = 0; i < _level; i++) 
                    next[i].ptr = _next;
            });
        }
        Node(Node *_next, int _level, KeyType _key_type) : 
            level(_level),
            key_type(_key_type),
            key(),
            payload(nullptr),
            next{} 
        {
            oflf::updateTx([&] () {
                for(int i = 0; i < _level; i++) 
                    next[i].ptr = _next;
            });
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

    padded<std::mt19937>* rands;
    alignas(64) NodePtr head;
    RCUTracker tracker;
    Node* search_predecessors(const K& key, Node** pa, Node** na);
    bool do_update(const K& key, Payload* val, int tid, optional<V>& res, bool overwrite);

public:
    OneFileSkipList(GlobalTestConfig* gtc) : tracker(gtc->task_num, 100, 1000, true){ 
        rands = new padded<std::mt19937>[gtc->task_num];
        for(int i=0;i<gtc->task_num;i++){
            rands[i].ui.seed(i);
        }
        Node* tail = oflf::tmNew<Node>(nullptr, NUM_LEVELS, MAX);
        head.ptr = oflf::tmNew<Node>(tail, NUM_LEVELS, MIN);
    };
    optional<V> get(K key, int tid);
    optional<V> remove(K key, int tid);
    optional<V> put(K key, V val, int tid);
    bool insert(K key, V val, int tid);
    optional<V> replace(K key, V val, int tid);
};

template<class K, class V>
typename OneFileSkipList<K,V>::Node* OneFileSkipList<K,V>::search_predecessors(const K& key, OneFileSkipList<K,V>::Node** pa, OneFileSkipList<K,V>::Node** na)
{
    Node* x;
    Node* x_next;
    int        i;

    x = head.ptr;
    for ( i = NUM_LEVELS - 1; i >= 0; i-- )
    {
        for ( ; ; )
        {
            x_next = x->next[i].ptr;
            if ( x_next->key_type != MIN && ( x_next->key_type == MAX || x_next->key >= key) ) break;
            x  = x_next;
        }

        if ( pa ) pa[i] = x;
        if ( na ) na[i] = x_next;
    }

    return x_next;
}

// Should be called inside txn
template<class K, class V>
bool OneFileSkipList<K,V>::do_update(const K& key, Payload* val, int tid, optional<V>& res, bool overwrite)
{
    Payload*  ov;
    Node* preds[NUM_LEVELS];
    Node* succs[NUM_LEVELS];
    Node* p;
    Node* x;
    Node* new_node = nullptr;
    int        i, level, retval;
    bool result = false;
    x = search_predecessors(key, preds, succs);

    if ( x->key_type == REAL && x->key == key )
    {
        /* Already a @key node in the list: update its mapping. */
        if ( overwrite ) {
            ov = x->payload.ptr;
            x->payload.ptr = val;
            res = ov->val;
            result = true;
            oflf::tmDelete(ov);
        }
    } else {
        ov = nullptr;
        new_node = oflf::tmNew<Node>(key, val, nullptr, get_level(tid), REAL);
        level = new_node->level;
        for ( i = 0; i < level; i++ )
        {
            new_node->next[i].ptr = succs[i];
            preds[i]->next[i].ptr = new_node;
        }
        result = true;
    }
    return result;
}


template<class K, class V>
optional<V> OneFileSkipList<K,V>::get(K key, int tid)
{
    optional<V> res = {};
    Node* x;
    Payload* v = nullptr;

    oflf::readTx([&] () {
        x = search_predecessors(key, nullptr, nullptr);
        if ( x->key_type == REAL && x->key == key ) 
            v = x->payload.ptr;
        if ( v != nullptr ) res = v->val;
    });

    return res;
}

template<class K, class V>
optional<V> OneFileSkipList<K,V>::remove(K key, int tid){
    optional<V> res = {};
    Payload* v = nullptr;
    Node* preds[NUM_LEVELS];
    Node* succs[NUM_LEVELS];
    Node* x;
    int level, i;

    oflf::updateTx([&] () {
        x = search_predecessors(key, preds, succs);

        if ( x->key_type == REAL && x->key == key ) 
        {
            v = x->payload.ptr;
            for ( i = 0; i < x->level; i++ )
            {
                preds[i]->next[i].ptr = x->next[i].ptr;
            }
        }
        else 
        {
            v = nullptr;
        }

        if( v != nullptr ) {
            res = v->val;
            oflf::tmDelete(v);
            oflf::tmDelete(succs[0]);
        }
    });
    return res;
}

template<class K, class V>
optional<V> OneFileSkipList<K,V>::put(K key, V val, int tid){
    optional<V> res = {};
    oflf::updateTx([&] () {
        Payload* _v = oflf::tmNew<Payload>(val);
        bool ret = do_update(key, _v, tid, res, true);
    });
    return res;
}

template<class K, class V>
bool OneFileSkipList<K,V>::insert(K key, V val, int tid) {
    optional<V> res = {};
    bool ret = false;
    oflf::updateTx([&] () {
        Payload* _v = oflf::tmNew<Payload>(val);
        if (do_update(key, _v, tid, res, false)){
            ret = true;
        } else {
            oflf::tmDelete(_v);
            ret = false;
        }
    });
    return ret;
}

template<class K, class V>
optional<V> OneFileSkipList<K,V>::replace(K key, V val, int tid) {
    optional<V> res = {};
    assert("replace not implemented!");
    return res;
}

template <class T> 
class OneFileSkipListFactory : public RideableFactory{
    Rideable* build(GlobalTestConfig* gtc){
        return new OneFileSkipList<T,T>(gtc);
    }
};
#endif