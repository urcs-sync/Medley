/*
 * Copyright 2017-2018
 *   Andreia Correia <andreia.veiga@unine.ch>
 *   Pedro Ramalhete <pramalhe@gmail.com>
 *   Pascal Felber <pascal.felber@unine.ch>
 *   Nachshon Cohen <nachshonc@gmail.com>
 *
 * This work is published under the MIT license. See LICENSE.txt
 */
#ifndef _P_ONEFILE_HASH_TABLE_H_
#define _P_ONEFILE_HASH_TABLE_H_

#include <string>
#include <algorithm>
#include <utility>
#include <functional>

#include "HarnessUtils.hpp"
#include "ConcurrentPrimitives.hpp"
#include "RMap.hpp"
#include "OneFile/OneFilePTMLF.hpp"
/**
 * <h1> A Resizable Hash Map for usage with STMs </h1>
 * TODO
 *
 */
template<typename K, typename V>
class POneFileHashTable : public RMap<K,V> {

private:
    struct Node {
        poflf::tmtype<K>     key;
        poflf::tmtype<V>     val;
        poflf::tmtype<Node*> next {nullptr};
        Node(const K& k, const V& v) : key{k}, val(v) { } // Copy constructor for k
    };

    std::hash<K> hash_fn;
    poflf::tmtype<uint64_t>                     capacity;
    poflf::tmtype<uint64_t>                     sizeHM = 0;
    // static constexpr double                         loadFactor = 0.75;
    poflf::tmtype<poflf::tmtype<Node*>*>        buckets;      // An array of pointers to Nodes


public:
    POneFileHashTable(GlobalTestConfig* gtc) : capacity{1000000} {
        int t = sizeof(poflf::tmtype<uint64_t>);
        poflf::updateTx([&] () {
            buckets = (poflf::tmtype<Node*>*)poflf::tmMalloc(capacity*sizeof(poflf::tmtype<Node*>));
        });
        for (uint64_t i = 0; i < capacity; i++) {
            poflf::updateTx([&] () {
                buckets[i] = nullptr;
            });
        }
    }

    ~POneFileHashTable() {
        for (uint64_t i = 0; i < capacity; i++){
            poflf::updateTx([&] () {
                    Node* node = buckets[i];
                    while (node != nullptr) {
                        Node* next = node->next;
                        poflf::tmDelete(node);
                        node = next;
                    }
            });
        }
        poflf::updateTx([&] () {
            poflf::tmFree(buckets.pload());
        });
    }


    static std::string className() { return poflf::OneFileLF::className() + "-HashMap"; }


    void rebuild() {
        uint64_t newcapacity = 2*capacity;
        poflf::tmtype<Node*>* newbuckets = (poflf::tmtype<Node*>*)poflf::tmMalloc(newcapacity*sizeof(poflf::tmtype<Node*>));
        for (uint64_t i = 0; i < newcapacity; i++) newbuckets[i] = nullptr;
        for (uint64_t i = 0; i < capacity; i++) {
            Node* node = buckets[i];
            while (node!=nullptr) {
                Node* next = node->next;
                auto h = hash_fn(node->key) % newcapacity;
                node->next = newbuckets[h];
                newbuckets[h] = node;
                node = next;
            }
        }
        poflf::tmFree(buckets.pload());
        buckets = newbuckets;
        capacity = newcapacity;
    }


    //
    // Map methods for running the usual tests and benchmarks
    //

    optional<V> replace(K key, V val, int tid) {
        return poflf::updateTx<optional<V>>([&] () {
            optional<V> res = {};
            // if (sizeHM.pload() > capacity.pload()*loadFactor) rebuild();
            auto h = hash_fn(key) % capacity;
            Node* node = buckets[h];
            Node* prev = node;
            while (true) {
                if (node == nullptr) {
                    return res;
                }
                if (key == node->key) {
                    res = node->val;
                    node->val = val;
                    return res;
                }
                prev = node;
                node = node->next;
            }
        });
    }

    optional<V> put(K key, V val, int tid) {
        return poflf::updateTx<optional<V>>([&] () {
            optional<V> res = {};
            // if (sizeHM.pload() > capacity.pload()*loadFactor) rebuild();
            auto h = hash_fn(key) % capacity;
            Node* node = buckets[h];
            Node* prev = node;
            while (true) {
                if (node == nullptr) {
                    Node* newnode = poflf::tmNew<Node>(key, val);
                    if (node == prev) {
                        buckets[h] = newnode;
                    } else {
                        prev->next = newnode;
                    }
                    sizeHM++;
                    return res;  // New insertion
                }
                if (key == node->key) {
                    res = node->val;
                    node->val = val;
                    return res;
                }
                prev = node;
                node = node->next;
            }
        });
    }

    bool insert(K key, V val, int tid) {
        return poflf::updateTx<bool>([&] () {
            // if (sizeHM.pload() > capacity.pload()*loadFactor) rebuild();
            auto h = hash_fn(key) % capacity;
            Node* node = buckets[h];
            Node* prev = node;
            while (true) {
                if (node == nullptr) {
                    Node* newnode = poflf::tmNew<Node>(key, val);
                    if (node == prev) {
                        buckets[h] = newnode;
                    } else {
                        prev->next = newnode;
                    }
                    sizeHM++;
                    return true;  // New insertion
                }
                if (key == node->key) {
                    return false;
                }
                prev = node;
                node = node->next;
            }
        });
    }

    optional<V> remove(K key, int tid) {
        return poflf::updateTx<optional<V>>([&] () {
            optional<V> res = {};
            auto h = hash_fn(key) % capacity;
            Node* node = buckets[h];
            Node* prev = node;
            while (true) {
                if (node == nullptr) return res;
                if (key == node->key) {
                    if (node == prev) {
                        buckets[h] = node->next;
                    } else {
                        prev->next = node->next;
                    }
                    sizeHM--;
                    res = node->val;
                    poflf::tmDelete(node);
                    return res;
                }
                prev = node;
                node = node->next;
            }
        });
    }

    optional<V> get(K key, int tid) {
        return poflf::readTx<optional<V>>([&] () {
            optional<V> res = {};
            auto h = hash_fn(key) % capacity;
            Node* node = buckets[h];
            while (true) {
                if (node == nullptr) return res;
                if (key == node->key) {
                    res = node->val;
                    return res;
                }
                node = node->next;
            }
        });
    }
};

template <class T> 
class POneFileHashTableFactory : public RideableFactory{
    POneFileHashTable<T,T>* s = nullptr;
    Rideable* build(GlobalTestConfig* gtc){
        s = poflf::tmNew<POneFileHashTable<T,T>>(gtc);
        return s;
    }
};


#endif /* _P_ONEFILE_HASH_TABLE_H_ */
