// This is a lock-free hash table designed for holding read write
// entries for nbtc.
// 
// The backbone is Michael's hash table, but nodes are allocated from
// a fixed-sized slab (array) for traversal and better locality.
// 
// Slabs are managed as a chained list of arrays, each of which
// contains fixed number of items and each of whose last item is a
// pointer to the next array. 
// Every slab/array contains a watermark indicating up to where this
// array is consumed. 
// 
// Each item has an `active` bit, indicating whether this item is in
// use or not. It's updated when an item is allocated and inserted in
// or when an item is removed from the hash table. 
// By assuming removal is rare, this won't affect traversal by much.
// Also, since seeing an removed item is benign for a helper thread,
// we only need to ensure the ordering of updates but no atomicity.

#ifndef LOCKFREE_SLAB_HASHTABLE_HPP
#define LOCKFREE_SLAB_HASHTABLE_HPP

#include <stdlib.h>

#include <atomic>
#include <array>
#include <optional>

#include "ConcurrentPrimitives.hpp"
#include "RCUTracker.hpp"

// amount: based on jemalloc's size class, we choose
// 127 for read set (4096B size class)
// 102 for write set (4096B size class)
template <class K, class V, int amount=102>
class LockfreeSlabHashTable{
public:
    struct Node;
    struct ItemIterator;
protected:
    struct Slab{
        std::atomic<int> in_use_amount;
        std::atomic<Slab*> next;
        std::array<Node, amount> slab;
        Slab() : in_use_amount(0), next(nullptr), slab() { }
        Node* alloc_new_node(){
            //only called by owner
            assert(in_use_amount.load()<amount);
            int in_use = in_use_amount.load();
            Node* ret = &slab[in_use];
            return ret;
        }
        void alloc_finalize(Node* n){
            int in_use = in_use_amount.load();
            assert(in_use>=0 && in_use<amount && n==&slab[in_use]);
            in_use_amount.store(in_use+1);
        }
        void reset(){
            in_use_amount.store(0);
        }
    };
    struct SlabAllocator{
        std::atomic<Slab*> first;
        // last is only accessed by owner thread
        Slab* last;
        template <typename... Types> 
        Node* allocate(Types... args){
            // only called by owner thread
            Slab* old_last = last;
            int in_use = old_last->in_use_amount.load();
            if(in_use == amount){
                // the last slab is already full; allocate new slab
                Slab* new_last = old_last->next.load();
                if (new_last==nullptr){
                    new_last = new Slab();
                    old_last->next.store(new_last);
                }
                assert(new_last->in_use_amount.load()==0);
                last = new_last;
            }
            Node* ret = last->alloc_new_node();
            new (ret) Node(args...);
            std::atomic_thread_fence(std::memory_order_release);
            // Wentao: we need release fence to ensure ordering with
            // entry load and tid_sn load from helper threads, and
            // tid_sn store from this owner thread. See more details
            // in sc_desc_t::helper_uninstall_desc().
            return ret;
        }
        void allocate_finalize(Node* n){
            // Wentao: this allocate/finalize model is better than
            // allocate/deallocate model in our use case, because this
            // delays the time when helper may traverse and access a
            // just allocated object until it's fully ready. 
            // Given that a descriptor is installed after adding to
            // write set, helper thread missing one just allocated
            // object during an early traversal is benign.
            last->alloc_finalize(n);
        }

        void reset(){
            Slab* c = first.load();
            last = c;
            Slab* n = nullptr;
            while(c!=nullptr){
                int in_use = c->in_use_amount.load();
                c->reset();
                if(in_use<amount) break;
                n = c->next.load();
                c = n;
            }
        }

        bool empty() const {
            // only called by owner thread
            Slab* c = first.load();
            return c == nullptr || c->in_use_amount.load() == 0;
        }

        ItemIterator begin() const{
            // called by any thread
            return ItemIterator(0, first.load());
        }
        SlabAllocator(): first(nullptr), last(nullptr){ 
            Slab* n = new Slab();
            first.store(n);
            last = n;
        }
        ~SlabAllocator(){
            Slab* c = first.load();
            Slab* n = nullptr;
            while(c!=nullptr){
                n = c->next.load();
                delete(c);
                c = n;
            }
        }
    };

    struct MarkPtr{
        std::atomic<Node*> ptr;
        MarkPtr(Node* n):ptr(n){};
        MarkPtr():ptr(nullptr){};
    };

public:
    struct Node{
        K key;
        MarkPtr next;
        V val;
        Node():key(),next(),val(){};
        Node(K k, V v, Node* n):key(k),next(n),val(v){};
    };
    struct ItemIterator{
        int curr_idx;
        int in_use_amount;
        Slab* curr_slab;
        ItemIterator(int idx, Slab* slab): curr_idx(idx), in_use_amount(0), curr_slab(slab){ 
            // we snapshot in_use_amount here so that the watermark
            // won't ever change and we can expect a stable output of
            // reached_end().
            if(curr_slab!=nullptr)
                in_use_amount = curr_slab->in_use_amount.load();
            while (true){
                if(reached_end()) return;
                if(!deleted()) return;
                advance();
            }
        }
        Node& operator*() const {
            assert(in_use_amount>curr_idx);
            assert(curr_idx<amount-1);
            return curr_slab->slab[curr_idx];
        }
        Node* operator->() {
            return &(this->operator*());
        }
        bool deleted(){
            assert(in_use_amount>curr_idx);
            assert(curr_idx<amount-1);
            return getMark(curr_slab->slab[curr_idx].next.ptr.load());
        }
        bool reached_end(){
            return curr_slab == nullptr || in_use_amount == curr_idx;
        }
        ItemIterator& operator++(){
            if(reached_end()) return *this;
            while(true) {
                advance();
                if(reached_end()) return *this;
                if(!deleted()) return *this;
            }
        }
        void advance(){
            curr_idx++;
            if(in_use_amount == amount && curr_idx == amount) {
                // curr_idx surpasses curr_slab end, but there
                // might be a following slab available.
                curr_slab = curr_slab->next.load();
                in_use_amount = curr_slab->in_use_amount.load();
                curr_idx = 0;
            }
        }
    };
private:
    std::hash<K> hash_fn;
    static constexpr int idxSize=128;//number of buckets for hash table
    padded<MarkPtr>* buckets = new padded<MarkPtr>[idxSize]{};
    bool findNode(MarkPtr* &prev, Node* &curr, Node* &next, K key);

    SlabAllocator alloc;

    static constexpr uint64_t MARK_MASK = ~0x1;
    inline static Node* getPtr(Node* mptr){
        return (Node*) ((uint64_t)mptr & MARK_MASK);
    }
    inline static bool getMark(Node* mptr){
        return (bool)((uint64_t)mptr & 1);
    }
    inline static Node* mixPtrMark(Node* ptr, bool mk){
        return (Node*) ((uint64_t)ptr | mk);
    }
    inline static Node* setMark(Node* mptr){
        return mixPtrMark(mptr,true);
    }
public:
    LockfreeSlabHashTable() : alloc() {};
    ~LockfreeSlabHashTable(){};

    std::optional<V> get(K key);
    std::optional<V> put(K key, V val);
    std::optional<V> remove(K key);
    void reset();
    bool empty() const { return alloc.empty(); }
    // Below are the only API called by helpers
    ItemIterator begin() const { return alloc.begin(); }
};

//-------Definition----------
template <class K, class V, int amount> 
std::optional<V> LockfreeSlabHashTable<K,V,amount>::get(K key) {
    MarkPtr* prev=nullptr;
    Node* curr=nullptr;
    Node* next=nullptr;
    std::optional<V> res={};

    if(findNode(prev,curr,next,key)) {
        res=curr->val;
    }

    return res;
}

template <class K, class V, int amount> 
std::optional<V> LockfreeSlabHashTable<K,V,amount>::put(K key, V val) {
    Node* tmpNode = nullptr;
    MarkPtr* prev=nullptr;
    Node* curr=nullptr;
    Node* next=nullptr;
    std::optional<V> res={};
    tmpNode = alloc.allocate(key, val, nullptr);

    if(findNode(prev,curr,next,key)) {
        // exists; replace
        res=curr->val;
        tmpNode->next.ptr.store(next);
        // insert tmpNode after cur and mark cur
        assert(curr->next.ptr.load()==next);
        curr->next.ptr.store(setMark(next));
        assert(prev->ptr.load()==curr);
        prev->ptr.store(tmpNode);
    }
    else {
        //does not exist; insert.
        tmpNode->next.ptr.store(curr);
        assert(prev->ptr.load()==curr);
        prev->ptr.store(tmpNode);
    }
    alloc.allocate_finalize(tmpNode);
    return res;
}

template <class K, class V, int amount> 
std::optional<V> LockfreeSlabHashTable<K,V,amount>::remove(K key) {
    MarkPtr* prev=nullptr;
    Node* curr=nullptr;
    Node* next=nullptr;
    std::optional<V> res={};

    if(findNode(prev,curr,next,key)) {
        res=curr->val;
        assert(curr->next.ptr.load()==next);
        curr->next.ptr.store(setMark(next));
        assert(prev->ptr.load()==curr);
        prev->ptr.store(next);
    }

    return res;
}

template <class K, class V, int amount> 
void LockfreeSlabHashTable<K,V,amount>::reset(){
    if(empty()) return;
    for(int i=0;i<idxSize;i++){
        // Wentao: it's OK to use no fence here, because helper
        // threads never access buckets.
        buckets[i].ui.ptr.store(nullptr, std::memory_order_relaxed);
    }
    alloc.reset();
}

template <class K, class V, int amount> 
bool LockfreeSlabHashTable<K,V,amount>::findNode(MarkPtr* &prev, Node* &curr, Node* &next, K key){
    size_t idx=hash_fn(key)%idxSize;
    bool cmark=false;
    prev=&buckets[idx].ui;
    curr=getPtr(prev->ptr.load());

    while(true){//to lock old and curr
        if(curr==nullptr) return false;
        next=curr->next.ptr.load();
        cmark=getMark(next);
        next=getPtr(next);
        auto ckey=curr->key;
        assert(prev->ptr.load()==curr);
        assert(!cmark);
        if(ckey>=key) return ckey==key;
        prev=&(curr->next);
        curr=next;
    }
}


#endif