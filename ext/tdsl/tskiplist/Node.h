#pragma once

#include "Utils.h"
#include "Mutex.h"
#include "skiplist/skiplist.h"

namespace tdsl{

class IndexOperation;
template<typename K, typename V> class SkipList;

enum NodeType{MIN, NORMAL};

class NodeBase{
public:
    NodeBase(unsigned int version, NodeType type = NORMAL):
        type(type), deleted(false), version(version), next(nullptr){
        skiplist_init_node(&snode);
    }
    virtual ~NodeBase() = default;

    bool isLocked()
    {
        return lock.isLocked();
    }

    virtual void update_index(IndexOperation& op) = 0;

    skiplist_node snode;
    NodeType type;
    bool deleted;
    Mutex lock;
    unsigned int version;
    NodeBase * next;
    
};

template<typename K, typename V>
class Node : public NodeBase
{
public:
    Node(SkipList<K,V>* sl, const K & k, const V & v, unsigned int version, NodeType type = NORMAL) :
        NodeBase(version, type), sl(sl), key(k), val(v) {}

    Node(SkipList<K,V>* sl, const K & k, unsigned int version, NodeType type = NORMAL) :
        NodeBase(version, type), sl(sl), key(k) {}

    virtual ~Node() = default;

    void update_index(IndexOperation& op) {
        sl->index.update(op);
    }

    SkipList<K,V>* sl = nullptr;
    K key;
    V val;
};

} // namespace tdsl