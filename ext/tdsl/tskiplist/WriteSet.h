#pragma once

#include "Utils.h"
#include "Node.h"
#include "SafeLock.h"

namespace tdsl {

class Operation
{
public:
    Operation(NodeBase * next, bool deleted) : next(next), deleted(deleted) {}

    NodeBase * next;
    bool deleted;
};

class WriteSet
{
public:
    void clear(){
        items.clear();
    }
    void addItem(NodeBase * node, NodeBase * next, bool deleted){
        const auto it = items.find(node);
        if (it == items.end()) {
            items.insert(std::pair<NodeBase *, Operation>(node, Operation(next, deleted)));
        } else {
            if (next) {
                it->second.next = next;
            }
            if (deleted) {
                it->second.deleted = deleted;
            }
        }
    }

    bool getValue(NodeBase * node, NodeBase *& next, bool * deleted = NULL){
        auto it = items.find(node);
        if (it == items.end()) {
            return false;
        }

        if (deleted) {
            *deleted = it->second.deleted;
        }

        if (it->second.next != NULL) {
            next = it->second.next;
        } else {
            next = node->next;
        }

        return true;
    }

    // TODO: Make sure this doesn't lock/unlock due to copy construction
    bool tryLock(SafeLockList & locks){
        for (auto & it : items) {
            NodeBase * node = it.first;
            if (node->lock.tryLock()) {
                locks.add(node->lock);
            } else {
                return false;
            }
        }
        return true;
    }

    void update(unsigned int newVersion){
        for (auto & it : items) {
            NodeBase * n = it.first;
            Operation & op = it.second;

            if (op.deleted) {
                n->deleted = op.deleted;
            }

            if (op.next) {
                n->next = op.next;
            }

            n->version = newVersion;
        }
    }

private:
    std::unordered_map<NodeBase *, Operation> items;
};

} // namespace tdsl