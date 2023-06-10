#ifndef LFTT_SKIPLIST_HPP
#define LFTT_SKIPLIST_HPP

#include "TestConfig.hpp"
#include "RMap.hpp"
#include "ConcurrentPrimitives.hpp"

#include "lftttransskip.h"
#include "lfttsetadaptor.h"

class LFTTSkipList : public RMap<uint64_t, uint64_t>{
    SetAdaptor<trans_skip> set;
    padded<SetOpArray*>* local_ops = nullptr;
public:
    // constructor: SetAdapter(max_node_number, thread_num + 1, max_transaction_size)
    LFTTSkipList(GlobalTestConfig* gtc) : set(1000000, gtc->task_num+1, 10){
        local_ops = new padded<SetOpArray*>[gtc->task_num];
        for (int i = 0 ;i < gtc->task_num; i++) {
            local_ops[i].ui = nullptr;
        }
        set.Init();
    }
    virtual ~LFTTSkipList(){
        set.Uninit();
        delete local_ops;
    }
    void init_thread(GlobalTestConfig* gtc, LocalTestConfig* ltc) override {
        if (ltc->tid != 0) {
            set.Init();
        }
    }
    /* RMap API: */
    optional<uint64_t> get(uint64_t key, int tid){
        if (!is_in_transaction(tid)){
            SetOpArray ops(1);
            ops[0].type = FIND;
            ops[0].key = key;
            set.ExecuteOps(ops);
        } else {
            SetOpArray* local_op = local_ops[tid].ui;
            local_op->emplace_back();
            local_op->back().type = FIND;
            local_op->back().key = key;
        }
        return {};
    }
    optional<uint64_t> put(uint64_t key, uint64_t val, int tid){
        errexit("LFTT skiplist does not provide put() method.");

        return {};
    }
    bool insert(uint64_t key, uint64_t val, int tid){
        if (!is_in_transaction(tid)){
            SetOpArray ops(1);
            ops[0].type = INSERT;
            ops[0].key = key;
            ops[0].val = val;
            set.ExecuteOps(ops);
        } else {
            SetOpArray* local_op = local_ops[tid].ui;
            local_op->emplace_back();
            local_op->back().type = INSERT;
            local_op->back().key = key;
            local_op->back().val = val;
        }
        return true;
    }
    optional<uint64_t> remove(uint64_t key, int tid){
        if (!is_in_transaction(tid)){
            SetOpArray ops(1);
            ops[0].type = DELETE;
            ops[0].key = key;
            set.ExecuteOps(ops);
        } else {
            SetOpArray* local_op = local_ops[tid].ui;
            local_op->emplace_back();
            local_op->back().type = DELETE;
            local_op->back().key = key;
        }
        return {};
    }
    optional<uint64_t> replace(uint64_t key, uint64_t val, int tid){
        errexit("LFTTSkipList::replace() not implemented!");
        return {};
    }
    /* transactional API */
    bool is_in_transaction(int tid) {
        return (local_ops[tid].ui != nullptr);
    }
    void begin_transaction(size_t trans_size, int tid) {
        assert(!is_in_transaction(tid));
        local_ops[tid].ui = new SetOpArray(trans_size);
    }
    bool commit_transaction(int tid){
        bool ret;
        assert(is_in_transaction(tid));
        ret = set.ExecuteOps(*local_ops[tid].ui);
        delete(local_ops[tid].ui);
        local_ops[tid].ui = nullptr;
        return ret;
    }
};

class LFTTSkipListFactory : public RideableFactory{
    Rideable* build(GlobalTestConfig* gtc){
        return new LFTTSkipList(gtc);
    }
};

#endif