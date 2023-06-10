#ifndef TDSL_SKIPLIST_HPP
#define TDSL_SKIPLIST_HPP

#include "TestConfig.hpp"
#include "RMap.hpp"
#include "ConcurrentPrimitives.hpp"

#include "TSkipList.h"

template <typename K, typename V>
class TDSLSkipList : public RMap<K,V>{
    tdsl::SkipList<K,V> sl;
    padded<::tdsl::SkipListTransaction>* _tdsl_txns = nullptr;
    bool _preallocated_tdsl_txns = false;

    void tx_begin(bool is_inside_txn, int tid){
        if(!is_inside_txn) {
            _tdsl_txns[tid].ui.TXBegin();
        }
    }
    void tx_end(bool is_inside_txn, int tid){
        if(!is_inside_txn) {
            _tdsl_txns[tid].ui.TXCommit();
        }
    }
public:
    TDSLSkipList(GlobalTestConfig* gtc) : _preallocated_tdsl_txns(gtc->_preallocated_tdsl_txns) {
        if(_preallocated_tdsl_txns) {
            _tdsl_txns = reinterpret_cast<padded<::tdsl::SkipListTransaction>*>(gtc->_tdsl_txns);
        } else {
            _tdsl_txns = new padded<::tdsl::SkipListTransaction>[gtc->task_num];
        }
    }
    virtual ~TDSLSkipList(){
        if (!_preallocated_tdsl_txns)
            delete _tdsl_txns;
    }
    /* RMap API: */
    optional<V> get(K key, int tid){
        bool succeeded = false;
        optional<V> ret = {};

        bool is_inside_txn = _tdsl_txns[tid].ui.is_inside_txn;

        uint64_t retry = 0;
        // while(!succeeded){
            try{
                tx_begin(is_inside_txn, tid);
                succeeded = true;
                ret = sl.get(key, _tdsl_txns[tid].ui);
                tx_end(is_inside_txn, tid);
            } catch (AbortTransactionException &) {
                retry ++;
                if (retry >= 1000) {
                    errexit("too many retries.");
                }
                succeeded = false;
                // throw;
            }
        // }
        return ret;
    }
    optional<V> put(K key, V val, int tid){
        bool succeeded = false;
        optional<V> ret = {};

        bool is_inside_txn = _tdsl_txns[tid].ui.is_inside_txn;

        uint64_t retry = 0;
        // while(!succeeded){
            try{
                tx_begin(is_inside_txn, tid);
                succeeded = true;
                ret = sl.put(key, val, _tdsl_txns[tid].ui);
                tx_end(is_inside_txn, tid);
            } catch (AbortTransactionException &) {
                retry ++;
                if (retry >= 1000) {
                    errexit("too many retries.");
                }
                succeeded = false;
                // throw;
            }
        // }
        return ret;
    }
    bool insert(K key, V val, int tid){
        bool succeeded = false;
        bool ret = false;

        bool is_inside_txn = _tdsl_txns[tid].ui.is_inside_txn;

        uint64_t retry = 0;
        // while(!succeeded){
            try{
                tx_begin(is_inside_txn, tid);
                succeeded = true;
                ret = sl.insert(key, val, _tdsl_txns[tid].ui);
                tx_end(is_inside_txn, tid);
            } catch (AbortTransactionException &) {
                retry ++;
                if (retry >= 1000) {
                    errexit("too many retries.");
                }
                succeeded = false;
                // throw;
            }
        // }
        return ret;
    }
    optional<V> remove(K key, int tid){
        bool succeeded = false;
        optional<V> ret = {};

        bool is_inside_txn = _tdsl_txns[tid].ui.is_inside_txn;

        uint64_t retry = 0;
        // while(!succeeded){
            try{
                tx_begin(is_inside_txn, tid);
                succeeded = true;
                ret = sl.remove_return(key, _tdsl_txns[tid].ui);
                tx_end(is_inside_txn, tid);
            } catch (AbortTransactionException &) {
                retry ++;
                if (retry >= 1000) {
                    errexit("too many retries.");
                }
                succeeded = false;
                // throw;
            }
        // }
        return ret;
    }
    optional<V> replace(K key, V val, int tid){
        errexit("TDSLSkipList::replace() not implemented!");
        return {};
    }
};

template <typename T> 
class TDSLSkipListFactory : public RideableFactory{
    Rideable* build(GlobalTestConfig* gtc){
        return new TDSLSkipList<T, T>(gtc);
    }
};

#endif