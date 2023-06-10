#ifndef TXNMAPQUEUETEST_HPP
#define TXNMAPQUEUETEST_HPP

/*
 * This is a test for transactions.
 */

#include "AllocatorMacro.hpp"
#include "Persistent.hpp"
#include "TestConfig.hpp"
#include "EpochSys.hpp"
#include "RMap.hpp"
#include <iostream>
#include <cstdlib>
#include <algorithm>

//KEY_SIZE and VAL_SIZE are only for string kv
template <class K, class V, bool txn_on=true>
class TxnMapQueueTest : public Test{
    enum op_type {
        GetTotal=0,
        Increase,
        Decrease,
        Transfer,
        Aggregate,
        Deposit,
        Get, // single op
        Insert, // single op
        Remove // single op
    };
    const int init_val = 1000000;
    const int op_val = 1000;
public:
    vector<int> op_type_percent;
    vector<int> maps_types;
    vector<int> queues_types;
    vector<RMap<K,V>*> maps;
    vector<RQueue<V>*> queues;
    size_t key_size = TESTS_KEY_SIZE;
    size_t val_size = TESTS_VAL_SIZE;
    std::string value_buffer; // for string kv only
    int range;
    int prefill;
    pds::EpochSys* _esys = nullptr;

    TxnMapQueueTest(int p_get_total, 
        int p_increase, int p_decrease,
        int p_transfer, 
        int p_aggregate, int p_deposit, 
        int p_get, int p_insert, int p_remove, 
        int range_, int prefill_, 
        const vector<int>& maps_types_, const vector<int>& queues_types_) : 
            maps_types(maps_types_), queues_types(queues_types_){
        // get_total, increase, decrease, transfer, aggregate, deposit, get, insert, remove
        op_type_percent.push_back(p_get_total);
        op_type_percent.push_back(p_get_total+p_increase);
        op_type_percent.push_back(p_get_total+p_increase+p_decrease);
        op_type_percent.push_back(p_get_total+p_increase+p_decrease+p_transfer);
        op_type_percent.push_back(p_get_total+p_increase+p_decrease+p_transfer+p_aggregate);
        op_type_percent.push_back(p_get_total+p_increase+p_decrease+p_transfer+p_aggregate+p_deposit);
        op_type_percent.push_back(p_get_total+p_increase+p_decrease+p_transfer+p_aggregate+p_deposit+p_get);
        op_type_percent.push_back(p_get_total+p_increase+p_decrease+p_transfer+p_aggregate+p_deposit+p_get+p_insert);
        op_type_percent.push_back(p_get_total+p_increase+p_decrease+p_transfer+p_aggregate+p_deposit+p_get+p_insert+p_remove);
        assert(op_type_percent.size()==9);
        assert(op_type_percent[op_type::Remove] == 100);

        range = range_;
        prefill = prefill_;
    }

    inline K fromInt(uint64_t v){
        return (K)v;
    }

    void tx_begin(){
        _esys->tx_begin();
    }

    void tx_end(){
        _esys->tx_end();
    }

    void tx_abort(){
        _esys->tx_abort();
    }

    virtual void init(GlobalTestConfig* gtc){
        if(gtc->checkEnv("Liveness")){
            string env_liveness = gtc->getEnv("Liveness");
            if(env_liveness == "Nonblocking"){
                _esys = new pds::nbEpochSys(gtc);
            } else if (env_liveness == "Blocking"){
                _esys = new pds::EpochSys(gtc);
            } else {
                errexit("unrecognized 'Liveness' environment");
            }
        } else {
            gtc->setEnv("Liveness", "Blocking");
            _esys = new pds::EpochSys(gtc);
        }
        gtc->setUpEpochSys(reinterpret_cast<void*>(_esys));

        if(gtc->checkEnv("KeySize")){
            key_size = atoi((gtc->getEnv("KeySize")).c_str());
            assert(key_size<=TESTS_KEY_SIZE&&"KeySize dynamically passed in is greater than macro TESTS_KEY_SIZE!");
        }
        if(gtc->checkEnv("ValueSize")){
            val_size = atoi((gtc->getEnv("ValueSize")).c_str());
            assert(val_size<=TESTS_VAL_SIZE&&"ValueSize dynamically passed in is greater than macro TESTS_VAL_SIZE!");
        }

        value_buffer.reserve(val_size);
        value_buffer.clear();
        std::mt19937_64 gen_v(7);
        for (size_t i = 0; i < val_size - 1; i++) {
            value_buffer += (char)((i % 2 == 0 ? 'A' : 'a') + (gen_v() % 26));
        }
        value_buffer += '\0';

        for(auto m_type:maps_types)
            allocRideable(gtc, m_type, true);
        for(auto q_type:queues_types)
            allocRideable(gtc, q_type, false);

        if(gtc->verbose){
            std::cout<<"Map types:";
            for(auto m_type:maps_types){
                std::cout<<gtc->getRideableName(m_type);
                std::cout<<",";
            }
            std::cout<<"\n";
            std::cout<<"Queue types:";
            for(auto q_type:queues_types){
                std::cout<<gtc->getRideableName(q_type);
                std::cout<<",";
            }
            std::cout<<"\n";
            // TODO: anything else to print?
        }

        // overrides for constructor arguments
        if(gtc->checkEnv("range")){
            range = atoi((gtc->getEnv("range")).c_str());
        }
        if(gtc->checkEnv("prefill")){
            prefill = atoi((gtc->getEnv("prefill")).c_str());
        }

        doPrefill(gtc);
    }

    virtual void parInit(GlobalTestConfig* gtc, LocalTestConfig* ltc){
        maps[0]->init_thread(gtc, ltc);
    }

    int allocRideable(GlobalTestConfig* gtc, int rideableType, bool is_map){
        Rideable* ptr = gtc->allocRideable(rideableType);
        if (is_map){
            RMap<K,V>* m = dynamic_cast<RMap<K, V>*>(ptr);
            if(!m){
                errexit("Expecting Rideable to be RMap<K,V> type.");
            }
            maps.push_back(m);
            return maps.size()-1;
        }else{
            // must be queue
            RQueue<V>* q = dynamic_cast<RQueue<V>*>(ptr);
            if (!q){
                errexit("Expecting Rideable to be RQueue<V> type.");
            }
            queues.push_back(q);
            return queues.size()-1;
        }
    }

    void doPrefill(GlobalTestConfig* gtc){
        if (this->prefill > 0){
            /* Wentao: 
             *	to avoid repeated k during prefilling, we instead 
             *	insert [0,min(prefill-1,range)] 
             */
            // std::mt19937_64 gen_k(0);
            // int stride = this->range/this->prefill;
            int i = 0;
            while(i<this->prefill){
                K k = this->fromInt(i%range);
                for(auto m:maps)
                    m->insert(k,init_val,0);
                i++;
            }
            if(gtc->verbose){
                printf("Prefilled %d\n",i);
            }
            // one sync is enough
            Recoverable* rec=dynamic_cast<Recoverable*>(maps[0]);
            if(rec){
                rec->sync();
            }
        }
    }
    int execute(GlobalTestConfig* gtc, LocalTestConfig* ltc){
        auto time_up = gtc->finish;

        int ops = 0;
        uint64_t r = ltc->seed;
        std::mt19937_64 gen_k(r);
        std::mt19937_64 gen_p(r+1);
        std::mt19937_64 gen_idx(r+3);

        int tid = ltc->tid;

        // atomic_thread_fence(std::memory_order_acq_rel);
        //broker->threadInit(gtc,ltc);
        auto now = std::chrono::high_resolution_clock::now();

        while(std::chrono::duration_cast<std::chrono::microseconds>(time_up - now).count()>0){

            uint64_t k1 = abs(static_cast<long>(gen_k()%range));
            uint64_t k2 = abs(static_cast<long>(gen_k()%range));
            uint64_t m_idx1 = abs(static_cast<long>(gen_idx()%maps.size()));
            uint64_t m_idx2 = abs(static_cast<long>(gen_idx()%maps.size()));
            uint64_t q_idx = abs(static_cast<long>(gen_idx()%queues.size()));

            // r = abs(rand_nums[(k_idx++)%1000]%range);
            int p = abs(static_cast<long>(gen_p()%100));
            // int p = abs(rand_nums[(p_idx++)%1000]%100);

            operation(p, k1, k2, m_idx1, m_idx2, q_idx, tid);
            
            ops++;
            if (ops % 512 == 0){
                now = std::chrono::high_resolution_clock::now();
            }
        }
        return ops;
    }


    void operation(int op, uint64_t k1_, uint64_t k2_, 
        uint64_t m_idx1, uint64_t m_idx2, uint64_t q_idx, int tid){
        // TODO: replace `operation` with multiple different
        // transaction routines
        K k1 = this->fromInt(k1_);
        K k2 = this->fromInt(k2_);
        V v = op_val;
        // printf("%d.\n", r);
        
        if(op<this->op_type_percent[op_type::GetTotal]){
            txn_get_total(k1,tid);
        }
        else if(op<this->op_type_percent[op_type::Increase]){
            txn_increase(k1,m_idx1,v,tid);
        }
        else if(op<this->op_type_percent[op_type::Decrease]){
            txn_decrease(k1,m_idx1,v,tid);
        }
        else if(op<this->op_type_percent[op_type::Transfer]){
            txn_transfer(k1,k2,m_idx1,m_idx2,v,tid);
        }
        else if(op<this->op_type_percent[op_type::Aggregate]){
            txn_aggregate(k1,q_idx,v,tid);
        }
        else if(op<this->op_type_percent[op_type::Deposit]){
            txn_deposit(k1,m_idx1,q_idx,tid);
        }
        else if(op<this->op_type_percent[op_type::Get]){
            maps[m_idx1]->get(k1,tid);
        }
        else if (op<this->op_type_percent[op_type::Insert]){
            maps[m_idx1]->insert(k1,v,tid);
        }
        else{ // op<=this->op_type_percent[op_type::Remove]
            maps[m_idx1]->remove(k1,tid);
        }
    }
    void cleanup(GlobalTestConfig* gtc){
        // Pronto handles deletion by its own
        for(auto m:maps)
            delete m;
        for(auto q:queues)
            delete q;
    }

    V txn_get_total(K key, int tid){
        int attempt = 0;
        while (attempt<10){
            try{
                tx_begin();
                V total_v = 0;
                for (auto m:maps){
                    auto v = m->get(key, tid);
                    if(!v.has_value()) {
                        continue;
                    }
                    total_v+=v.value();
                }
                tx_end();
                return total_v;
            }catch(const pds::TransactionAborted& e){
                // aborted 
                attempt++;
                continue; // retry until failed 10 times
            }
        }
        assert(0&&"`get_total` retry too many times");
        return {};
    }

    // return val after increase
    V txn_increase(K key, int m_idx, V val, int tid){
        int attempt = 0;
        while (attempt<10){
            try{
                tx_begin();
                auto v = maps[m_idx]->get(key, tid);
                if(!v.has_value()) {
                    maps[m_idx]->insert(key, val, tid);
                } else {
                    maps[m_idx]->replace(key, val+v.value(), tid);
                }
                tx_end();
                return v.value_or(0)+val;
            }catch(const pds::TransactionAborted& e){
                // aborted 
                attempt++;
                continue; // retry until failed 10 times
            }
        }
        assert(0&&"`txn_increase` retry too many times");
        return {};
    }

    // return val really decreased
    V txn_decrease(K key, int m_idx, V val, int tid){
        int attempt = 0;
        while (attempt<10){
            try{
                tx_begin();
                auto v = maps[m_idx]->get(key, tid);
                if(!v.has_value()) {
                    tx_end();
                    return 0;
                } else {
                    if (v.value()>val)
                        maps[m_idx]->replace(key, val-v.value(), tid);
                    else 
                        maps[m_idx]->remove(key, tid);
                }
                tx_end();
                return std::min(v.value_or(0), val);
            }catch(const pds::TransactionAborted& e){
                // aborted 
                attempt++;
                continue; // retry until failed 10 times
            }
        }
        assert(0&&"`txn_increase` retry too many times");
        return {};
    }

    bool txn_transfer(K key1, K key2, int m1_idx, int m2_idx, V val, int tid){
        // transfer from key1 in m1 to key2 in m2
        int attempt = 0;
        while (attempt<100){
            try{
                tx_begin();
                auto v1 = maps[m1_idx]->get(key1, tid);
                auto v2 = maps[m2_idx]->get(key2, tid);
                if(!v1.has_value() || v1<val) {
                    tx_end();
                    return false;
                }
                if(!v2.has_value()) v2=0;
                (*v1)-=val;
                (*v2)+=val;
                if(v1==0)
                    maps[m1_idx]->remove(key1, tid);
                else
                    maps[m1_idx]->replace(key1, *v1, tid);
                maps[m2_idx]->put(key2, *v2, tid);
                tx_end();
                return true;
            }catch(const pds::TransactionAborted& e){
                // aborted 
                attempt++;
                continue; // retry until failed 100 times
            }
        }
        assert(0&&"`transfer` retry too many times");
        return false;
    }

    bool txn_aggregate(K key, int q_idx, V val, int tid){
        // aggregate money from key in multiple maps and buffer it in
        // message queue
        int attempt = 0;
        while (attempt<100){
            try{
                tx_begin();
                V total_v = 0;
                for (auto m:maps){
                    auto v = m->get(key, tid);
                    if(!v.has_value()){
                        continue;
                    } else if (v.value() < val) {
                        total_v+=v.value();
                        m->remove(key, tid);
                    } else {
                        total_v+=val;
                        m->replace(key, v.value()-val, tid);
                    }
                }
                queues[q_idx]->enqueue(total_v, tid);
                tx_end();
                return true;
            }catch(const pds::TransactionAborted& e){
                // aborted 
                attempt++;
                continue; // retry until failed 10000 times
            }
        }
        assert(0&&"`aggregate` retry too many times");
        return false;
    }

    bool txn_deposit(K key, int m_idx, int q_idx, int tid){
        // dequeue a message from queue and deposit money to key in
        // maps[m_idx]
        int attempt = 0;
        while (attempt<1000){
            try{
                tx_begin();
                V deposit_v = 0;
                auto message = queues[q_idx]->dequeue(tid);
                if(!message.has_value()){
                    attempt=-1; // don't retry
                    tx_abort();
                }

                auto v = maps[m_idx]->get(key, tid);
                if(!v.has_value()) {
                    // not exist
                    maps[m_idx]->insert(key, message.value(), tid);
                } else {
                    maps[m_idx]->replace(key, v.value()+message.value(), tid);
                }
                tx_end();
                return true;
            }catch(const pds::TransactionAborted& e){
                // aborted 
                if(attempt==-1)
                    return false;
                attempt++;
                continue; // retry until failed 10 times
            }
        }
        assert(0&&"`deposit` retry too many times");
        return false;
    }
    // abandoned due to very bad contention at the beginning of txn,
    // which leads to classic livelock issue
    bool txn_distribute(K key, int q_idx, int tid){
        // dequeue a message from queue and distribute money to key in
        // multiple maps
        int attempt = 0;
        while (attempt<10){
            try{
                tx_begin();
                V distributed_v = 0;
                auto message = queues[q_idx]->dequeue(tid);
                if(!message.has_value()){
                    attempt=-1; // don't retry
                    tx_abort();
                }
                distributed_v = message.value()/maps.size();

                for (auto m:maps){
                    auto v = m->get(key, tid);
                    if(!v.has_value()) {
                        // not exist
                        m->insert(key, distributed_v, tid);
                    } else {
                        m->replace(key, v.value()+distributed_v, tid);
                    }
                }
                tx_end();
                return true;
            }catch(const pds::TransactionAborted& e){
                // aborted 
                if (attempt==998)
                    attempt=998;
                if(attempt==-1)
                    return false;
                attempt++;
                continue; // retry until failed 10 times
            }
        }
        assert(0&&"`distribute` retry too many times");
        return false;
    }
};
template <>
void TxnMapQueueTest<uint64_t,uint64_t,false>::tx_begin(){
    return;
}

template <>
void TxnMapQueueTest<uint64_t,uint64_t,false>::tx_end(){
    return;
}

template <>
void TxnMapQueueTest<uint64_t,uint64_t,false>::tx_abort(){
    throw pds::AbortBeforeCommit();
    return;
}

#endif