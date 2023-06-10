#ifndef TXN_VERIFY_HPP
#define TXN_VERIFY_HPP

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
#include <string>

template <class K, class V>
class TxnVerify : public Test{
    enum op_type {
        ReadSingle=0,
        WriteA,
        ReadB,
        WriteB,
        ReadC,
        WriteC
    };
    const uint64_t init_val = 1000;
    const int op_val = 10;
public:
    vector<int> op_type_percent;
    int m_amount;
    vector<RMap<K,V>*> maps;
    size_t key_size = TESTS_KEY_SIZE;
    size_t val_size = TESTS_VAL_SIZE;
    std::string value_buffer; // for string kv only
    int range;
    int prefill;
    pds::EpochSys* _esys = nullptr;

    TxnVerify(int p_get, int p_write_a, 
        int p_read_b, int p_write_b, 
        int p_read_c, int p_write_c,
        int range_, 
        int maps_amount){
        // get_total, increase, decrease, transfer, aggregate, deposit, get, insert, remove
        op_type_percent.push_back(p_get);
        op_type_percent.push_back(p_get+p_write_a);
        op_type_percent.push_back(p_get+p_write_a+p_read_b);
        op_type_percent.push_back(p_get+p_write_a+p_read_b+p_write_b);
        op_type_percent.push_back(p_get+p_write_a+p_read_b+p_write_b+p_read_c);
        op_type_percent.push_back(p_get+p_write_a+p_read_b+p_write_b+p_read_c+p_write_c);
        assert(op_type_percent.size()==6);
        assert(op_type_percent[op_type::WriteC] == 100);

        assert(range_%8==0);
        range = range_;
        prefill = range_;
        m_amount = maps_amount;
    }

    inline K fromInt(uint64_t v){
        return (K)v;
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

        for(int i=0; i<m_amount; i++)
            allocRideable(gtc);

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

    int allocRideable(GlobalTestConfig* gtc){
		Rideable* ptr = gtc->allocRideable();
        RMap<K,V>* m = dynamic_cast<RMap<K, V>*>(ptr);
		if (!m) {
			 errexit("MapChurnTest must be run on RMap<K,V> type object.");
		}
        maps.push_back(m);
        return maps.size()-1;
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

            // r = abs(rand_nums[(k_idx++)%1000]%range);
            int p = abs(static_cast<long>(gen_p()%100));
            // int p = abs(rand_nums[(p_idx++)%1000]%100);

            if(operation(p, k1, k2, m_idx1, m_idx2, tid))
                ops++;
            
            if (ops % 512 == 0){
                now = std::chrono::high_resolution_clock::now();
            }
        }
        return ops;
    }


    bool operation(int op, uint64_t k1_, uint64_t k2_, 
        uint64_t m_idx1, uint64_t m_idx2, int tid){
        // TODO: replace `operation` with multiple different
        // transaction routines
        V v = op_val;
        // printf("%d.\n", r);
        
        if(op<this->op_type_percent[op_type::ReadSingle]){
            // single read, ensure val%10==0
            // for any key, particularly key%8==0
            K k1 = this->fromInt(k1_);
            auto v = maps[m_idx1]->get(k1,tid);
            assert(v.has_value() && (*v)%10==0);
        }
        else if(op<this->op_type_percent[op_type::WriteA]){
            // write key 2 times, to make val%10==0
            // key%8==0
            k1_ = k1_ & ~7ULL;// reset lowest 3 bits
            K k1 = this->fromInt(k1_);
            return txn_write_a(k1,m_idx1,tid);
        }
        else if(op<this->op_type_percent[op_type::ReadB]){
            // read 2 keys together, ensure they equal
            // key%8==1 and 2
            k1_ = (k1_ & ~7ULL)+1;// reset lowest 3 bits
            k2_ = k1_+1;// reset lowest 3 bits
            K k1 = this->fromInt(k1_);
            K k2 = this->fromInt(k2_);
            return txn_read_b(k1,k2,m_idx1,tid);
        }
        else if(op<this->op_type_percent[op_type::WriteB]){
            // write 2 keys together, make them equal
            // key%8==1 and 2
            k1_ = (k1_ & ~7ULL)+1;// reset lowest 3 bits
            k2_ = k1_+1;// reset lowest 3 bits
            K k1 = this->fromInt(k1_);
            K k2 = this->fromInt(k2_);
            return txn_write_b(k1,k2,m_idx1,v,tid);
        }
        else if(op<this->op_type_percent[op_type::ReadC]){
            // read 1 key from two maps, ensure they have stable sum
            // key%8==3, m_idx1%2==0
            k1_ = (k1_ & ~7ULL)+3;// reset lowest 3 bits
            K k1 = this->fromInt(k1_);
            m_idx1 = m_idx1 & ~1ULL;
            m_idx2 = m_idx1+1;
            return txn_read_c(k1,m_idx1,m_idx2,tid);
        }
        else if(op<this->op_type_percent[op_type::WriteC]){
            // write 1 key from two maps, ensure they have stable sum
            // key%8==3, m_idx1%2==0
            k1_ = (k1_ & ~7ULL)+3;// reset lowest 3 bits
            K k1 = this->fromInt(k1_);
            m_idx1 = m_idx1 & ~1ULL;
            m_idx2 = m_idx1+1;
            return txn_write_c(k1,m_idx1,m_idx2,op_val,tid);
        }
        else{ // impossible
            assert(false);
        }
        return true;
    }
    void cleanup(GlobalTestConfig* gtc){
        // Pronto handles deletion by its own
        for(auto m:maps)
            delete m;
        std::cout<<"Test passed!\n";
    }

    bool txn_write_a(K key, int m_idx, int tid){
        int attempt = 0;
        while (attempt<1000){
            try{
                _esys->tx_begin();
                auto v = maps[m_idx]->get(key, tid);
                if(!v.has_value()) {
                    *v = 0;
                    maps[m_idx]->insert(key, 5, tid);
                } else {
                    maps[m_idx]->replace(key, *v + 5, tid);
                }
                maps[m_idx]->replace(key, *v + 10, tid);
                _esys->tx_end();
                return true;
            }catch(const pds::TransactionAborted& e){
                // aborted 
                attempt++;
                continue; // retry until failed 10 times
            }
        }
        assert(0&&"`txn_write_a` retry too many times");
        return false;
    }

    bool txn_read_b(K key1, K key2, int m_idx1, int tid){
        int attempt = 0;
        while (attempt<100000){
            try{
                _esys->tx_begin();
                auto v1 = maps[m_idx1]->get(key1, tid);
                auto v2 = maps[m_idx1]->get(key2, tid);
                if(!v1.has_value()) {
                    _esys->tx_end();
                    assert(!v2.has_value());
                    return false;
                }
                _esys->tx_end();
                assert(*v1 == *v2);
                return true;
            }catch(const pds::TransactionAborted& e){
                // aborted 
                attempt++;
                continue; // retry until failed 100 times
            }
        }
        assert(0&&"`txn_read_b` retry too many times");
        return false;
    }

    bool txn_write_b(K key1, K key2, int m_idx1, V val, int tid){
        int attempt = 0;
        while (attempt<1000){
            try{
                _esys->tx_begin();
                auto v1 = maps[m_idx1]->get(key1, tid);
                auto v2 = maps[m_idx1]->get(key2, tid);
                if(!v1.has_value()) {
                    maps[m_idx1]->insert(key1, val, tid);
                    maps[m_idx1]->insert(key2, val, tid);
                } else { // key1 and key2 must exist
                    maps[m_idx1]->replace(key1, val, tid);
                    maps[m_idx1]->replace(key2, val, tid);
                }
                _esys->tx_end();
                assert((!v1.has_value()&&!v2.has_value()) || *v1 == *v2);
                return true;
            }catch(const pds::TransactionAborted& e){
                // aborted 
                attempt++;
                continue; // retry until failed 100 times
            }
        }
        assert(0&&"`txn_write_b` retry too many times");
        return false;
    }

    bool txn_read_c(K k,int m_idx1, int m_idx2, int tid){
        int attempt = 0;
        while (attempt<100000){
            try{
                _esys->tx_begin();
                auto v1 = maps[m_idx1]->get(k, tid);
                auto v2 = maps[m_idx2]->get(k, tid);
                if(!v1.has_value() || !v2.has_value()){
                    _esys->tx_end();
                    assert(false);
                    return false;
                }
                _esys->tx_end();
                assert(*v1+*v2 == 2*init_val);
                return true;
            }catch(const pds::TransactionAborted& e){
                // aborted 
                attempt++;
                continue; // retry until failed 10000 times
            }
        }
        assert(0&&"`txn_read_c` retry too many times");
        return false;
    }

    bool txn_write_c(K k,int m_idx1, int m_idx2, V val, int tid){
        int attempt = 0;
        while (attempt<1000){
            try{
                _esys->tx_begin();
                auto v1 = maps[m_idx1]->get(k, tid);
                auto v2 = maps[m_idx2]->get(k, tid);
                if(!v1.has_value() || !v2.has_value()){
                    _esys->tx_end();
                    assert(false);
                    return false;
                }
                if(*v1==0){
                    _esys->tx_end();
                    assert(*v2==2*init_val);
                    return false;
                }
                *v1-=val;
                *v2+=val;
                maps[m_idx1]->replace(k, *v1, tid);
                maps[m_idx2]->replace(k, *v2, tid);
                _esys->tx_end();
                return true;
            }catch(const pds::TransactionAborted& e){
                // aborted 
                attempt++;
                continue; // retry until failed 10000 times
            }
        }
        assert(0&&"`txn_write_c` retry too many times");
        return false;
    }
};



#endif