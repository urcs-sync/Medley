#ifndef TXN_MAPCHURNTEST_HPP
#define TXN_MAPCHURNTEST_HPP

/*
 * This is a test with a time length for mappings.
 */

#include "ChurnTest.hpp"
#include "TestConfig.hpp"
#include "RMap.hpp"
#include "TxnMeta.hpp"

#include <iostream>

//KEY_SIZE and VAL_SIZE are only for string kv
template <class K, class V, TxnType txn_type=TxnType::NBTC>
class TxnMapChurnTest : public ChurnTest{
	void do_tx(int tid, std::function<void(void)> f, int sz = 0){
        txn_manager.do_tx(tid, f, sz);
    }
	// void tx_begin(int tid){
    //     txn_manager.tx_begin(tid);
    // }

    // void tx_end(int tid){
    //     txn_manager.tx_end(tid);
    // }

    // void tx_abort(int tid){
    //     txn_manager.tx_abort(tid);
    // }
public:
	RMap<K,V>* m;
	size_t key_size = TESTS_KEY_SIZE;
	size_t val_size = TESTS_VAL_SIZE;
	const bool fix_sized_txn;
	int max_op_per_txn;
	TxnManager<txn_type> txn_manager;
	std::string value_buffer; // for string kv only
	TxnMapChurnTest(bool fix_sized_txn, int max_op_per_txn, int p_gets, int p_puts, int p_inserts, int p_removes, int range, int prefill):
		ChurnTest(p_gets, p_puts, p_inserts, p_removes, range, prefill),
		fix_sized_txn(fix_sized_txn),
		max_op_per_txn(max_op_per_txn){}
	TxnMapChurnTest(int max_op_per_txn,int p_gets, int p_puts, int p_inserts, int p_removes, int range):
		ChurnTest(p_gets, p_puts, p_inserts, p_removes, range),
		fix_sized_txn(fix_sized_txn),
		max_op_per_txn(max_op_per_txn){}

	inline K fromInt(uint64_t v);

	virtual void init(GlobalTestConfig* gtc){
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
		ChurnTest::init(gtc);
	}

	virtual void parInit(GlobalTestConfig* gtc, LocalTestConfig* ltc){
		m->init_thread(gtc, ltc);
		ChurnTest::parInit(gtc, ltc);
	}

	void allocRideable(GlobalTestConfig* gtc){
		// set up epoch system for txMontage ds
		if(gtc->checkEnv("Liveness")){
            string env_liveness = gtc->getEnv("Liveness");
            if(env_liveness == "Nonblocking"){
                txn_manager._esys = new ::pds::nbEpochSys(gtc);
            } else if (env_liveness == "Blocking"){
                txn_manager._esys = new ::pds::EpochSys(gtc);
            } else {
                errexit("unrecognized 'Liveness' environment");
            }
        } else {
            gtc->setEnv("Liveness", "Blocking");
            txn_manager._esys = new ::pds::EpochSys(gtc);
        }
        gtc->setUpEpochSys(reinterpret_cast<void*>(txn_manager._esys));

		// set up txn for TDSL
		txn_manager._tdsl_txns = new padded<::tdsl::SkipListTransaction>[gtc->task_num];
        gtc->setUpTDSLTxns(reinterpret_cast<void*>(txn_manager._tdsl_txns));

		Rideable* ptr = gtc->allocRideable();
		m = dynamic_cast<RMap<K, V>*>(ptr);
		if (!m) {
			 errexit("TxnMapChurnTest must be run on RMap<K,V> type object.");
		}

		// LFTT skip list
		if (txn_type == LFTT) {
			txn_manager._lftt_skiplist = dynamic_cast<LFTTSkipList*>(m);
			assert(txn_manager._lftt_skiplist);
		}
	}
	Rideable* getRideable(){
		return m;
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
				m->insert(k,k,0);
				i++;
			}
			if(gtc->verbose){
				printf("Prefilled %d\n",i);
			}
			Recoverable* rec=dynamic_cast<Recoverable*>(m);
			if(rec){
				rec->sync();
			}
		}
	}

	int execute(GlobalTestConfig* gtc, LocalTestConfig* ltc) override{
		auto time_up = gtc->finish;
		
		int ops = 0;
		uint64_t seed = ltc->seed;
		std::mt19937_64 gen_k(seed);
		std::mt19937_64 gen_p(seed+1);
		std::mt19937_64 gen_txn_size(seed+2);

		int tid = ltc->tid;

		// atomic_thread_fence(std::memory_order_acq_rel);
		//broker->threadInit(gtc,ltc);
		auto now = std::chrono::high_resolution_clock::now();

		while(std::chrono::duration_cast<std::chrono::microseconds>(time_up - now).count()>0){

	        int retry = 0;
			int sz = 0;
			if (fix_sized_txn) {
				sz = max_op_per_txn;
			} else {
				sz = abs((long)gen_txn_size()%max_op_per_txn)+1;
			}
			int r[sz], p[sz]; 
			for(int i=0;i<sz;i++) {
				r[i] = abs((long)gen_k()%range);
				p[i] = abs((long)gen_p()%100);
			}
			// while (true){
				try {
					do_tx(tid, [&] () {
						for(int i=0;i<sz;i++)
							operation(r[i], p[i], tid);
					}, sz);
					ops++;
					// break;
				} catch(const pds::TransactionAborted& e) {
                	if (retry>1000) {
						errexit("Retry too many times!");
						break;
					} else {
						retry++;
					}
				}
			// } // while(true)
			
			if (ops % 512 == 0){
				now = std::chrono::high_resolution_clock::now();
			}
			// TODO: replace this with __rdtsc
			// or use hrtimer (high-resolution timer API in linux.)
		}
		return ops;
	}
	void operation(uint64_t key, int op, int tid){
		K k = this->fromInt(key);
		V v = k;
		// printf("%d.\n", r);
		
		if(op<this->prop_gets){
			m->get(k,tid);
		}
		else if(op<this->prop_puts){
			m->put(k,v,tid);
		}
		else if(op<this->prop_inserts){
			m->insert(k,v,tid);
		}
		else{ // op<=prop_removes
			m->remove(k,tid);
		}
	}
	void cleanup(GlobalTestConfig* gtc){
		ChurnTest::cleanup(gtc);
#ifndef PRONTO
		// Pronto handles deletion by its own
		if constexpr (txn_type != TxnType::OneFile){
			delete m;
		}
		// Wentao: we don't manually deallocate OneFile data
		// structures, as it would need special tmDelete
#endif
	}
};

template <class K, class V, TxnType txn_type>
inline K TxnMapChurnTest<K,V,txn_type>::fromInt(uint64_t v){
	return (K)v;
}

template<>
inline std::string TxnMapChurnTest<std::string,std::string,TxnType::NBTC>::fromInt(uint64_t v){
	auto _key = std::to_string(v);
	return "user"+std::string(key_size-_key.size()-4,'0')+_key;
}

template<>
inline void TxnMapChurnTest<std::string,std::string,TxnType::NBTC>::doPrefill(GlobalTestConfig* gtc){
	// randomly prefill until specified amount of keys are successfully inserted
	if (this->prefill > 0){
		std::mt19937_64 gen_k(0);
		// int stride = this->range/this->prefill;
		int i = 0;
		while(i<this->prefill){
			std::string k = this->fromInt(gen_k()%range);
			m->insert(k,value_buffer,0);
			i++;
		}
		if(gtc->verbose){
			printf("Prefilled %d\n",i);
		}
		Recoverable* rec=dynamic_cast<Recoverable*>(m);
		if(rec){
			rec->sync();
		}
	}
}

template<>
inline void TxnMapChurnTest<std::string,std::string,TxnType::NBTC>::operation(uint64_t key, int op, int tid){
	std::string k = this->fromInt(key);
	// printf("%d.\n", r);
	
	if(op<this->prop_gets){
		m->get(k,tid);
	}
	else if(op<this->prop_puts){
		m->put(k,value_buffer,tid);
	}
	else if(op<this->prop_inserts){
		m->insert(k,value_buffer,tid);
	}
	else{ // op<=prop_removes
		m->remove(k,tid);
	}
}

#endif