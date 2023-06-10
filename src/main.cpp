#include <iostream>
#include <string>
#include <thread>
#include <random>
#include <time.h>
#include <sys/time.h>

#include "ConcurrentPrimitives.hpp"
#include "HarnessUtils.hpp"
#include "CustomTypes.hpp"

#include "LockfreeHashTable.hpp"
#include "NVMLockfreeHashTable.hpp"

#include "txMontageLfHashTable.hpp"
#include "MedleyLfHashTable.hpp"

#include "TxnBoostingLfHashTable.hpp"

#include "OneFileHashTable.hpp"
#include "POneFileHashTable.hpp"

#include "FraserSkipList.hpp"
#include "NVMFraserSkipList.hpp"

#include "txMontageFraserSkipList.hpp"
#include "MedleyFraserSkipList.hpp"

#include "TxnBoostingFraserSkipList.hpp"

#include "OneFileSkipList.hpp"
#include "POneFileSkipList.hpp"

#include "TDSLSkipList.hpp"
#include "LFTTSkipList.hpp"

#include "MapChurnTest.hpp"
#include "TxnMapChurnTest.hpp"
#include "TxnVerify.hpp"
#include "TPCC.hpp"

using namespace std;


int main(int argc, char *argv[])
{
	GlobalTestConfig gtc;

	/* hash tables */
	gtc.addRideableOption(new LockfreeHashTableFactory<uint64_t>(), "LfHashTable<uint64_t>");
	gtc.addRideableOption(new NVMLockfreeHashTableFactory<uint64_t>(), "NVMLockfreeHashTable<uint64_t>");
	gtc.addRideableOption(new MedleyLfHashTableFactory<uint64_t>(), "MedleyLfHashTable<uint64_t>");
	gtc.addRideableOption(new txMontageLfHashTableFactory<uint64_t>(), "txMontageLfHashTable<uint64_t>");
	gtc.addRideableOption(new TxnBoostingLfHashTableFactory<uint64_t>(), "TxnBoostingLfHashTable<uint64_t>");
	gtc.addRideableOption(new OneFileHashTableFactory<uint64_t>(), "OneFileHashTable<uint64_t>");
	gtc.addRideableOption(new POneFileHashTableFactory<uint64_t>(), "POneFileHashTable<uint64_t>");

	/* skiplists */
	gtc.addRideableOption(new FraserSkipListFactory<uint64_t>(), "FraserSkipList<uint64_t>");
	gtc.addRideableOption(new NVMFraserSkipListFactory<uint64_t>(), "NVMFraserSkipList<uint64_t>");
	gtc.addRideableOption(new MedleyFraserSkipListFactory<uint64_t>(), "MedleyFraserSkipList<uint64_t>");
	gtc.addRideableOption(new txMontageFraserSkipListFactory<uint64_t>(), "txMontageFraserSkipList<uint64_t>");
	gtc.addRideableOption(new TxnBoostingFraserSkipListFactory<uint64_t>(), "TxnBoostingFraserSkipList<uint64_t>");
	gtc.addRideableOption(new OneFileSkipListFactory<uint64_t>(), "OneFileSkipList<uint64_t>");
	gtc.addRideableOption(new POneFileSkipListFactory<uint64_t>(), "POneFileSkipList<uint64_t>");
	gtc.addRideableOption(new TDSLSkipListFactory<uint64_t>(), "TDSLSkipList<uint64_t>");
	gtc.addRideableOption(new LFTTSkipListFactory(), "LFTTSkipList<uint64_t>");

	/* non-transactional microbenchmark */
	gtc.addTestOption(new MapChurnTest<uint64_t,uint64_t>(50, 0, 25, 25, 1000000, 500000), "MapChurnTest<uint64_t>:g50p0i25rm25:range=1000000:prefill=500000");

	/* transactional TPCC benchmark */
	gtc.addTestOption(new tpcc::TPCC<TxnType::NBTC>(50,50,0,0,0),"TPCC<NBTC>");
	gtc.addTestOption(new tpcc::TPCC<TxnType::TDSL>(50,50,0,0,0),"TPCC<TDSL>");
	gtc.addTestOption(new tpcc::TPCC<TxnType::OneFile>(50,50,0,0,0),"TPCC<OneFile>");

	/* transactional microbenchmark */
	// Read-most. Get:Insert:Remove=18:1:1
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::None>(false, 10, 90, 0, 5, 5, 1000000, 500000), "TxnMapChurnTest<uint64_t:None>:txn10:g90p0i5rm5:range=1000000:prefill=500000");
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::NBTC>(false, 10, 90, 0, 5, 5, 1000000, 500000), "TxnMapChurnTest<uint64_t:NBTC>:txn10:g90p0i5rm5:range=1000000:prefill=500000"); // Uses Medley/txMontage framework
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::TDSL>(false, 10, 90, 0, 5, 5, 1000000, 500000), "TxnMapChurnTest<uint64_t:TDSL>:txn10:g90p0i5rm5:range=1000000:prefill=500000");
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::LFTT>(false, 10, 90, 0, 5, 5, 1000000, 500000), "TxnMapChurnTest<uint64_t:LFTT>:txn10:g90p0i5rm5:range=1000000:prefill=500000");
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::OneFile>(false, 10, 90, 0, 5, 5, 1000000, 500000), "TxnMapChurnTest<uint64_t:OneFile>:txn10:g90p0i5rm5:range=1000000:prefill=500000");

	// Read-write. Get:Insert:Remove=2:1:1
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::None>(false, 10, 50, 0, 25, 25, 1000000, 500000), "TxnMapChurnTest<uint64_t:None>:txn10:g50p0i25rm25:range=1000000:prefill=500000");
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::NBTC>(false, 10, 50, 0, 25, 25, 1000000, 500000), "TxnMapChurnTest<uint64_t:NBTC>:txn10:g50p0i25rm25:range=1000000:prefill=500000"); // Uses Medley/txMontage framework
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::TDSL>(false, 10, 50, 0, 25, 25, 1000000, 500000), "TxnMapChurnTest<uint64_t:TDSL>:txn10:g50p0i25rm25:range=1000000:prefill=500000");
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::LFTT>(false, 10, 50, 0, 25, 25, 1000000, 500000), "TxnMapChurnTest<uint64_t:LFTT>:txn10:g50p0i25rm25:range=1000000:prefill=500000");
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::OneFile>(false, 10, 50, 0, 25, 25, 1000000, 500000), "TxnMapChurnTest<uint64_t:OneFile>:txn10:g50p0i25rm25:range=1000000:prefill=500000");

	// Write-only. Get:Insert:Remove=0:1:1
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::None>(false, 10, 0, 0, 50, 50, 1000000, 500000), "TxnMapChurnTest<uint64_t:None>:txn10:g0p0i50rm50:range=1000000:prefill=500000");
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::NBTC>(false, 10, 0, 0, 50, 50, 1000000, 500000), "TxnMapChurnTest<uint64_t:NBTC>:txn10:g0p0i50rm50:range=1000000:prefill=500000"); // Uses Medley/txMontage framework
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::TDSL>(false, 10, 0, 0, 50, 50, 1000000, 500000), "TxnMapChurnTest<uint64_t:TDSL>:txn10:g0p0i50rm50:range=1000000:prefill=500000");
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::LFTT>(false, 10, 0, 0, 50, 50, 1000000, 500000), "TxnMapChurnTest<uint64_t:LFTT>:txn10:g0p0i50rm50:range=1000000:prefill=500000");
	gtc.addTestOption(new TxnMapChurnTest<uint64_t,uint64_t,TxnType::OneFile>(false, 10, 0, 0, 50, 50, 1000000, 500000), "TxnMapChurnTest<uint64_t:OneFile>:txn10:g0p0i50rm50:range=1000000:prefill=500000");

	gtc.addTestOption(new TxnVerify<uint64_t, uint64_t>(30, 14,14, 14, 14,14, 500000,10), "TxnVerify<uint64_t>:g30:wa14:rb14:wb14:rc14:wc14:range=500000");
	

	gtc.parseCommandLine(argc, argv);
        omp_set_num_threads(gtc.task_num);
	gtc.runTest();

	// print out results
	if(gtc.verbose){
		printf("Operations/sec: %ld\n",(uint64_t)(gtc.total_operations/gtc.interval));
	}
	else{
		string rideable_name = gtc.getRideableName().c_str();
		if(gtc.getEnv("PersistStrat") == "No") {
			rideable_name = "NoPersist"+rideable_name;
		}
		if(gtc.getEnv("Liveness") == "Nonblocking") {
			rideable_name = "nb"+rideable_name;
		}
		printf("%d,%ld,%s,%s\n",gtc.task_num,(uint64_t)(gtc.total_operations/gtc.interval),rideable_name.c_str(),gtc.getTestName().c_str());
	}
}
