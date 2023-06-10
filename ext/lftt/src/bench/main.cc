//------------------------------------------------------------------------------
// 
//     Testing different priority queues
//
//------------------------------------------------------------------------------

#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <vector>
#include <array>
#include <set>
#include <execinfo.h>
#include <sys/mman.h>
#include <thread>
#include <mutex>
#include <errno.h> 
#include <signal.h>
#include <boost/random.hpp>
#include <sched.h>
#include <chrono>
#include <pthread.h>
#include <unistd.h>
#include "common/timehelper.h"
#include "common/threadbarrier.h"
#include "bench/lfttsetadaptor.h"
#include "bench/mapadaptor.h"

bool testComplete;
void alarmhandler(int sig){
	if(testComplete==false){
		fprintf(stderr,"Time out error.\n");
		void *buf[30];
        size_t sz;

        // scrape backtrace
        sz = backtrace(buf, 30);

        // print error msg
        fprintf(stderr, "Error: signal %d:\n", sig);
        backtrace_symbols_fd(buf, sz, STDERR_FILENO);
        exit(1);
	}
}

template<typename T>
void WorkThread(uint32_t numThread, int threadId, uint32_t testSize, uint32_t tranSize, uint32_t keyRange, uint32_t insertion, uint32_t deletion, ThreadBarrier& barrier,  T& set)
{
    const int pinning_map_2x20a_1[] = {
 	0,2,4,6,8,10,12,14,16,18,
 	20,22,24,26,28,30,32,34,36,38,
 	40,42,44,46,48,50,52,54,56,58,
 	60,62,64,66,68,70,72,74,76,78,
 	1,3,5,7,9,11,13,15,17,19,
 	21,23,25,27,29,31,33,35,37,39,
 	41,43,45,47,49,51,53,55,57,59,
 	61,63,65,67,69,71,73,75,77,79};
    //set affinity for each thread
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    int core_id = pinning_map_2x20a_1[threadId%80];
    CPU_SET(core_id, &cpuset);
    int set_result = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (set_result != 0){
        fprintf(stderr, "setaffinity failed for thread %d to cpu %d\n", threadId, core_id);
        exit(1);
    }
    int get_result = pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (get_result != 0){
        fprintf(stderr, "getaffinity failed for thread %d to cpu %d\n", threadId, core_id);
        exit(1);
    }
    if (!CPU_ISSET(core_id, &cpuset)){
        fprintf(stderr, "WARNING: thread aiming for cpu %d is pinned elsewhere.\n", core_id);	 
    } else {
    	// fprintf(stderr, "thread pinning on cpu %d succeeded.\n", core_id);
    }

    boost::mt19937 randomGenKey;
    boost::mt19937 randomGenOp;
    boost::mt19937 randomGenSize;
    double startTime = Time::GetWallTime();
    randomGenKey.seed(startTime + threadId);
    randomGenOp.seed(startTime + threadId + 1000);
    randomGenSize.seed(startTime + threadId + 10);
    boost::uniform_int<uint32_t> randomDistKey(1, keyRange);
    boost::uniform_int<uint32_t> randomDistOp(1, 100);
    boost::uniform_int<uint32_t> randomDistSize(1, tranSize);
    
    set.Init();

    auto now = std::chrono::high_resolution_clock::now();
    auto time_up = now+std::chrono::seconds{(uint64_t)testSize};

    barrier.Wait();
    
    int cnt = 0;

    while(std::chrono::duration_cast<std::chrono::microseconds>(time_up - now).count()>0){
        uint32_t real_tran_size = randomDistSize(randomGenSize);
        SetOpArray ops(real_tran_size);
        for(uint32_t t = 0; t < real_tran_size; ++t)
        {
            uint32_t op_dist = randomDistOp(randomGenOp);
            ops[t].type = op_dist <= insertion ? INSERT : op_dist <= insertion + deletion ? DELETE : FIND;
            ops[t].key  = randomDistKey(randomGenKey);
            ops[t].val  = ops[t].key;
        }

        if (set.ExecuteOps(ops)) cnt++;

        if (cnt % 512 == 0){
            now = std::chrono::high_resolution_clock::now();
        }
    }

    set.Uninit();
}


template<typename T>
void Tester(uint32_t numThread, uint32_t testSize, uint32_t tranSize, uint32_t keyRange, uint32_t insertion, uint32_t deletion,  SetAdaptor<T>& set)
{
    std::vector<std::thread> thread(numThread);
    ThreadBarrier barrier(numThread + 1);

    double startTime = Time::GetWallTime();
    boost::mt19937 randomGen;
    randomGen.seed(startTime - 10);
    boost::uniform_int<uint32_t> randomDist(1, keyRange);

    set.Init();

    SetOpArray ops(1);


    // TODO: don't count the aborts caused in the prefill? the prefill is already untimed
    for(unsigned int i = 0; i < keyRange/2; ++i)
    {
        ops[0].type = INSERT;
        ops[0].key  = randomDist(randomGen);
        ops[0].val  = ops[0].key;
        set.ExecuteOps(ops);

        // if (i % 10000 == 0)
        // {
        // 	printf("%d\t", i);
        // 	fflush(stdout);
        // }

    }

    //Create joinable threads
    for (unsigned i = 0; i < numThread; i++) 
    {
        thread[i] = std::thread(WorkThread<SetAdaptor<T> >, numThread, i + 1, testSize, tranSize, keyRange, insertion, deletion, std::ref(barrier), std::ref(set));
    }
	signal(SIGALRM, &alarmhandler);  // set a signal handler
    alarm(testSize+30);
    barrier.Wait();

    auto start = std::chrono::high_resolution_clock::now();

    //Wait for the threads to finish
    for (unsigned i = 0; i < thread.size(); i++) 
    {
        thread[i].join();
    }
    auto now = std::chrono::high_resolution_clock::now();
    uint32_t count_commit = set.Uninit();
    printf("%d,%ld,LFTTSkipList<uint64_t>,TxnMapChurnTest<uint64_t:LFTT>:txn%d:g%dp0i%drm%d:range=%d:prefill=%d\n",
    numThread,
    (uint64_t)(count_commit/(std::chrono::duration_cast<std::chrono::microseconds>(now - start).count()/1000000.0)), 
    tranSize,
    100-insertion-deletion,insertion,deletion,
    keyRange,keyRange/2);

}

template<typename T>
void MapWorkThread(uint32_t numThread, int threadId, uint32_t testSize, uint32_t tranSize, uint32_t keyRange, uint32_t insertion, uint32_t deletion, uint32_t update, ThreadBarrier& barrier,  T& map)
{
    //set affinity for each thread
    cpu_set_t cpu = {{0}};
    CPU_SET(threadId, &cpu);
    sched_setaffinity(0, sizeof(cpu_set_t), &cpu);

    double startTime = Time::GetWallTime();

    boost::mt19937 randomGenKey;
    boost::mt19937 randomGenOp;
    randomGenKey.seed(startTime + threadId);
    randomGenOp.seed(startTime + threadId + 1000);
    boost::uniform_int<uint32_t> randomDistKey(1, keyRange);
    boost::uniform_int<uint32_t> randomDistOp(1, 100);
    
    map.Init();

    barrier.Wait();
    
    MapOpArray ops(tranSize);

    for(unsigned int i = 0; i < testSize; ++i)
    {
        for(uint32_t t = 0; t < tranSize; ++t)
        {
            uint32_t op_dist = randomDistOp(randomGenOp);
            //ops[t].type = op_dist <= insertion ? INSERT : op_dist <= insertion + deletion ? DELETE : FIND;
            if(op_dist <= insertion)
            {
                ops[t].type = MAP_INSERT;
                ops[t].value = randomDistKey(randomGenKey);
            }
            else if(op_dist <= insertion + deletion)
            {
                ops[t].type = MAP_DELETE;
                ops[t].value = 0;
            }
            else if(op_dist <= insertion + deletion + update)
            {
                ops[t].type = MAP_UPDATE;
                ops[t].value = randomDistKey(randomGenKey);
            }
            else
            {
                ops[t].type = MAP_FIND;
                ops[t].value = 0;
            }

            ops[t].key  = randomDistKey(randomGenKey);
        }

        //std::vector<VALUE> toR;
        map.ExecuteOps(ops, threadId);//, toR);
    }

    map.Uninit();
}


template<typename T>
void MapTester(uint32_t numThread, uint32_t testSize, uint32_t tranSize, uint32_t keyRange, uint32_t insertion, uint32_t deletion, uint32_t update, MapAdaptor<T>& map)
{
    std::vector<std::thread> thread(numThread);
    ThreadBarrier barrier(numThread + 1);

    double startTime = Time::GetWallTime();
    boost::mt19937 randomGen;
    randomGen.seed(startTime - 10);
    boost::uniform_int<uint32_t> randomDist(1, keyRange);

    map.Init();

    MapOpArray ops(1);

    for(unsigned int i = 0; i < keyRange; ++i)
    {
        //std::vector<VALUE> toR;
        ops[0].type = INSERT;
        ops[0].key  = randomDist(randomGen);
        ops[0].value = randomDist(randomGen);
        // all prefill gets done by thread 0, the main thread; worker threads start numbering at 1
        map.ExecuteOps(ops, 0);//, toR); 
    }

    //Create joinable threads
    for (unsigned i = 0; i < numThread; i++) 
    {
        thread[i] = std::thread(MapWorkThread<MapAdaptor<T> >, numThread, i + 1, testSize, tranSize, keyRange, insertion, deletion, update, std::ref(barrier), std::ref(map));
    }

    barrier.Wait();

    {
        ScopedTimer timer(true);

        //Wait for the threads to finish
        for (unsigned i = 0; i < thread.size(); i++) 
        {
            thread[i].join();
        }
    }

    map.Uninit();
}

int main(int argc, const char *argv[])
{
    uint32_t setType = 0;
    uint32_t numThread = 1;
    uint32_t testSize = 100;
    uint32_t tranSize = 1;
    uint32_t keyRange = 100;
    uint32_t insertion = 50;
    uint32_t deletion = 50;
    uint32_t update = 0;

    if(argc > 1) setType = atoi(argv[1]);
    if(argc > 2) numThread = atoi(argv[2]);
    if(argc > 3) testSize = atoi(argv[3]);
    if(argc > 4) tranSize = atoi(argv[4]);
    if(argc > 5) keyRange = atoi(argv[5]);
    if(argc > 6) insertion = atoi(argv[6]);
    if(argc > 7) deletion = atoi(argv[7]);
    if(argc > 8) update = atoi(argv[8]);

    assert(setType < 10);
    assert(keyRange < 0xffffffff);

    const char* setName[] = 
    {   "TransList", 
        "RSTMList",
        "BoostingList",
        "TransSkip",
        "BoostingSkip",
        "OSTMSkip",
        "TransMap",
        "BoostingMap"
        "ObsSkip",
        "ObsList"
    };

    fprintf(stderr,"Start testing %s with %d threads %d seconds %d txnsize %d unique keys %d%% insert %d%% delete %d%% update.\n", setName[setType], numThread, testSize, tranSize, keyRange, insertion, deletion, update);//(insertion + deletion) >= 100 ? 100 - insertion : deletion, update);

    uint64_t numNodes = keyRange + 20000000; //note: is this too much?

    switch(setType)
    {
    case 0:
        { SetAdaptor<TransList> set(numNodes, numThread + 1, tranSize); Tester(numThread, testSize, tranSize, keyRange, insertion, deletion, set); }
        break;
    case 3:
        { SetAdaptor<trans_skip> set(numNodes, numThread + 1, tranSize); Tester(numThread, testSize, tranSize, keyRange, insertion, deletion, set); }
    break;
    case 5:
        { SetAdaptor<stm_skip> set; Tester(numThread, testSize, tranSize, keyRange, insertion, deletion, set); }
        break;
    case 6: //NOTE: the transmap gets constructed with numthread + 1 as the the threadcount
        { MapAdaptor<TransMap> map(numNodes, numThread + 1, tranSize); MapTester(numThread, testSize, tranSize, keyRange, insertion, deletion, update, map); }
        break;
    case 8:
        { SetAdaptor<obs_skip> set(testSize, numThread + 1, tranSize); Tester(numThread, testSize, tranSize, keyRange, insertion, deletion, set); }
        break;
    case 9:
        { SetAdaptor<ObsList> set(testSize, numThread + 1, tranSize); Tester(numThread, testSize, tranSize, keyRange, insertion, deletion, set); }
        break;
    default:
        break;
    }

    return 0;
}
