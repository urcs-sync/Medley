#include <iostream>
#include <time.h>
#include <random>
#include <thread>
#include <atomic>

#include "tskiplist/Utils.h"
#include "tskiplist/TSkipList.h"

using namespace std;

enum WorkloadType
{
    READ_ONLY = 0,
    MIXED = 1,
    UPDATE_ONLY = 2
};

unsigned int constexpr WARM_UP_NUM_KEYS = 100000;
unsigned int constexpr MIN_KEY_VAL = 1;
unsigned int constexpr MAX_KEY_VAL = 1000000;
unsigned int constexpr TIMEOUT = 10;


void warmUp(tdsl::SkipList<ItemType, ValueType> & sl)
{
    minstd_rand generator;
    uniform_int_distribution<int> distribution(MIN_KEY_VAL, MAX_KEY_VAL);

    cout << "Warming up..." << endl;
    for (auto i = 0; i < WARM_UP_NUM_KEYS; i++) {
        int percent = (int)((i / float(WARM_UP_NUM_KEYS)) * 100);

        tdsl::SkipListTransaction trans;
#ifdef STRING_KV
        ItemType key = std::to_string(distribution(generator));
#else
        ItemType key = distribution(generator);
#endif
        trans.TXBegin();
        sl.insert(key, key, trans);
        trans.TXCommit();
    }

    cout << "Finished" << endl;
}

void chooseOps(WorkloadType wtype, uint32_t numOps,
               vector<tdsl::OperationType> & outOps)
{
    switch (wtype) {
    case READ_ONLY:
        for (uint32_t i = 0; i < numOps; i++) {
            outOps.at(i) = tdsl::OperationType::GET;
        }
        break;
    case UPDATE_ONLY: {
        const uint32_t halfPoint = (uint32_t)(numOps / 2.0);
        for (uint32_t i = 0; i < halfPoint; i++) {
            outOps.at(i) = tdsl::OperationType::PUT;
        }
        for (uint32_t i = halfPoint; i < numOps; i++) {
            outOps.at(i) = tdsl::OperationType::REMOVE;
        }
        break;
    }
    case MIXED: {
        const uint32_t halfPoint = (uint32_t)(numOps / 2.0);
        const uint32_t threeQuarters = (uint32_t)(numOps * 3.0 / 4.0);
        for (uint32_t i = 0; i < halfPoint; i++) {
            outOps.at(i) = tdsl::OperationType::GET;
        }
        for (uint32_t i = halfPoint; i < threeQuarters; i++) {
            outOps.at(i) = tdsl::OperationType::REMOVE;
        }
        for (uint32_t i = threeQuarters; i < numOps; i++) {
            outOps.at(i) = tdsl::OperationType::PUT;
        }
        break;
    }
    }
}

void performOp(tdsl::SkipList<ItemType, ValueType> * sl, tdsl::OperationType & opType,
               tdsl::SkipListTransaction & trans,
               ItemType key)
{
    if (opType == tdsl::OperationType::CONTAINS) {
        sl->contains(key, trans);
    } else if (opType == tdsl::OperationType::INSERT) {
        if (sl->insert(key, key, trans)) {
        }
    } else if (opType == tdsl::OperationType::REMOVE) {
        sl->remove(key, trans);
    } else if (opType == tdsl::OperationType::GET) {
        sl->get(key, trans);
    } else if (opType == tdsl::OperationType::PUT) {
        sl->put(key, key, trans);
    }
}

void worker(tdsl::SkipList<ItemType, ValueType> * sl, atomic<uint32_t> * opsCounter,
            atomic<uint32_t> * abortCounter,
            WorkloadType wtype, time_t end)
{
    minstd_rand generator;
    uniform_int_distribution<int> key_distribution(MIN_KEY_VAL, MAX_KEY_VAL);
    uniform_int_distribution<uint32_t> transaction_distribution(1, 7);

    while (time(NULL) < end) {
        int numOps = transaction_distribution(generator);

        vector<tdsl::OperationType> ops(numOps);
        chooseOps(wtype, numOps, ops);

        tdsl::SkipListTransaction trans;
        trans.TXBegin();
        try {
            for (uint32_t i = 0; i < numOps; i++) {
#ifdef STRING_KV
                ItemType key = std::to_string(key_distribution(generator));
#else
                ItemType key = key_distribution(generator);
#endif
                performOp(sl, ops[i], trans, key);
            }
            trans.TXCommit();
            atomic_fetch_add<uint32_t>(opsCounter, numOps);
        } catch (AbortTransactionException &) {
            atomic_fetch_add<uint32_t>(abortCounter, 1);
        }
    }
}

int main(int argc, char * argv[])
{
    if (argc != 3) {
        cout << "Invalid number of parameters" << endl;
        cout << "Usage: " << argv[0] << " <WORKLOAD_TYPE> <NUM_THREADS>" << endl;
        cout << "Workload types: 0 = READ_ONLY, 1 = MIXED, 2 = UPDATE_ONLY" << endl;
        return 1;
    }

    srand(time(NULL));

    WorkloadType wtype = (WorkloadType)(atoi(argv[1]));

    tdsl::SkipList<ItemType, ValueType> sl;
    warmUp(sl);

    uint32_t numThreads = atoi(argv[2]);
    vector<thread> threads;

    time_t end = time(NULL) + TIMEOUT;

    atomic<uint32_t> opsCounter(0);
    atomic<uint32_t> abortCounter(0);

    for (uint32_t i = 0; i < numThreads; i++) {
        threads.push_back(thread(worker, &sl, &opsCounter, &abortCounter,
                                 wtype, end));
    }

    for (auto & t : threads) {
        t.join();
    }

    cout << "Num ops: " << opsCounter << endl;
    cout << "Num aborts: " << abortCounter << endl;
    return 0;
}