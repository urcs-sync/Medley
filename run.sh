#!/bin/bash
# THREADS=(1 4 8 12 16 20 24 32 36 40 48 62 72 80 90)
# VALUE_SIZES=(64 128 256 512 1024 2048 4096)

# go to root (./)
cd "$( dirname "${BASH_SOURCE[0]}" )"

outfile_dir="data"
THREADS=(1 4 8 12 16 20 24 32 36 40 48 62 72 80)
TASK_LENGTH=30 # length of each workload in second
REPEAT_NUM=3 # number of trials

# txMontage persistent lockfree hashtable that supports transactions
pm_ht_plain="txMontageLfHashTable<uint64_t>" 
# Medley transient lockfree hashtable that supports transactions
dram_ht_plain=(
    "MedleyLfHashTable<uint64_t>"
) 
# OneFile-backed lockfree hashtable that supports transactions
of_ht_plain=(
    "OneFileHashTable<uint64_t>"
    "POneFileHashTable<uint64_t>"
) 

# txMontage persistent lockfree skiplist that supports transactions
pm_sl_plain="txMontageFraserSkipList<uint64_t>" 
# Medley transient lockfree skiplist that supports transactions
dram_sl_plain=(
    "MedleyFraserSkipList<uint64_t>"
)
# OneFile-backed lockfree skiplist that supports transactions
of_sl_plain=(
    "OneFileSkipList<uint64_t>"
    "POneFileSkipList<uint64_t>"
)
# TDSL-backed lockfree skiplist that supports transactions
tdsl_sl_plain="TDSLSkipList<uint64_t>"
# LFTT-backed lockfree skiplist that supports transactions
lftt_sl_plain="LFTTSkipList<uint64_t>"

# Baseline test without any transaction
no_map_test_write="TxnMapChurnTest<uint64_t:None>:txn10:g0p0i50rm50:range=1000000:prefill=500000"
no_map_test_readmost="TxnMapChurnTest<uint64_t:None>:txn10:g90p0i5rm5:range=1000000:prefill=500000"
no_map_test_5050="TxnMapChurnTest<uint64_t:None>:txn10:g50p0i25rm25:range=1000000:prefill=500000"

# Transaction test for Medley/txMontage
txmon_map_test_write="TxnMapChurnTest<uint64_t:NBTC>:txn10:g0p0i50rm50:range=1000000:prefill=500000"
txmon_map_test_readmost="TxnMapChurnTest<uint64_t:NBTC>:txn10:g90p0i5rm5:range=1000000:prefill=500000"
txmon_map_test_5050="TxnMapChurnTest<uint64_t:NBTC>:txn10:g50p0i25rm25:range=1000000:prefill=500000"

# Transaction test for OneFile
of_map_test_write="TxnMapChurnTest<uint64_t:OneFile>:txn10:g0p0i50rm50:range=1000000:prefill=500000"
of_map_test_readmost="TxnMapChurnTest<uint64_t:OneFile>:txn10:g90p0i5rm5:range=1000000:prefill=500000"
of_map_test_5050="TxnMapChurnTest<uint64_t:OneFile>:txn10:g50p0i25rm25:range=1000000:prefill=500000"

# Transaction test for TDSL
tdsl_map_test_write="TxnMapChurnTest<uint64_t:TDSL>:txn10:g0p0i50rm50:range=1000000:prefill=500000"
tdsl_map_test_readmost="TxnMapChurnTest<uint64_t:TDSL>:txn10:g90p0i5rm5:range=1000000:prefill=500000"
tdsl_map_test_5050="TxnMapChurnTest<uint64_t:TDSL>:txn10:g50p0i25rm25:range=1000000:prefill=500000"

# Transaction test for LFTT
lftt_map_test_write="TxnMapChurnTest<uint64_t:LFTT>:txn10:g0p0i50rm50:range=1000000:prefill=500000"
lftt_map_test_readmost="TxnMapChurnTest<uint64_t:LFTT>:txn10:g90p0i5rm5:range=1000000:prefill=500000"
lftt_map_test_5050="TxnMapChurnTest<uint64_t:LFTT>:txn10:g50p0i25rm25:range=1000000:prefill=500000"

# TPCC test for Medley/txMontage, Onefile, and TDSL
tpcc_mon="TPCC<NBTC>"
tpcc_of="TPCC<OneFile>"
tpcc_tdsl="TPCC<TDSL>"


delete_heap_file(){
    # rm -rf /mnt/pmem/${USER}* /mnt/pmem/savitar.cat /mnt/pmem/psegments
    # rm -f /mnt/pmem/*.log /mnt/pmem/snapshot*
    rm -rf /mnt/pmem/*
    sleep 1
}

ht_init(){
    echo "Running hash tables, g0i50r50, g50i25r25 and g90i5r5 for $TASK_LENGTH seconds"
    rm -rf $outfile_dir/hts_g0i50r50_thread.csv $outfile_dir/hts_g50i25r25_thread.csv $outfile_dir/hts_g90i5r5_thread.csv
    echo "thread,ops,ds,test" > $outfile_dir/hts_g0i50r50_thread.csv
    echo "thread,ops,ds,test" > $outfile_dir/hts_g50i25r25_thread.csv
    echo "thread,ops,ds,test" > $outfile_dir/hts_g90i5r5_thread.csv
}

ht_execute(){
    make clean;make -j
    ht_init
    # 1. Transient Medley
    for ((i=1; i<=REPEAT_NUM; ++i))
    do
        for threads in "${THREADS[@]}"
        do
            for rideable in "${dram_ht_plain[@]}"
            do
                delete_heap_file
                echo -n "g0i50r50,"
                ./bin/main -R $rideable -M $txmon_map_test_write -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/hts_g0i50r50_thread.csv

                delete_heap_file
                echo -n "g50i25r25,"
                ./bin/main -R $rideable -M $txmon_map_test_5050 -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/hts_g50i25r25_thread.csv

                delete_heap_file
                echo -n "g90i5r5,"
                ./bin/main -R $rideable -M $txmon_map_test_readmost -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/hts_g90i5r5_thread.csv
            done
        done
    done

    # 2. Persistent txMontage 
    for ((i=1; i<=REPEAT_NUM; ++i))
    do
        for threads in "${THREADS[@]}"
        do
            # delete_heap_file
            # echo -n "g0i50r50,"
            # ./bin/main -R $pm_ht_plain -M $txmon_map_test_write -t $threads -i $TASK_LENGTH -dLiveness=Blocking | tee -a $outfile_dir/hts_g0i50r50_thread.csv

            # delete_heap_file
            # echo -n "g50i25r25,"
            # ./bin/main -R $pm_ht_plain -M $txmon_map_test_5050 -t $threads -i $TASK_LENGTH -dLiveness=Blocking | tee -a $outfile_dir/hts_g50i25r25_thread.csv

            # delete_heap_file
            # echo -n "g90i5r5,"
            # ./bin/main -R $pm_ht_plain -M $txmon_map_test_readmost -t $threads -i $TASK_LENGTH -dLiveness=Blocking | tee -a $outfile_dir/hts_g90i5r5_thread.csv

            delete_heap_file
            echo -n "g0i50r50,"
            ./bin/main -R $pm_ht_plain -M $txmon_map_test_write -t $threads -i $TASK_LENGTH -dLiveness=Nonblocking | tee -a $outfile_dir/hts_g0i50r50_thread.csv

            delete_heap_file
            echo -n "g50i25r25,"
            ./bin/main -R $pm_ht_plain -M $txmon_map_test_5050 -t $threads -i $TASK_LENGTH -dLiveness=Nonblocking | tee -a $outfile_dir/hts_g50i25r25_thread.csv

            delete_heap_file
            echo -n "g90i5r5,"
            ./bin/main -R $pm_ht_plain -M $txmon_map_test_readmost -t $threads -i $TASK_LENGTH -dLiveness=Nonblocking | tee -a $outfile_dir/hts_g90i5r5_thread.csv
        done
    done

    # 3. OneFile
    for ((i=1; i<=REPEAT_NUM; ++i))
    do
        for threads in "${THREADS[@]}"
        do
            for rideable in "${of_ht_plain[@]}"
            do
                delete_heap_file
                echo -n "g0i50r50,"
                ./bin/main -R $rideable -M $of_map_test_write -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/hts_g0i50r50_thread.csv

                delete_heap_file
                echo -n "g50i25r25,"
                ./bin/main -R $rideable -M $of_map_test_5050 -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/hts_g50i25r25_thread.csv

                delete_heap_file
                echo -n "g90i5r5,"
                ./bin/main -R $rideable -M $of_map_test_readmost -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/hts_g90i5r5_thread.csv
            done
        done
    done
}


sl_init(){
    echo "Running skiplists, g0i50r50, g50i25r25 and g90i5r5 for $TASK_LENGTH seconds"
    rm -rf $outfile_dir/sls_g0i50r50_thread.csv $outfile_dir/sls_g50i25r25_thread.csv $outfile_dir/sls_g90i5r5_thread.csv
    echo "thread,ops,ds,test" > $outfile_dir/sls_g0i50r50_thread.csv
    echo "thread,ops,ds,test" > $outfile_dir/sls_g50i25r25_thread.csv
    echo "thread,ops,ds,test" > $outfile_dir/sls_g90i5r5_thread.csv
}

sl_execute(){
    make clean;make -j
    sl_init
    # 1. Transient Medley
    for ((i=1; i<=REPEAT_NUM; ++i))
    do
        for threads in "${THREADS[@]}"
        do
            for rideable in "${dram_sl_plain[@]}"
            do
                delete_heap_file
                echo -n "g0i50r50,"
                ./bin/main -R $rideable -M $txmon_map_test_write -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_g0i50r50_thread.csv

                delete_heap_file
                echo -n "g50i25r25,"
                ./bin/main -R $rideable -M $txmon_map_test_5050 -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_g50i25r25_thread.csv

                delete_heap_file
                echo -n "g90i5r5,"
                ./bin/main -R $rideable -M $txmon_map_test_readmost -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_g90i5r5_thread.csv
            done
        done
    done

    # 2. Persistent txMontage
    for ((i=1; i<=REPEAT_NUM; ++i))
    do
        for threads in "${THREADS[@]}"
        do
            # delete_heap_file
            # echo -n "g0i50r50,"
            # ./bin/main -R $pm_sl_plain -M $txmon_map_test_write -t $threads -i $TASK_LENGTH -dLiveness=Blocking | tee -a $outfile_dir/sls_g0i50r50_thread.csv

            # delete_heap_file
            # echo -n "g50i25r25,"
            # ./bin/main -R $pm_sl_plain -M $txmon_map_test_5050 -t $threads -i $TASK_LENGTH -dLiveness=Blocking | tee -a $outfile_dir/sls_g50i25r25_thread.csv

            # delete_heap_file
            # echo -n "g90i5r5,"
            # ./bin/main -R $pm_sl_plain -M $txmon_map_test_readmost -t $threads -i $TASK_LENGTH -dLiveness=Blocking | tee -a $outfile_dir/sls_g90i5r5_thread.csv

            delete_heap_file
            echo -n "g0i50r50,"
            ./bin/main -R $pm_sl_plain -M $txmon_map_test_write -t $threads -i $TASK_LENGTH -dLiveness=Nonblocking | tee -a $outfile_dir/sls_g0i50r50_thread.csv

            delete_heap_file
            echo -n "g50i25r25,"
            ./bin/main -R $pm_sl_plain -M $txmon_map_test_5050 -t $threads -i $TASK_LENGTH -dLiveness=Nonblocking | tee -a $outfile_dir/sls_g50i25r25_thread.csv

            delete_heap_file
            echo -n "g90i5r5,"
            ./bin/main -R $pm_sl_plain -M $txmon_map_test_readmost -t $threads -i $TASK_LENGTH -dLiveness=Nonblocking | tee -a $outfile_dir/sls_g90i5r5_thread.csv
        done
    done

    # 3. OneFile
    for ((i=1; i<=REPEAT_NUM; ++i))
    do
        for threads in "${THREADS[@]}"
        do
            for rideable in "${of_sl_plain[@]}"
            do
                delete_heap_file
                echo -n "g0i50r50,"
                ./bin/main -R $rideable -M $of_map_test_write -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_g0i50r50_thread.csv

                delete_heap_file
                echo -n "g50i25r25,"
                ./bin/main -R $rideable -M $of_map_test_5050 -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_g50i25r25_thread.csv

                delete_heap_file
                echo -n "g90i5r5,"
                ./bin/main -R $rideable -M $of_map_test_readmost -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_g90i5r5_thread.csv
            done
        done
    done

    # 4. TDSL
    for ((i=1; i<=REPEAT_NUM; ++i))
    do
        for threads in "${THREADS[@]}"
        do
            delete_heap_file
            echo -n "g0i50r50,"
            ./bin/main -R $tdsl_sl_plain -M $tdsl_map_test_write -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_g0i50r50_thread.csv

            delete_heap_file
            echo -n "g50i25r25,"
            ./bin/main -R $tdsl_sl_plain -M $tdsl_map_test_5050 -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_g50i25r25_thread.csv

            delete_heap_file
            echo -n "g90i5r5,"
            ./bin/main -R $tdsl_sl_plain -M $tdsl_map_test_readmost -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_g90i5r5_thread.csv
        done
    done

    # 5. LFTT
    ### Integrating LFTT into our testing framework results in slow
    ### performance. We can't figure out why and which side the bug is
    ### on, so we instead run it with its native testing framework for fairness.
    ext/lftt/txn_run.sh
}

tpcc_init(){
    echo "Running tpcc on hash tables for $TASK_LENGTH seconds"
    rm -rf $outfile_dir/tpcc.csv
    echo "thread,ops,ds,test" > $outfile_dir/tpcc.csv
}

tpcc_execute(){
    tpcc_init
    for ((i=1; i<=REPEAT_NUM; ++i))
    do
        make clean;make medley_tpcc -j
        for threads in "${THREADS[@]}"
        do
            delete_heap_file
            ./bin/main -R ${dram_sl_plain[0]} -M $tpcc_mon -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/tpcc.csv

        done
        
        make clean;make txmon_tpcc -j
        for threads in "${THREADS[@]}"
        do
            delete_heap_file
            ./bin/main -R $pm_sl_plain -M $tpcc_mon -t $threads -dLiveness=Nonblocking -i $TASK_LENGTH | tee -a $outfile_dir/tpcc.csv
        done
        
        make clean;make of_tpcc -j
        for threads in "${THREADS[@]}"
        do
            delete_heap_file
            ./bin/main -R ${of_sl_plain[0]} -M $tpcc_of -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/tpcc.csv
        done
        
        ### Too slow to finish, skipping
        # make clean;make pof_tpcc -j
        # for threads in "${THREADS[@]}"
        # do
        #     delete_heap_file
        #     ./bin/main -R ${of_sl_plain[1]} -M $tpcc_of -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/tpcc.csv
        # done

        make clean;make tdsl_tpcc -j
        for threads in "${THREADS[@]}"
        do
            delete_heap_file
            ./bin/main -R $tdsl_sl_plain -M $tpcc_tdsl -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/tpcc.csv
        done
    done
}


sls_latency_init(){
    echo "Running latency on skiplist for $TASK_LENGTH seconds"
    rm -rf $outfile_dir/sls_latency.csv
    echo "thread,ops,ds,test" > $outfile_dir/sls_latency.csv
}

sls_latency_execute(){
    sls_latency_init
    threads=40

    make clean;make -j
    for ((i=1; i<=REPEAT_NUM; ++i))
    do
        # Original DRAM
        delete_heap_file
        echo -n "g0i50r50,"
        ./bin/main -R "FraserSkipList<uint64_t>" -M $no_map_test_write -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g50i25r25,"
        ./bin/main -R "FraserSkipList<uint64_t>" -M $no_map_test_5050 -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g90i5r5,"
        ./bin/main -R "FraserSkipList<uint64_t>" -M $no_map_test_readmost -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        # Original NVM
        delete_heap_file
        echo -n "g0i50r50,"
        ./bin/main -R "NVMFraserSkipList<uint64_t>" -M $no_map_test_write -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g50i25r25,"
        ./bin/main -R "NVMFraserSkipList<uint64_t>" -M $no_map_test_5050 -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g90i5r5,"
        ./bin/main -R "NVMFraserSkipList<uint64_t>" -M $no_map_test_readmost -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        # Medley no txn
        delete_heap_file
        echo -n "g0i50r50,"
        ./bin/main -R ${dram_sl_plain[0]} -M $no_map_test_write -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g50i25r25,"
        ./bin/main -R ${dram_sl_plain[0]} -M $no_map_test_5050 -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g90i5r5,"
        ./bin/main -R ${dram_sl_plain[0]} -M $no_map_test_readmost -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        # Montage no txn no persist
        delete_heap_file
        echo -n "g0i50r50,"
        ./bin/main -R $pm_sl_plain -M $no_map_test_write -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g50i25r25,"
        ./bin/main -R $pm_sl_plain -M $no_map_test_5050 -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g90i5r5,"
        ./bin/main -R $pm_sl_plain -M $no_map_test_readmost -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        # Montage no txn persist
        delete_heap_file
        echo -n "g0i50r50,"
        ./bin/main -R $pm_sl_plain -M $no_map_test_write -t $threads -dLiveness=Nonblocking -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g50i25r25,"
        ./bin/main -R $pm_sl_plain -M $no_map_test_5050 -t $threads -dLiveness=Nonblocking -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g90i5r5,"
        ./bin/main -R $pm_sl_plain -M $no_map_test_readmost -t $threads -dLiveness=Nonblocking -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        # Medley txn
        delete_heap_file
        echo -n "g0i50r50,"
        ./bin/main -R ${dram_sl_plain[0]} -M $txmon_map_test_write -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g50i25r25,"
        ./bin/main -R ${dram_sl_plain[0]} -M $txmon_map_test_5050 -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g90i5r5,"
        ./bin/main -R ${dram_sl_plain[0]} -M $txmon_map_test_readmost -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        # Montage txn no persist
        delete_heap_file
        echo -n "g0i50r50,"
        ./bin/main -R $pm_sl_plain -M $txmon_map_test_write -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g50i25r25,"
        ./bin/main -R $pm_sl_plain -M $txmon_map_test_5050 -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g90i5r5,"
        ./bin/main -R $pm_sl_plain -M $txmon_map_test_readmost -t $threads -dPersistStrat=No -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        # Montage txn persist
        delete_heap_file
        echo -n "g0i50r50,"
        ./bin/main -R $pm_sl_plain -M $txmon_map_test_write -t $threads -dLiveness=Nonblocking -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g50i25r25,"
        ./bin/main -R $pm_sl_plain -M $txmon_map_test_5050 -t $threads -dLiveness=Nonblocking -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv

        delete_heap_file
        echo -n "g90i5r5,"
        ./bin/main -R $pm_sl_plain -M $txmon_map_test_readmost -t $threads -dLiveness=Nonblocking -i $TASK_LENGTH | tee -a $outfile_dir/sls_latency.csv
    done
}


########################
###       Main       ###
########################
ht_execute
sl_execute
tpcc_execute
sls_latency_execute