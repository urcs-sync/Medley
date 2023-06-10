#!/bin/bash
# THREADS=(1 4 8 12 16 20 24 32 36 40 48 62 72 80 90)
# VALUE_SIZES=(64 128 256 512 1024 2048 4096)

# go to lftt
cd "$( dirname "${BASH_SOURCE[0]}" )"

outfile_dir="../../data"
THREADS=(1 4 8 12 16 20 24 32 36 40 48 62 72 80 90)
TASK_LENGTH=10 # length of each workload in second
REPEAT_NUM=3 # number of trials

# lftt_sl_plain="LFTTSkipList<uint64_t>"

# lftt_map_test_write="TxnMapChurnTest<uint64_t:LFTT>:txn10:g0p0i50rm50:range=1000000:prefill=500000"

# lftt_map_test_readmost="TxnMapChurnTest<uint64_t:LFTT>:txn10:g90p0i5rm5:range=1000000:prefill=500000"

# lftt_map_test_5050="TxnMapChurnTest<uint64_t:LFTT>:txn10:g50p0i25rm25:range=1000000:prefill=500000"

sl_execute(){
    # 5. LFTT
    for ((i=1; i<=REPEAT_NUM; ++i))
    do
        for threads in "${THREADS[@]}"
        do
            trans-compile/src/trans 3 $threads $TASK_LENGTH 10 1000000 50 50 0 | tee -a $outfile_dir/sls_g0i50r50_thread.csv

            trans-compile/src/trans 3 $threads $TASK_LENGTH 10 1000000 25 25 0 | tee -a $outfile_dir/sls_g50i25r25_thread.csv

            trans-compile/src/trans 3 $threads $TASK_LENGTH 10 1000000 5 5 0 | tee -a $outfile_dir/sls_g90i5r5_thread.csv
        done
    done
}

########################
###       Main       ###
########################
# ht_execute
sl_execute
