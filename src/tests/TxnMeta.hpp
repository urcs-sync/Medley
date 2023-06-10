#ifndef TXN_META_HPP
#define TXN_META_HPP
#include "TDSLSkipList.hpp"
#include "EpochSys.hpp"
#include "OneFile/OneFileLF.hpp"
#include "LFTTSkipList.hpp"

enum TxnType { None, NBTC, TDSL, OneFile, LFTT};

template <TxnType txn_type=TxnType::NBTC>
struct TxnManager {
	::pds::EpochSys* _esys = nullptr;
    padded<::tdsl::SkipListTransaction>* _tdsl_txns = nullptr;
    LFTTSkipList* _lftt_skiplist = nullptr;
	void do_tx(int tid, std::function<void(void)> f, int sz = 0){
        _esys->tx_begin();
        f();
        _esys->tx_end();
    }

    // void tx_end(int tid){
    //     _esys->tx_end();
    // }

    // void tx_abort(int tid){
    //     _esys->tx_abort();
    // }
	TxnManager(::pds::EpochSys* esys, padded<::tdsl::SkipListTransaction>* tdsl_txns) :
		_esys(esys),
		_tdsl_txns(tdsl_txns){};
	TxnManager() : TxnManager(nullptr, nullptr) {};
};

template <>
void TxnManager<TxnType::None>::do_tx(int tid, std::function<void(void)> f, int){
    f();
}

template <>
void TxnManager<TxnType::TDSL>::do_tx(int tid, std::function<void(void)> f, int){
    try{
        _tdsl_txns[tid].ui.TXBegin();
        f();
        _tdsl_txns[tid].ui.TXCommit();
    } catch(AbortTransactionException&){
        throw pds::TransactionAborted();
    }
}

template <>
void TxnManager<TxnType::OneFile>::do_tx(int tid, std::function<void(void)> f, int){
    oflf::updateTx(f);
}

template <>
void TxnManager<TxnType::LFTT>::do_tx(int tid, std::function<void(void)> f, int trans_size){
    _lftt_skiplist->begin_transaction(trans_size, tid);
    f();
    if(!_lftt_skiplist->commit_transaction(tid))
        throw pds::TransactionAborted();
}

// template <>
// void TxnManager<TxnType::None>::tx_begin(int tid){
//     return;
// }

// template <>
// void TxnManager<TxnType::None>::tx_end(int tid){
//     return;
// }

// template <>
// void TxnManager<TxnType::None>::tx_abort(int tid){
//     throw ::pds::AbortBeforeCommit();
//     return;
// }

// template <>
// void TxnManager<TxnType::TDSL>::tx_begin(int tid){
//     _tdsl_txns[tid].ui.TXBegin();
//     return;
// }

// template <>
// void TxnManager<TxnType::TDSL>::tx_end(int tid){
//     try{
//         _tdsl_txns[tid].ui.TXCommit();
//     } catch(AbortTransactionException&){
//         throw pds::TransactionAborted();
//     }
//     return;
// }

// template <>
// void TxnManager<TxnType::TDSL>::tx_abort(int tid){
//     throw ::pds::AbortBeforeCommit();
//     return;
// }

#endif