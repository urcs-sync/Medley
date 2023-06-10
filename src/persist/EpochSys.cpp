#include "EpochSys.hpp"
#include "Recoverable.hpp"

#include <omp.h>
#include <atomic>

namespace pds{
    /**********************************************/
    /* Definitions for sc_desc_t member functions */
    /**********************************************/
    bool sc_desc_t::owner_validate_reads(EpochSys* esys){
        auto r_iter = read_set->begin();
        // Read verification only needs to verify values but
        // unnecessarily anti-ABA count
        for(;!r_iter.reached_end();++r_iter){
            // First check if in write set
            auto res = write_set->get(r_iter->key);
            if(res.has_value()){
                assert(res->old_val == r_iter->val.val && res->old_cnt == r_iter->val.cnt);
            } else {
                // check if in-object value still equals
                if(r_iter->key->full_load(esys) != r_iter->val) return false;
            }
        }
        return true;
    }
    bool sc_desc_t::helper_validate_reads(EpochSys* esys, uint64_t _d){
        // Read verification only needs to verify values but
        // unnecessarily anti-ABA count
        auto r_iter = read_set->begin();
        for(;!r_iter.reached_end();++r_iter){
            // check if in-object value still equals
            if(r_iter->key->full_load(esys) != r_iter->val) return false;
            if(!match(_d,tid_sn.load())) return false;
        }
        return true;
    }

    void sc_desc_t::helper_try_complete(Recoverable* ds, std::atomic<lin_var>& obj, lin_var obj_var){
        return helper_try_complete(ds->_esys, obj, obj_var);
    }
    void sc_desc_t::helper_try_complete(EpochSys* esys, std::atomic<lin_var>& obj, lin_var obj_var){
        // 0. we first load tid_sn and verify obj's var hasn't changed
        // since we found this descriptor, so we know the loaded _d
        // reflects the desired version of the transaction we try to
        // help. 
        uint64_t _d = tid_sn.load();
        if(obj.load() != obj_var) return;

        // 1. If still in preparation, try abort the txn.
        if(in_prep(_d)) {
            abort(_d);
            uint64_t new_d = tid_sn.load();
            if(!match(_d,new_d)) return;
            _d=new_d;
        }
        // 2. Check status again; if still in progress, try commit or abort
        if(in_progress(_d)){
            // 3. Validate reads and check epoch
            if(helper_validate_reads(esys, _d) && esys->check_epoch(epoch)){
                commit(_d);
            } else {
                abort(_d);
            }
        }
        // 4. Uninstall desc
        helper_uninstall_desc(_d);
    }

    void sc_desc_t::owner_try_complete(Recoverable* ds){
        uint64_t _d = tid_sn.load();
        assert(!in_prep(_d));
        auto w_iter = write_set->begin();
        assert(!w_iter.reached_end());
        // Check status; if still in progress, try commit or abort
        if(in_progress(_d)){
            if(owner_validate_reads(ds->_esys) && ds->_esys->check_epoch(epoch)){
                commit(_d);
            } else {
                abort(_d);
            }
        }
        // 4. Uninstall desc
        owner_uninstall_desc();
    }

    void sc_desc_t::try_abort(uint64_t expected_e){
        uint64_t _d = tid_sn.load();
        if(epoch == expected_e && (in_progress(_d) || in_prep(_d))){
            abort(_d);
        }
        // epoch advancer aborts but doesn't clean up descriptors
    }

    void sc_desc_t::helper_uninstall_desc(uint64_t old_d){
        auto w_iter = write_set->begin();
        uint64_t new_d = tid_sn.load();
        assert(committed(new_d) || aborted(new_d) || !match(new_d, old_d));
        // must be called after desc is aborted or committed
        // need to validate old_d, as it may be called by other threads
        if(!match(old_d,new_d)) return;
        assert(!in_progress(new_d));
        assert(!w_iter.reached_end());
        if(committed(new_d)) {
            // bring atomic_lin_var's cnt from x01 to (x+1)00
            for(;!w_iter.reached_end();++w_iter){
                auto addr = w_iter->key;
                auto entry = w_iter->val;
                // Wentao: acquire fence to ensure tid_sn verification
                // won't be reordered before load from write entry.
                //
                // Specifically, load from write entry is ordered with
                // release store on the entry, which happens after
                // another release store that increments tid_sn. 
                //
                // If the following tid_sn load sees a new value, then
                // we should retry to avoid possible data race;
                // otherwise if the load sees the matching value, we
                // are 100% safe to read the entry.
                std::atomic_thread_fence(std::memory_order_acquire);
                lin_var expected(reinterpret_cast<uint64_t>(this), entry.old_cnt+1);
                if (!match(new_d, tid_sn.load())) return;
                addr->var.compare_exchange_strong(expected, lin_var(entry.new_val, entry.old_cnt+4));
            }
        } else {
            assert(aborted(new_d));
            // aborted
            // bring atomic_lin_var's cnt from x01 to (x+1)00
            for(;!w_iter.reached_end();++w_iter){
                auto addr = w_iter->key;
                auto entry = w_iter->val;
                // Wentao: See above
                std::atomic_thread_fence(std::memory_order_acquire);
                lin_var expected(reinterpret_cast<uint64_t>(this), entry.old_cnt+1);
                if (!match(new_d, tid_sn.load())) return;
                addr->var.compare_exchange_strong(expected, lin_var(entry.old_val, entry.old_cnt+4));
            }
        }
    }
    void sc_desc_t::owner_uninstall_desc(){
        uint64_t new_d = tid_sn.load();
        auto w_iter = write_set->begin();
        assert(!in_progress(new_d));
        if(committed(new_d)) {
            // bring atomic_lin_var's cnt from x01 to (x+1)00
            for(;!w_iter.reached_end();++w_iter){
                lin_var expected(reinterpret_cast<uint64_t>(this), w_iter->val.old_cnt+1);
                w_iter->key->var.compare_exchange_strong(expected, lin_var(w_iter->val.new_val, w_iter->val.old_cnt+4));
            }
        } else {
            assert(aborted(new_d));
            // aborted
            // bring atomic_lin_var's cnt from x01 to (x+1)00
            for(;!w_iter.reached_end();++w_iter){
                lin_var expected(reinterpret_cast<uint64_t>(this), w_iter->val.old_cnt+1);
                w_iter->key->var.compare_exchange_strong(expected, lin_var(w_iter->val.old_val, w_iter->val.old_cnt+4));
            }
        }
    }

    void sc_desc_t::reinit(){
        // reinit local descriptor in begin_op
        increment_sn_reset_status();
        // retire and reallocate new read write sets
        read_set->reset();
        write_set->reset();
        std::atomic_thread_fence(std::memory_order_release);
    }
    
    /*********************************************/
    /* Definitions for EpochSys member functions */
    /*********************************************/
    thread_local int EpochSys::tid = -1;
    std::atomic<int> EpochSys::esys_num(0);
    void EpochSys::parse_env(){
        if (epoch_advancer){
            delete epoch_advancer;
        }
        if (trans_tracker){
            delete trans_tracker;
        }
        if (to_be_persisted) {
            delete to_be_persisted;
        }
        if (to_be_freed) {
            delete to_be_freed;
        }

        if (!gtc->checkEnv("EpochLengthUnit")){
            gtc->setEnv("EpochLengthUnit", "Millisecond");
        }

        if (!gtc->checkEnv("EpochLength")){
            gtc->setEnv("EpochLength", "50");
        }

        if (!gtc->checkEnv("BufferSize")){
            gtc->setEnv("BufferSize", "64");
        }

        if (gtc->checkEnv("PersistStrat")){
            if (gtc->getEnv("PersistStrat") == "No"){
                to_be_persisted = new NoToBePersistContainer();
                to_be_freed = new NoToBeFreedContainer(this);
                epoch_advancer = new NoEpochAdvancer();
                trans_tracker = new NoTransactionTracker(this->global_epoch);
                persisted_epochs = new IncreasingMindicator(task_num);
                return;
            }
        }

        if (gtc->checkEnv("PersistStrat")){
            string env_persist = gtc->getEnv("PersistStrat");
            if (env_persist == "DirWB"){
                to_be_persisted = new DirWB(_ral, gtc->task_num);
            } else if (env_persist == "BufferedWB"){
                to_be_persisted = new BufferedWB(gtc, _ral);
            } else {
                errexit("unrecognized 'persist' environment");
            }
        } else {
            gtc->setEnv("PersistStrat", "BufferedWB");
            to_be_persisted = new BufferedWB(gtc, _ral);
        }

        if (gtc->checkEnv("Free")){
            string env_free = gtc->getEnv("Free");
            if (env_free == "PerEpoch"){
                to_be_freed = new PerEpochFreedContainer(this, gtc);
            } else if(env_free == "ThreadLocal"){
                to_be_freed = new ThreadLocalFreedContainer(this, gtc);
            }else if (env_free == "No"){
                to_be_freed = new NoToBeFreedContainer(this);
            } else {
                errexit("unrecognized 'free' environment");
            }
        } else {
            to_be_freed = new ThreadLocalFreedContainer(this, gtc);
        }

        if (gtc->checkEnv("TransTracker")){
            string env_transcounter = gtc->getEnv("TransTracker");
            if (env_transcounter == "AtomicCounter"){
                trans_tracker = new AtomicTransactionTracker(this->global_epoch);
            } else if (env_transcounter == "ActiveThread"){
                trans_tracker = new FenceBeginTransactionTracker(this->global_epoch, task_num);
            } else if (env_transcounter == "CurrEpoch"){
                trans_tracker = new PerEpochTransactionTracker(this->global_epoch, task_num);
            } else {
                errexit("unrecognized 'transaction counter' environment");
            }
        } else {
            trans_tracker = new PerEpochTransactionTracker(this->global_epoch, task_num);
        }

        if (gtc->checkEnv("PersistTracker")){
            string env_persisttracker = gtc->getEnv("PersistTracker");
            if (env_persisttracker == "IncreasingMindicator"){
                persisted_epochs = new IncreasingMindicator(task_num);
            } else if (env_persisttracker == "Mindicator"){
                persisted_epochs = new Mindicator(task_num);
            } else {
                errexit("unrecognized 'persist tracker' environment");
            }
        } else {
            persisted_epochs = new IncreasingMindicator(task_num);
        }

        epoch_advancer = new DedicatedEpochAdvancer(gtc, this);

        // if (gtc->checkEnv("EpochAdvance")){
        //     string env_epochadvance = gtc->getEnv("EpochAdvance");
        //     if (env_epochadvance == "Global"){
        //         epoch_advancer = new GlobalCounterEpochAdvancer();
        //     } else if (env_epochadvance == "SingleThread"){
        //         epoch_advancer = new SingleThreadEpochAdvancer(gtc);
        //     } else if (env_epochadvance == "Dedicated"){
        //         epoch_advancer = new DedicatedEpochAdvancer(gtc, this);
        //     } else {
        //         errexit("unrecognized 'epoch advance' argument");
        //     }
        // } else {
        //     gtc->setEnv("EpochAdvance", "Dedicated");
        //     epoch_advancer = new DedicatedEpochAdvancer(gtc, this);
        // }

        // if (gtc->checkEnv("EpochFreq")){
        //     int env_epoch_advance = stoi(gtc->getEnv("EpochFreq"));
        //     if (gtc->getEnv("EpochAdvance") != "Dedicated" && env_epoch_advance > 63){
        //         errexit("invalid EpochFreq power");
        //     }
        //     epoch_advancer->set_epoch_freq(env_epoch_advance);
        // }
    }

    bool EpochSys::check_epoch(uint64_t c){
        return c == global_epoch->load(std::memory_order_seq_cst);
    }
    bool EpochSys::check_epoch(){
        return check_epoch(epochs[tid].ui);
    }
    uint64_t EpochSys::get_local_epoch(){
        return epochs[tid].ui;
    }

    void EpochSys::begin_op(){
        assert(epochs[tid].ui == NULL_EPOCH);

        flags[tid].start_rolling_CAS = false;

        cleanups[tid].ui.clear();
        local_descs[tid]->reinit();
        do{
            epochs[tid].ui = global_epoch->load(std::memory_order_seq_cst);
        } while(!trans_tracker->consistent_register_active(epochs[tid].ui, epochs[tid].ui));
        auto last_epoch = last_epochs[tid].ui;
        if(last_epoch != epochs[tid].ui){
            last_epochs[tid].ui = epochs[tid].ui;
            if (last_epoch == epochs[tid].ui - 1) {
                // we just entered a new epoch.
                persisted_epochs->first_write_on_new_epoch(epochs[tid].ui, EpochSys::tid);
            }
            // persist past epochs if a target needs us.
            uint64_t persist_until =
                min(epoch_advancer->ongoing_target() - 2, epochs[tid].ui - 1);
            while (true) {
                uint64_t to_persist =
                    persisted_epochs->next_epoch_to_persist(EpochSys::tid);
                if (to_persist == NULL_EPOCH || to_persist > persist_until) {
                    break;
                }
                to_be_persisted->persist_epoch_local(to_persist, EpochSys::tid);
                persisted_epochs->after_persist_epoch(to_persist,
                                                      EpochSys::tid);
            }
        }

        to_be_freed->free_on_new_epoch(epochs[tid].ui);
        local_descs[tid]->set_up_epoch(epochs[tid].ui);

        /* code for pending retire and alloc */
        for(auto & r : pending_retires[tid].ui) {
            // prepare_retire_pblk is a virtual function. 
            // For nonblocking (nbEpochSys), create anti-nodes for
            // retires called before begin_op, place anti-nodes into
            // pending_retires, and set tid_sn 
            // For blocking, just noop
            prepare_retire_pblk(r,epochs[tid].ui);
        }
        for (auto b = pending_allocs[tid].ui.begin(); 
            b != pending_allocs[tid].ui.end(); b++){
            assert((*b)->get_epoch() == NULL_EPOCH);
            register_alloc_pblk(*b, epochs[tid].ui);
        }
        assert(epochs[tid].ui != NULL_EPOCH);
    }

    void EpochSys::end_op(){
        // entering here means the operation has committed
        assert(epochs[tid].ui != NULL_EPOCH);

        if (!pending_retires[tid].ui.empty()){
            for(const auto& r : pending_retires[tid].ui){
                // in-place retire or create anti-node,
                // and register in to_be_persisted
                retire_pblk(r.first, epochs[tid].ui, r.second);
            }
            pending_retires[tid].ui.clear();
        }

        trans_tracker->unregister_active(epochs[tid].ui);
        epoch_advancer->on_end_transaction(this, epochs[tid].ui);
        last_epochs[tid].ui = epochs[tid].ui;
        epochs[tid].ui = NULL_EPOCH;

        pending_allocs[tid].ui.clear();

        // make sure wrapped cleanups are executed even in single operation
        for (auto& f:cleanups[tid].ui)
            f();
    }

    uint64_t EpochSys::begin_reclaim_transaction(){
        uint64_t ret;
        do{
            ret = global_epoch->load(std::memory_order_seq_cst);
        } while(!trans_tracker->consistent_register_active(ret, ret));
        to_be_freed->free_on_new_epoch(ret);
        return ret;
    }

    void EpochSys::end_reclaim_transaction(uint64_t c){
        last_epochs[tid].ui = c;
        trans_tracker->unregister_active(c);
        epoch_advancer->on_end_transaction(this, c);
        epochs[tid].ui = NULL_EPOCH;
    }

    void EpochSys::end_readonly_op(){
        assert(epochs[tid].ui != NULL_EPOCH);
        
        trans_tracker->unregister_active(epochs[tid].ui);
        epochs[tid].ui = NULL_EPOCH;

        assert(pending_allocs[tid].ui.empty());
        assert(pending_retires[tid].ui.empty());
    }

    void EpochSys::abort_op(){
        // entering here means the operation has aborted

        assert(epochs[tid].ui != NULL_EPOCH);
        clear_pending_retires();
        for (auto b = pending_allocs[tid].ui.begin(); 
            b != pending_allocs[tid].ui.end(); b++){
            // reset epochs registered in pending blocks
            reset_alloc_pblk(*b,epochs[tid].ui);
        }
        trans_tracker->unregister_active(epochs[tid].ui);
        epochs[tid].ui = NULL_EPOCH;
    }

    void EpochSys::tx_begin(){
        assert(pending_allocs[tid].ui.empty());
        assert(pending_retires[tid].ui.empty());
        flags[tid].start_rolling_CAS = false;
        flags[tid].inside_txn = true;

        prologue();


        local_descs[tid]->reinit();
        assert(local_descs[tid]->write_set->empty());
        assert(local_descs[tid]->read_set->empty());
        tracker.start_op(tid);
        // all epoch-related things are not executed until tx_end()
    }

    void EpochSys::commit_epilogue() {
        local_descs[tid]->owner_uninstall_desc();// uninstall desc

        for (auto f = unlocks[tid].ui.rbegin();f != unlocks[tid].ui.rend();f++)
            (*f)();
            
        if (!pending_retires[tid].ui.empty()){
            for(const auto& r : pending_retires[tid].ui){
                // in-place retire or create anti-node,
                // and register in to_be_persisted
                retire_pblk(r.first, epochs[tid].ui, r.second);
            }
            pending_retires[tid].ui.clear();
        }

        trans_tracker->unregister_active(epochs[tid].ui);
        epoch_advancer->on_end_transaction(this, epochs[tid].ui);
        last_epochs[tid].ui = epochs[tid].ui;
        epochs[tid].ui = NULL_EPOCH;

        pending_allocs[tid].ui.clear();
        flags[tid].inside_txn = false;

        for (auto& f:cleanups[tid].ui)
            f();

        tracker.end_op(tid);
    }

    void EpochSys::abort_epilogue(){
        local_descs[tid]->owner_uninstall_desc();// uninstall desc
        
        set_during_abort();
        for (auto f = undos[tid].ui.rbegin();f != undos[tid].ui.rend();f++)
            (*f)();
        reset_during_abort();

        for (auto f = unlocks[tid].ui.rbegin();f != unlocks[tid].ui.rend();f++)
            (*f)();
        for(auto& a:allocs[tid].ui){
            a.second(a.first);
        }
        tracker.abort_op(tid); // clear nodes retired to limbo list since tracker.start_op

        // deallocate persistent anti-nodes
        // pending_retires have not yet handled in blocking esys
        // just discard them
        pending_retires[tid].ui.clear();
        // persistent payload has the same lifecycle as their
        // transient nodes, so we assume undo transient allocs
        // already handles them correctly.
        pending_allocs[tid].ui.clear();

        trans_tracker->unregister_active(epochs[tid].ui);
        epoch_advancer->on_end_transaction(this, epochs[tid].ui);
        last_epochs[tid].ui = epochs[tid].ui;
        epochs[tid].ui = NULL_EPOCH;

        flags[tid].inside_txn = false;
        tracker.end_op(tid);

        throw AbortDuringCommit();
    }

    void EpochSys::tx_end(){
        assert(epochs[tid].ui == NULL_EPOCH);

        if (local_descs[tid]->write_set->empty() && undos[tid].ui.empty()){
            // read-only txn without MCAS nor undos; optimized routine
            // that doesn't update desc nor hold epoch
            assert (allocs[tid].ui.empty());
            assert(pending_retires[tid].ui.empty());
            assert(pending_allocs[tid].ui.empty());
            assert(cleanups[tid].ui.empty());
            if(!local_descs[tid]->owner_validate_reads(this)) {
                // failed; abort
                for (auto f = unlocks[tid].ui.rbegin();f != unlocks[tid].ui.rend();f++)
                    (*f)();
                // there shouldn't be txn_tretire
                tracker.check_temp_retire(tid);
                // tracker.abort_op(tid); // clear nodes retired to limbo list since tracker.start_op
                flags[tid].inside_txn = false;
                tracker.end_op(tid);

                throw AbortDuringCommit();
            } else {
                for (auto f = unlocks[tid].ui.rbegin();f != unlocks[tid].ui.rend();f++)
                    (*f)();
                flags[tid].inside_txn = false;
                tracker.end_op(tid);
            }
            return;
        }
        bool retried=false; // TODO: remove this; this is for debugging only 
    retry:
        do{
            epochs[tid].ui = global_epoch->load(std::memory_order_seq_cst);
        } while(!trans_tracker->consistent_register_active(epochs[tid].ui, epochs[tid].ui));
        auto last_epoch = last_epochs[tid].ui;
        if(last_epoch != epochs[tid].ui){
            last_epochs[tid].ui = epochs[tid].ui;
            if (last_epoch == epochs[tid].ui - 1) {
                // we just entered a new epoch.
                persisted_epochs->first_write_on_new_epoch(epochs[tid].ui, EpochSys::tid);
            }
            // persist past epochs if a target needs us.
            uint64_t persist_until =
                min(epoch_advancer->ongoing_target() - 2, epochs[tid].ui - 1);
            while (true) {
                uint64_t to_persist =
                    persisted_epochs->next_epoch_to_persist(EpochSys::tid);
                if (to_persist == NULL_EPOCH || to_persist > persist_until) {
                    break;
                }
                to_be_persisted->persist_epoch_local(to_persist, EpochSys::tid);
                persisted_epochs->after_persist_epoch(to_persist,
                                                      EpochSys::tid);
            }
        }

        to_be_freed->free_on_new_epoch(epochs[tid].ui);
        local_descs[tid]->set_up_epoch(epochs[tid].ui);

        /* code for pending retire and alloc */
        // prepare_retire_pblk is noop in blocking esys
        for (auto b = pending_allocs[tid].ui.begin(); 
            b != pending_allocs[tid].ui.end(); b++){
            assert((*b)->get_epoch() == NULL_EPOCH || retried==true);
            register_alloc_pblk(*b, epochs[tid].ui);
        }
        assert(epochs[tid].ui != NULL_EPOCH);

        /* commit phase begins here */
        if (!local_descs[tid]->set_ready()){
            // failed bringing desc from in prep to in prog
            // this case, the txn has been aborted
            assert(local_descs[tid]->aborted());
            abort_epilogue(); // throw abort exception
        } else {
            /* try_complete with auto retry */

            uint64_t _d = local_descs[tid]->tid_sn.load();
            assert(!local_descs[tid]->in_prep(_d));
            // 1. If read verification fails, abort the txn.
            if(!local_descs[tid]->owner_validate_reads(this)) {
                local_descs[tid]->abort(_d);
            } else {
                // 2. Check status
                if(local_descs[tid]->in_progress(_d)){
                    // 3. Check epoch
                    if(check_epoch()){
                        local_descs[tid]->commit(_d);
                    } else {
                        if (local_descs[tid]->set_unready()){
                            // unregister fetched epoch and retry
                            // commit
                            trans_tracker->unregister_active(epochs[tid].ui);
                            epoch_advancer->on_end_transaction(this, epochs[tid].ui);
                            last_epochs[tid].ui = epochs[tid].ui;
                            epochs[tid].ui = NULL_EPOCH;
                            retried=true;
                            goto retry;
                        }
                    }
                }
            }
            // 4. Uninstall desc
            if (local_descs[tid]->committed()) {
                commit_epilogue();
            } else {
                assert(local_descs[tid]->aborted());
                abort_epilogue(); // throw abort exception
            }
        }

        // code below are moved to commit_epilogue and abort_epilogue.
        // flags[tid].inside_txn = false;
        // tracker.end_op(tid);
    }
    
    void EpochSys::tx_abort(){
        assert(epochs[tid].ui == NULL_EPOCH);

        uint64_t _d = local_descs[tid]->tid_sn.load();
        assert(local_descs[tid]->in_prep(_d) || local_descs[tid]->aborted(_d));

        if (local_descs[tid]->write_set->empty() && undos[tid].ui.empty()){
            // read-only txn without MCAS nor undos; optimized routine
            // that doesn't update desc nor hold epoch
            
            // allocs may still non-empty, because tx_abort can be
            // called in the middle of an op, especially between tnew
            // and tdelete, due to failed addToReadSet.
            for(auto& a:allocs[tid].ui){
                a.second(a.first);
            }
            assert(pending_retires[tid].ui.empty());
            assert(pending_allocs[tid].ui.empty());
            assert(cleanups[tid].ui.empty());
            for (auto f = unlocks[tid].ui.rbegin();f != unlocks[tid].ui.rend();f++)
                (*f)();
            // there shouldn't be txn_tretire
            tracker.check_temp_retire(tid);
            // tracker.abort_op(tid); // clear nodes retired to limbo list since tracker.start_op
            flags[tid].inside_txn = false;
            tracker.end_op(tid);

            throw AbortBeforeCommit();
        }

        local_descs[tid]->abort(_d);
        assert(local_descs[tid]->aborted());

        // uninstall desc
        local_descs[tid]->owner_uninstall_desc();

        set_during_abort();
        for (auto f = undos[tid].ui.rbegin();f != undos[tid].ui.rend();f++)
            (*f)();
        reset_during_abort();

        for (auto f = unlocks[tid].ui.rbegin();f != unlocks[tid].ui.rend();f++)
            (*f)();
        for(auto& a:allocs[tid].ui){
            a.second(a.first);
        }
        tracker.abort_op(tid); // clear nodes retired to limbo list since tracker.start_op

        // deallocate persistent anti nodes
        // no pending_retires at abort for blocking esys
        pending_retires[tid].ui.clear();
        // persistent payload has the same lifecycle as their
        // transient nodes, so we assume undo transient allocs
        // already handles them correctly.
        pending_allocs[tid].ui.clear();

        flags[tid].inside_txn = false;
        tracker.end_op(tid);

        throw AbortBeforeCommit();
    }

    void EpochSys::validate_access(const PBlk* b, uint64_t c){
        if (c == NULL_EPOCH){
            errexit("access with NULL_EPOCH. BEGIN_OP not called?");
        }
        if (b->epoch > c){
            throw OldSeeNewException();
        }
    }

    void EpochSys::register_alloc_pblk(PBlk* b, uint64_t c){
        // static_assert(std::is_convertible<T*, PBlk*>::value,
        //     "T must inherit PBlk as public");
        // static_assert(std::is_copy_constructible<T>::value,
        //             "requires copying");
        PBlk* blk = b;
        assert(c != NULL_EPOCH);
        blk->epoch = c;
        assert(blk->blktype == INIT || blk->blktype == OWNED || (blk->blktype == ALLOC && flags[tid].inside_txn == true)); 
        if (blk->blktype == INIT){
            blk->blktype = ALLOC;
        }
        if (blk->id == 0){
            blk->id = uid_generator.get_id(tid);
        }

        to_be_persisted->register_persist(blk, c);
        PBlk* data = blk->get_data();
        if (data){
            register_alloc_pblk(data, c);
        }
    }

    void EpochSys::register_update_pblk(PBlk* b){
        // to_be_persisted[c%4].push(b);
        if (epochs[tid].ui == NULL_EPOCH){
            // update before BEGIN_OP, return. This register will be done by BEGIN_OP.
            return;
        }
        to_be_persisted->register_persist(b, epochs[tid].ui);
    }

    void EpochSys::prepare_retire_pblk(PBlk* b, const uint64_t& c, std::vector<std::pair<PBlk*,PBlk*>>& pending_retires){
        pending_retires.emplace_back(b, nullptr);
    }

    void EpochSys::prepare_retire_pblk(std::pair<PBlk*,PBlk*>& pending_retire, const uint64_t& c){ 
         // noop
     }

    void EpochSys::withdraw_retire_pblk(PBlk* b, uint64_t c){
         // noop
     }

    void EpochSys::retire_pblk(PBlk* b, uint64_t c, PBlk* anti){
        PBlk* blk = b;
        assert(anti == nullptr);
        if (blk->retire != nullptr){
            errexit("double retire error, or this block was tentatively retired before recent crash.");
        }
        uint64_t e = blk->epoch;
        PBlkType blktype = blk->blktype;
        if (e > c){
            throw OldSeeNewException();
        } else if (e == c){
            // retiring a block updated/allocated in the same epoch.
            // changing it directly to a DELETE node without putting it in to_be_freed list.
            if (blktype == ALLOC || blktype == UPDATE){
                blk->blktype = DELETE;
            } else {
                errexit("wrong type of PBlk to retire.");
            }
        } else {
            // note this actually modifies 'retire' field of a PBlk from the past
            // Which is OK since nobody else will look at this field.
            blk->retire = new_pblk<PBlk>(*b);
            blk->retire->blktype = DELETE;
            blk->retire->epoch = c;
            to_be_persisted->register_persist(blk->retire, c);
        }
        to_be_persisted->register_persist(b, c);
    }

    uint64_t EpochSys::get_epoch(){
        return global_epoch->load(std::memory_order_acquire);
    }

    // // Arg is epoch we think we're ending
    // void EpochSys::advance_epoch(uint64_t c){
    //     // TODO: if we go with one bookkeeping thread, remove unecessary synchronizations.

    //     // Free all retired blocks from 2 epochs ago
    //     if (!trans_tracker->consistent_register_bookkeeping(c-2, c)){
    //         return;
    //     }

    //     to_be_freed->help_free(c-2);

    //     trans_tracker->unregister_bookkeeping(c-2);

    //     // Wait until any other threads freeing such blocks are done
    //     while(!trans_tracker->no_bookkeeping(c-2)){
    //         if (global_epoch->load(std::memory_order_acquire) != c){
    //             return;
    //         }
    //     }

    //     // Wait until all threads active one epoch ago are done
    //     if (!trans_tracker->consistent_register_bookkeeping(c-1, c)){
    //         return;
    //     }
    //     while(!trans_tracker->no_active(c-1)){
    //         if (global_epoch->load(std::memory_order_acquire) != c){
    //             return;
    //         }
    //     }

    //     // Persist all modified blocks from 1 epoch ago
    //     // while(to_be_persisted->persist_epoch(c-1));
    //     to_be_persisted->persist_epoch(c-1);

    //     trans_tracker->unregister_bookkeeping(c-1);

    //     // Wait until any other threads persisting such blocks are done
    //     while(!trans_tracker->no_bookkeeping(c-1)){
    //         if (global_epoch->load(std::memory_order_acquire) != c){
    //             return;
    //         }
    //     }
    //     // persist_func::sfence(); // given the length of current epoch, we may not need this.
    //     // Actually advance the epoch
    //     global_epoch->compare_exchange_strong(c, c+1, std::memory_order_seq_cst);
    //     // Failure is harmless
    // }

    // this epoch advancing logic has been put into epoch advancers.
    // void EpochSys::advance_epoch_dedicated(){
    //     uint64_t c = global_epoch->load(std::memory_order_relaxed);
    //     // Free all retired blocks from 2 epochs ago
    //     to_be_freed->help_free(c-2);
    //     // Wait until all threads active one epoch ago are done
    //     while(!trans_tracker->no_active(c-1)){}
    //     // Persist all modified blocks from 1 epoch ago
    //     to_be_persisted->persist_epoch(c-1);
    //     persist_func::sfence();
    //     // Actually advance the epoch
    //     // global_epoch->compare_exchange_strong(c, c+1, std::memory_order_seq_cst);
    //     global_epoch->store(c+1, std::memory_order_seq_cst);
    // }

    // atomically set the current global epoch number
    void EpochSys::set_epoch(uint64_t c){
        global_epoch->store(c, std::memory_order_seq_cst);
    }

    void EpochSys::on_epoch_begin(uint64_t c){
        // does reclamation for c-2
        to_be_freed->help_free(c-2);
    }

    void EpochSys::on_epoch_end(uint64_t c){
        // Wait until all threads active one epoch ago are done
        // TODO: optimization: persist inactive threads first.
        while(!trans_tracker->no_active(c-1)){}

        // take modular, in case of dedicated epoch advancer calling this function.
        int curr_thread = EpochSys::tid % gtc->task_num;
        curr_thread = persisted_epochs->next_thread_to_persist(c-1, curr_thread);
        // check the top of mindicator to get the last persisted epoch globally
        while(curr_thread >= 0){
            // traverse mindicator to persist each leaf lagging behind, until the top meets requirement
            to_be_persisted->persist_epoch_local(c-1, curr_thread);
            persisted_epochs->after_persist_epoch(c-1, curr_thread);
            curr_thread = persisted_epochs->next_thread_to_persist(c-1, curr_thread);
        }
    }

    std::unordered_map<uint64_t, PBlk*>* EpochSys::recover(const int rec_thd){
        std::unordered_map<uint64_t, PBlk*>* in_use = new std::unordered_map<uint64_t, PBlk*>();
        uint64_t max_epoch = 0;
#ifndef MNEMOSYNE
        bool clean_start;
        auto itr_raw = _ral->recover(rec_thd);
        sys_mode=RECOVER;
        // set system mode to RECOVER -- all PDELETE_DATA and PDELETE becomes no-ops.
        epoch_container = nullptr;
        if(itr_raw[0].is_dirty()) {
            clean_start = false;
            std::cout<<"dirty restart"<<std::endl;
            // dirty restart, epoch system and app need to handle
        } else {
            std::cout<<"clean restart"<<std::endl;
            clean_start = true;
            // clean restart, epoch system and app may still need iter to do something
        }

        std::atomic<int> curr_reporting;
        curr_reporting = 0;
        pthread_barrier_t sync_point;
        pthread_barrier_init(&sync_point, NULL, rec_thd);
        std::vector<std::thread> workers;
        
        std::unordered_set<uint64_t> deleted_ids;
        auto begin = chrono::high_resolution_clock::now();
        auto end = begin;
        for (int rec_tid = 0; rec_tid < rec_thd; rec_tid++) {
            workers.emplace_back(std::thread([&, rec_tid]() {
                hwloc_set_cpubind(gtc->topology, gtc->affinities[rec_tid]->cpuset, HWLOC_CPUBIND_THREAD);
                thread_local uint64_t max_epoch_local = 0;
                thread_local std::unordered_multimap<uint64_t, PBlk*> anti_nodes_local;
                thread_local std::unordered_set<uint64_t> deleted_ids_local;
                // make the first whole pass thorugh all blocks, find the epoch block
                // and help Ralloc fully recover by completing the pass.
                for (; !itr_raw[rec_tid].is_last(); ++itr_raw[rec_tid]){
                    PBlk* curr_blk = (PBlk*) *itr_raw[rec_tid];
                    if (curr_blk->blktype == EPOCH){
                        epoch_container = (Epoch*) curr_blk;
                        global_epoch = &epoch_container->global_epoch;
                        max_epoch_local = std::max(global_epoch->load(), max_epoch_local);
                    } else if (curr_blk->blktype == DELETE){
                        if (clean_start) {
                            errexit("delete node appears after a clean exit.");
                        }
                        anti_nodes_local.insert({curr_blk->get_epoch(), curr_blk});
                        if (curr_blk->get_epoch() != NULL_EPOCH) {
                            deleted_ids_local.insert(curr_blk->get_id());
                        }
                    }
                    max_epoch_local = std::max(max_epoch_local, curr_blk->get_epoch());
                }
                // report after the first pass:
                // calculate the maximum epoch number as the current epoch.
                pthread_barrier_wait(&sync_point);
                if (!epoch_container){
                    errexit("epoch container not found during recovery");
                }
                while(curr_reporting.load() != rec_tid);
                if (rec_tid == 0) {
                    end = chrono::high_resolution_clock::now();
                    auto dur = end - begin;
                    std::cout << "Spent "
                              << std::chrono::duration_cast<
                                     std::chrono::milliseconds>(dur)
                                     .count()
                              << "ms in first pass" << std::endl;
                    begin = chrono::high_resolution_clock::now();
                }
                max_epoch = std::max(max_epoch, max_epoch_local);
                if (rec_tid == rec_thd-1){
                    end = chrono::high_resolution_clock::now();
                    auto dur = end - begin;
                    std::cout << "Spent " << std::chrono::duration_cast<std::chrono::milliseconds>(dur).count()
                              << "ms in first merge"
                              << std::endl;
                    begin = chrono::high_resolution_clock::now();
                }
                curr_reporting.store((rec_tid+1) % rec_thd);
                
                pthread_barrier_wait(&sync_point);
                // remove premature deleted_ids, and merge deleted_ids.
                for (uint64_t e : std::vector<uint64_t>{max_epoch, max_epoch-1}){
                    auto immature = anti_nodes_local.equal_range(e);
                    for (auto itr = immature.first; itr != immature.second; itr++){
                        deleted_ids_local.erase(itr->second->get_id());
                    }
                }
                // merge the results of deleted_ids_local
                pthread_barrier_wait(&sync_point);
                while (curr_reporting.load() != rec_tid);
                deleted_ids.merge(deleted_ids_local);
                curr_reporting.store((rec_tid + 1) % rec_thd);

                // make a second pass through all pblks
                pthread_barrier_wait(&sync_point);
                if (rec_tid == 0){
                    itr_raw = _ral->recover(rec_thd);
                }
                pthread_barrier_wait(&sync_point);
                uint64_t epoch_cap = max_epoch - 2;
                thread_local std::vector<PBlk*> not_in_use_local;
                thread_local std::unordered_map<uint64_t, PBlk*> in_use_local;
                thread_local int second_pass_blks = 0;
                for (; !itr_raw[rec_tid].is_last(); ++itr_raw[rec_tid]) {
                    second_pass_blks++;
                    PBlk* curr_blk = (PBlk*)*itr_raw[rec_tid];
                    // put all premature pblks and those marked by
                    // deleted_ids in not_in_use
                    if (// leave DESC blocks untouched for now.
                        curr_blk->blktype != DESC &&
                        // DELETE blocks are already put into anti_nodes_local.
                        curr_blk->blktype != DELETE && (
                            // block without epoch number, probably just inited
                            curr_blk->epoch == NULL_EPOCH || 
                            // premature pblk
                            curr_blk->epoch > epoch_cap || 
                            // marked deleted by some anti-block
                            deleted_ids.find(curr_blk->get_id()) != deleted_ids.end() 
                        )) {
                        not_in_use_local.push_back(curr_blk);
                    } else {
                        // put all others in in_use while resolve conflict
                        switch (curr_blk->blktype) {
                            case OWNED:
                                errexit(
                                    "OWNED isn't a valid blktype in this "
                                    "version.");
                                break;
                            case ALLOC: {
                                auto insert_res =
                                    in_use_local.insert({curr_blk->id, curr_blk});
                                if (insert_res.second == false) {
                                    if (clean_start) {
                                        errexit(
                                            "more than one record with the "
                                            "same id after a clean exit.");
                                    }
                                    not_in_use_local.push_back(curr_blk);
                                }
                            } break;
                            case UPDATE: {
                                auto search = in_use_local.find(curr_blk->id);
                                if (search != in_use_local.end()) {
                                    if (clean_start) {
                                        errexit(
                                            "more than one record with the "
                                            "same id after a clean exit.");
                                    }
                                    if (curr_blk->epoch >
                                        search->second->epoch) {
                                        not_in_use_local.push_back(search->second);
                                        search->second =
                                            curr_blk;  // TODO: double-check if
                                                       // this is right.
                                    } else {
                                        not_in_use_local.push_back(curr_blk);
                                    }
                                } else {
                                    in_use_local.insert({curr_blk->id, curr_blk});
                                }
                            } break;
                            case DELETE:
                            case EPOCH:
                            case DESC: // TODO: allocate DESC in DRAM instead of NVM
                                break;
                            default:
                                errexit("wrong type of pblk discovered");
                                break;
                        }
                    }
                }
                // merge the results of in_use, resolve conflict
                pthread_barrier_wait(&sync_point);
                while (curr_reporting.load() != rec_tid);
                if (rec_tid == 0) {
                    end = chrono::high_resolution_clock::now();
                    auto dur = end - begin;
                    std::cout << "Spent "
                              << std::chrono::duration_cast<
                                     std::chrono::milliseconds>(dur)
                                     .count()
                              << "ms in second pass" << std::endl;
                    begin = chrono::high_resolution_clock::now();
                }
                std::cout<<"second pass blk count:"<<second_pass_blks<<std::endl;
                for (auto itr : in_use_local) {
                    auto found = in_use->find(itr.first);
                    if (found == in_use->end()) {
                        in_use->insert({itr.first, itr.second});
                    } else if (found->second->get_epoch() <
                               itr.second->get_epoch()) {
                        not_in_use_local.push_back(found->second);
                        found->second = itr.second;
                    } else {
                        not_in_use_local.push_back(itr.second);
                    }
                }
                if (rec_tid == rec_thd - 1) {
                    end = chrono::high_resolution_clock::now();
                    auto dur = end - begin;
                    std::cout << "Spent "
                              << std::chrono::duration_cast<
                                     std::chrono::milliseconds>(dur)
                                     .count()
                              << "ms in second merge" << std::endl;
                    begin = chrono::high_resolution_clock::now();
                }
                curr_reporting.store((rec_tid + 1) % rec_thd);
                // clean up not_in_use and anti-nodes
                for (auto itr : not_in_use_local) {
                    itr->set_epoch(NULL_EPOCH);
                    _ral->deallocate(itr, rec_tid);
                }
                for (auto itr : anti_nodes_local) {
                    itr.second->set_epoch(NULL_EPOCH);
                    _ral->deallocate(itr.second, rec_tid);
                }
                pthread_barrier_wait(&sync_point);
                if (rec_tid == rec_thd - 1) {
                    end = chrono::high_resolution_clock::now();
                    auto dur = end - begin;
                    std::cout << "Spent "
                              << std::chrono::duration_cast<
                                     std::chrono::milliseconds>(dur)
                                     .count()
                              << "ms in deallocation" << std::endl;
                    begin = chrono::high_resolution_clock::now();
                }
            })); // workers.emplace_back()
        } // for (rec_thd)
        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }

        // set system mode back to online
        sys_mode = ONLINE;
        reset();

        std::cout<<"returning from EpochSys Recovery."<<std::endl;
#endif /* !MNEMOSYNE */
        return in_use;
    }
    
    /***********************************************/
    /* Definitions for nbEpochSys member functions */
    /***********************************************/
    void nbEpochSys::reset(){
        EpochSys::reset();
        // TODO: only nbEpochSys needs persistent descs. consider move all
        // inits into nbEpochSys.
        for (int i = 0; i < gtc->task_num; i++) {
            to_be_persisted->init_desc_local(local_descs[i], i);
        }
    }

    void nbEpochSys::parse_env(){
        if (epoch_advancer){
            delete epoch_advancer;
        }
        if (trans_tracker){
            delete trans_tracker;
        }
        if (to_be_persisted) {
            delete to_be_persisted;
        }
        if (to_be_freed) {
            delete to_be_freed;
        }

        if (!gtc->checkEnv("EpochLengthUnit")){
            gtc->setEnv("EpochLengthUnit", "Millisecond");
        }

        if (!gtc->checkEnv("EpochLength")){
            gtc->setEnv("EpochLength", "50");
        }

        if (!gtc->checkEnv("BufferSize")){
            gtc->setEnv("BufferSize", "64");
        }

        if (gtc->checkEnv("PersistStrat")){
            if (gtc->getEnv("PersistStrat") == "No"){
                to_be_persisted = new NoToBePersistContainer();
                to_be_freed = new NoToBeFreedContainer(this);
                epoch_advancer = new NoEpochAdvancer();
                trans_tracker = new NoTransactionTracker(this->global_epoch);
                persisted_epochs = new IncreasingMindicator(task_num);
                return;
            }
        }

        if (gtc->checkEnv("PersistStrat")){
            string env_persist = gtc->getEnv("PersistStrat");
            if (env_persist == "DirWB"){
                to_be_persisted = new DirWB(_ral, gtc->task_num);
            } else if (env_persist == "PerEpoch"){
                errexit("nbEpochSys isn't compatible with PerEpoch!");
            } else if (env_persist == "BufferedWB"){
                to_be_persisted = new BufferedWB(gtc, _ral);
            } else {
                errexit("unrecognized 'persist' environment");
            }
        } else {
            gtc->setEnv("PersistStrat", "BufferedWB");
            to_be_persisted = new BufferedWB(gtc, _ral);
        }

        if (gtc->checkEnv("Free")){
            string env_free = gtc->getEnv("Free");
            if (env_free == "PerEpoch"){
                to_be_freed = new PerEpochFreedContainer(this, gtc);
            } else if (env_free == "No"){
                to_be_freed = new NoToBeFreedContainer(this);
            } else {
                errexit("unrecognized 'free' environment");
            }
        } else {
            to_be_freed = new PerEpochFreedContainer(this, gtc);
        }

        if (gtc->checkEnv("TransTracker")){
            string env_transcounter = gtc->getEnv("TransTracker");
            if (env_transcounter == "AtomicCounter"){
                trans_tracker = new AtomicTransactionTracker(this->global_epoch);
            } else if (env_transcounter == "ActiveThread"){
                trans_tracker = new FenceBeginTransactionTracker(this->global_epoch, task_num);
            } else if (env_transcounter == "CurrEpoch"){
                trans_tracker = new PerEpochTransactionTracker(this->global_epoch, task_num);
            } else {
                errexit("unrecognized 'transaction counter' environment");
            }
        } else {
            trans_tracker = new PerEpochTransactionTracker(this->global_epoch, task_num);
        }

        if (gtc->checkEnv("PersistTracker")){
            string env_persisttracker = gtc->getEnv("PersistTracker");
            if (env_persisttracker == "IncreasingMindicator"){
                persisted_epochs = new IncreasingMindicator(task_num);
            } else if (env_persisttracker == "Mindicator"){
                persisted_epochs = new Mindicator(task_num);
            } else {
                errexit("unrecognized 'persist tracker' environment");
            }
        } else {
            persisted_epochs = new IncreasingMindicator(task_num);
        }

        epoch_advancer = new DedicatedEpochAdvancerNbSync(gtc, this);

        // if (gtc->checkEnv("EpochAdvance")){
        //     string env_epochadvance = gtc->getEnv("EpochAdvance");
        //     if (env_epochadvance == "Global"){
        //         epoch_advancer = new GlobalCounterEpochAdvancer();
        //     } else if (env_epochadvance == "SingleThread"){
        //         epoch_advancer = new SingleThreadEpochAdvancer(gtc);
        //     } else if (env_epochadvance == "Dedicated"){
        //         epoch_advancer = new DedicatedEpochAdvancer(gtc, this);
        //     } else {
        //         errexit("unrecognized 'epoch advance' argument");
        //     }
        // } else {
        //     gtc->setEnv("EpochAdvance", "Dedicated");
        //     epoch_advancer = new DedicatedEpochAdvancer(gtc, this);
        // }

        // if (gtc->checkEnv("EpochFreq")){
        //     int env_epoch_advance = stoi(gtc->getEnv("EpochFreq"));
        //     if (gtc->getEnv("EpochAdvance") != "Dedicated" && env_epoch_advance > 63){
        //         errexit("invalid EpochFreq power");
        //     }
        //     epoch_advancer->set_epoch_freq(env_epoch_advance);
        // }
    }

    void nbEpochSys::register_alloc_pblk(PBlk* b, uint64_t c){
        // static_assert(std::is_convertible<T*, PBlk*>::value,
        //     "T must inherit PBlk as public");
        // static_assert(std::is_copy_constructible<T>::value,
        //             "requires copying");
        b->set_tid_sn(tid,get_dcss_desc()->get_sn());
        PBlk* blk = b;
        assert(c != NULL_EPOCH);
        blk->epoch = c;
        assert(blk->blktype == INIT || blk->blktype == OWNED || (blk->blktype == ALLOC && flags[tid].inside_txn == true)); 
        if (blk->blktype == INIT){
            blk->blktype = ALLOC;
        }
        if (blk->id == 0){
            blk->id = uid_generator.get_id(tid);
        }

        to_be_persisted->register_persist(blk, c);
        PBlk* data = blk->get_data();
        if (data){
            register_alloc_pblk(data, c);
        }
    }

    void nbEpochSys::prepare_retire_pblk(PBlk* b, const uint64_t& c, std::vector<std::pair<PBlk*,PBlk*>>& pending_retires){
        PBlk* blk = b;
        uint64_t e = blk->epoch;
        PBlkType blktype = blk->blktype;
        if (e > c){
            throw OldSeeNewException();
        } else {
            PBlk* anti = new_pblk<PBlk>(*b);
            anti->blktype = DELETE;
            anti->epoch = c;
            anti->set_tid_sn(tid, get_dcss_desc()->get_sn());
            pending_retires.emplace_back(blk, anti);
            // it may be registered in a newer bucket, but it's safe.
            to_be_persisted->register_persist(anti, c);
        }
    }

     void nbEpochSys::prepare_retire_pblk(std::pair<PBlk*,PBlk*>& pending_retire, const uint64_t& c){
        PBlk* blk = pending_retire.first;
        uint64_t e = blk->epoch;
        PBlkType blktype = blk->blktype;
        if (e > c){
            throw OldSeeNewException();
        } else {
            PBlk* anti = nullptr;
            if (pending_retire.second==nullptr){
                // regular call when anti hasn't been created
                anti = new_pblk<PBlk>(*blk);
                pending_retire.second = anti;
            } else {
                // is a retry in which anti has been created
                // this case, we must be in a txn
                assert(flags[tid].inside_txn == true);
                anti = pending_retire.second;
            }
            anti->blktype = DELETE;
            anti->epoch = c;
            anti->set_tid_sn(tid, get_dcss_desc()->get_sn());
            // it may be registered in a newer bucket, but it's safe.
            to_be_persisted->register_persist(anti, c);
        }
    }

    void nbEpochSys::withdraw_retire_pblk(PBlk* b, uint64_t c){
        // Wentao: WARNING!! Here we directly deallocate
        // anti-payloads; those deallocated may still be in
        // to_be_persisted, but it's safe to flush after
        // deallocation if the address is still valid, which is
        // true in Ralloc. If in the future Ralloc changes or
        // different allocator is used, be careful about this!
        if(b!=nullptr){
            delete_pblk(b, c);
        }
     }

    void nbEpochSys::retire_pblk(PBlk* b, uint64_t c, PBlk* anti){
        PBlk* blk = b;
        if (blk->retire != nullptr){
            errexit("double retire error, or this block was tentatively retired before recent crash.");
        }
        uint64_t e = blk->epoch;
        PBlkType blktype = blk->blktype;
        assert (e <= c);
        // note this actually modifies 'retire' field of a PBlk from the past
        // Which is OK since nobody else will look at this field.
        assert(anti!=nullptr);
        // 'retire' field isn't persistent, so no flush is needed
        blk->retire = anti;
    }

    void nbEpochSys::local_free(uint64_t c){
        auto last_epoch = last_epochs[tid].ui;
        if(last_epoch==c) return;
        // There are at most three buckets not cleaned up,
        // last_epoch-1, last_epoch, and last_epoch+1. Also, when
        // begin in epoch c, only buckets <= c-2 should be cleaned
        for(uint64_t i = last_epoch-1; 
            i <= min(last_epoch+1,c-2);
            i++){
            to_be_freed->help_free_local(i);
            persist_func::sfence();
        }
    }

    void nbEpochSys::local_persist(uint64_t c){
        auto last_epoch = last_epochs[tid].ui;
        if(last_epoch == c) return;
        persisted_epochs->first_write_on_new_epoch(c,EpochSys::tid);
        /* Strategy 1: always flush TBP[last_epoch] */
        // There are at most one bucket not cleaned up, last_epoch.
        // Also, when begin in epoch c, only buckets <= c-2 should be
        // cleaned
        to_be_persisted->persist_epoch_local(last_epoch, EpochSys::tid);
        uint64_t to_persist =
            persisted_epochs->next_epoch_to_persist(EpochSys::tid);
        if(to_persist<=last_epoch){
            persisted_epochs->after_persist_epoch(last_epoch, EpochSys::tid);
        }
        persist_func::sfence();
#if 0
        // Wentao: This strategy is incorrect!!! 
        // Consider the case where T1 aborts in c-1 and places reset
        // items to TBP[c-1], but after a concurrent epoch advance
        // from c to c+1 goes through everyone's TBP[c-1]. And before
        // the advance updates the epoch to c+1, T1 initiates another
        // transaction in c and updates its descriptor. If T1 doesn't
        // write back TBP[c-1] before updating its descriptor, the
        // recovery from a crash in c+1 may think those payloads are
        // valid.

        /* Strategy 2: flush TBP[last_epoch] if it <= c-2 and then
            * check if there needs a help */
        if(last_epoch<=c-2){
            // clean up potential leftover from last txn due to abortion
            to_be_persisted->persist_epoch_local(last_epoch, EpochSys::tid);
            persist_func::sfence();
        } else {
            // persist past epochs if a target needs us.
            uint64_t persist_until =
                min(epoch_advancer->ongoing_target() - 2, c-1);
            while (true) {
                uint64_t to_persist =
                    persisted_epochs->next_epoch_to_persist(EpochSys::tid);
                if (to_persist == NULL_EPOCH || to_persist > persist_until) {
                    break;
                }
                to_be_persisted->persist_epoch_local(to_persist, EpochSys::tid);
                persisted_epochs->after_persist_epoch(to_persist, EpochSys::tid);
            }
        }
#endif /* 0 */
    }

    void nbEpochSys::begin_op(){
        assert(epochs[tid].ui == NULL_EPOCH);

        flags[tid].start_rolling_CAS = false;

        cleanups[tid].ui.clear();
        epochs[tid].ui = global_epoch->load(std::memory_order_seq_cst);
        local_persist(epochs[tid].ui);
        local_descs[tid]->reinit();
        local_descs[tid]->set_up_epoch(epochs[tid].ui);
        to_be_persisted->register_persist_desc_local(epochs[tid].ui, EpochSys::tid);
        local_free(epochs[tid].ui);

        /* code for pending retire and alloc */
        for(auto & r : pending_retires[tid].ui) {
            // prepare_retire_pblk is a virtual function. 
            // For nonblocking (nbEpochSys), create anti-nodes for
            // retires called before begin_op, place anti-nodes into
            // pending_retires, and set tid_sn 
            // For blocking, just noop
            prepare_retire_pblk(r,epochs[tid].ui);
        }
        for (auto b = pending_allocs[tid].ui.begin(); 
            b != pending_allocs[tid].ui.end(); b++){
            assert((*b)->get_epoch() == NULL_EPOCH);
            register_alloc_pblk(*b, epochs[tid].ui);
        }
        assert(epochs[tid].ui != NULL_EPOCH);
    }

    void nbEpochSys::end_op(){
        assert(epochs[tid].ui != NULL_EPOCH);

        if (!pending_retires[tid].ui.empty()){
            for(const auto& r : pending_retires[tid].ui){
                // link anti-node to payload
                retire_pblk(r.first, epochs[tid].ui, r.second);
            }
            pending_retires[tid].ui.clear();
        }

        epoch_advancer->on_end_transaction(this, epochs[tid].ui);
        last_epochs[tid].ui = epochs[tid].ui;
        epochs[tid].ui = NULL_EPOCH;

        pending_allocs[tid].ui.clear();

        // make sure wrapped cleanups are executed even in single
        // operation, although it's recommended that single operation
        // executes cleanup in place.
        for (auto& f:cleanups[tid].ui)
            f();
    }

    void nbEpochSys::end_readonly_op(){
        assert(epochs[tid].ui != NULL_EPOCH);
        
        last_epochs[tid].ui = epochs[tid].ui;
        epochs[tid].ui = NULL_EPOCH;

        assert(pending_allocs[tid].ui.empty());
        assert(pending_retires[tid].ui.empty());
    };

    void nbEpochSys::abort_op(){
        assert(epochs[tid].ui != NULL_EPOCH);

        clear_pending_retires();

        for (auto b = pending_allocs[tid].ui.begin(); 
            b != pending_allocs[tid].ui.end(); b++){
            // reset epochs registered in pending blocks
            reset_alloc_pblk(*b,epochs[tid].ui);
        }
        last_epochs[tid].ui = epochs[tid].ui;
        epochs[tid].ui = NULL_EPOCH;
    };

    void nbEpochSys::commit_epilogue(){
        local_descs[tid]->owner_uninstall_desc();// uninstall desc

        assert(unlocks[tid].ui.empty());
        assert(undos[tid].ui.empty());

        if (!pending_retires[tid].ui.empty()){
            for(const auto& r : pending_retires[tid].ui){
                // link anti-node to payload
                retire_pblk(r.first, epochs[tid].ui, r.second);
            }
            pending_retires[tid].ui.clear();
        }

        epoch_advancer->on_end_transaction(this, epochs[tid].ui);
        last_epochs[tid].ui = epochs[tid].ui;
        epochs[tid].ui = NULL_EPOCH;

        pending_allocs[tid].ui.clear();
        flags[tid].inside_txn = false;

        for (auto& f:cleanups[tid].ui)
            f();

        tracker.end_op(tid);
    }

    void nbEpochSys::abort_epilogue(){
        local_descs[tid]->owner_uninstall_desc();// uninstall desc

        assert(unlocks[tid].ui.empty());
        assert(undos[tid].ui.empty());
        // undo transient allocs
        for(auto& a:allocs[tid].ui){
            a.second(a.first);
        }
        tracker.abort_op(tid); // clear nodes retired to limbo list since tracker.start_op

        // deallocate persistent anti-nodes
        clear_pending_retires();
        // persistent payload has the same lifecycle as their
        // transient nodes, so we assume undo transient allocs
        // already handles them correctly.
        pending_allocs[tid].ui.clear();

        epoch_advancer->on_end_transaction(this, epochs[tid].ui);
        last_epochs[tid].ui = epochs[tid].ui;
        epochs[tid].ui = NULL_EPOCH;

        flags[tid].inside_txn = false;
        tracker.end_op(tid);

        throw AbortDuringCommit();
    }

    void nbEpochSys::tx_end(){
        assert(epochs[tid].ui == NULL_EPOCH);

        if (local_descs[tid]->write_set->empty()){
            // read-only txn without MCAS; optimized routine
            // that doesn't update desc nor hold epoch
            assert (allocs[tid].ui.empty());
            assert(pending_retires[tid].ui.empty());
            assert(pending_allocs[tid].ui.empty());
            assert(cleanups[tid].ui.empty());
            if(!local_descs[tid]->owner_validate_reads(this)) {
                // failed; abort
                assert(unlocks[tid].ui.empty());
                assert(undos[tid].ui.empty());

                // there shouldn't be txn_tretire
                tracker.check_temp_retire(tid);
                // tracker.abort_op(tid); // clear nodes retired to limbo list since tracker.start_op
                flags[tid].inside_txn = false;
                tracker.end_op(tid);

                throw AbortDuringCommit();
            } else {
                assert(unlocks[tid].ui.empty());
                assert(undos[tid].ui.empty());

                flags[tid].inside_txn = false;
                tracker.end_op(tid);
            }
            return;
        }
        bool retried=false; // TODO: remove this; this is for debugging only 
    retry:
        epochs[tid].ui = global_epoch->load(std::memory_order_seq_cst);
        local_persist(epochs[tid].ui);
        local_descs[tid]->set_up_epoch(epochs[tid].ui);
        to_be_persisted->register_persist_desc_local(epochs[tid].ui, EpochSys::tid);
        local_free(epochs[tid].ui);

        /* code for pending retire and alloc */
        for(auto & r : pending_retires[tid].ui) {
            // prepare_retire_pblk is a virtual function. 
            // For nonblocking (nbEpochSys), create anti-nodes for
            // retires called before begin_op, place anti-nodes into
            // pending_retires, and set tid_sn 
            // For blocking, just noop
            prepare_retire_pblk(r,epochs[tid].ui);
        }
        for (auto b = pending_allocs[tid].ui.begin(); 
            b != pending_allocs[tid].ui.end(); b++){
            assert((*b)->get_epoch() == NULL_EPOCH || (retried && (*b)->get_epoch() == last_epochs[tid].ui));
            register_alloc_pblk(*b, epochs[tid].ui);
        }
        assert(epochs[tid].ui != NULL_EPOCH);

        /* commit phase begins here */
        if (!local_descs[tid]->set_ready()){
            // failed bringing desc from in prep to in prog
            // this case, the txn has been aborted
            assert(local_descs[tid]->aborted());
            abort_epilogue(); // throw abort exception
        } else {
            /* try_complete with auto retry */

            uint64_t _d = local_descs[tid]->tid_sn.load();
            assert(!local_descs[tid]->in_prep(_d));
            // 1. If still in preparation or read verification fails,
            //    abort the txn.
            if(!local_descs[tid]->owner_validate_reads(this)) {
                local_descs[tid]->abort(_d);
            } else {
                // 2. Check status
                if(local_descs[tid]->in_progress(_d)){
                    // 3. Check epoch
                    if(check_epoch()){
                        local_descs[tid]->commit(_d);
                    } else {
                        if (local_descs[tid]->set_unready()){
                            // unregister fetched epoch and retry commit
                            epoch_advancer->on_end_transaction(this, epochs[tid].ui);
                            last_epochs[tid].ui = epochs[tid].ui;
                            epochs[tid].ui = NULL_EPOCH;
                            retried=true;
                            goto retry;
                        }
                    }
                }
            }
            // 4. Uninstall desc
            if (local_descs[tid]->committed()) {
                commit_epilogue();
            } else {
                assert(local_descs[tid]->aborted());
                abort_epilogue(); // throw abort exception
            }
        }

        // code below are moved to commit_epilogue and abort_epilogue.
        // flags[tid].inside_txn = false;
        // tracker.end_op(tid);
    }

    void nbEpochSys::tx_abort(){
        assert(epochs[tid].ui == NULL_EPOCH);

        uint64_t _d = local_descs[tid]->tid_sn.load();
        assert(local_descs[tid]->in_prep(_d) || local_descs[tid]->aborted(_d));

        if (local_descs[tid]->write_set->empty()){
            // read-only txn without MCAS; optimized routine
            // that doesn't update desc nor hold epoch
            
            // allocs may still non-empty, because tx_abort can be
            // called in the middle of an op, especially between tnew
            // and tdelete, due to failed addToReadSet.
            assert(local_descs[tid]->in_prep(_d));
            for(auto& a:allocs[tid].ui){
                a.second(a.first);
            }
            assert(pending_retires[tid].ui.empty());
            assert(pending_allocs[tid].ui.empty());
            assert(cleanups[tid].ui.empty());
            assert(unlocks[tid].ui.empty());
            assert(undos[tid].ui.empty());

            // there shouldn't be txn_tretire
            tracker.check_temp_retire(tid);
            // tracker.abort_op(tid); // clear nodes retired to limbo list since tracker.start_op
            flags[tid].inside_txn = false;
            tracker.end_op(tid);

            throw AbortBeforeCommit();
        }

        local_descs[tid]->abort(_d);
        assert(local_descs[tid]->aborted());

        // uninstall desc
        local_descs[tid]->owner_uninstall_desc();
        
        assert(unlocks[tid].ui.empty());
        assert(undos[tid].ui.empty());
        for(auto& a:allocs[tid].ui){
            a.second(a.first);
        }
        tracker.abort_op(tid); // clear nodes retired to limbo list since tracker.start_op

        // deallocate persistent payloads and anti-nodes
        clear_pending_retires();
        // persistent payload has the same lifecycle as their
        // transient nodes, so we assume undo transient allocs
        // already handles them correctly.
        pending_allocs[tid].ui.clear();

        flags[tid].inside_txn = false;
        tracker.end_op(tid);

        throw AbortBeforeCommit();
    }

    uint64_t nbEpochSys::begin_reclaim_transaction(){
        uint64_t ret;
        ret = global_epoch->load(std::memory_order_seq_cst);
        local_persist(ret);
        local_free(ret);
        return ret;
    }

    void nbEpochSys::end_reclaim_transaction(uint64_t c){
        last_epochs[tid].ui = c;
        epoch_advancer->on_end_transaction(this, c);
        epochs[tid].ui = NULL_EPOCH;
    }
    void nbEpochSys::on_epoch_begin(uint64_t c){
        // do nothing -- memory reclamation is done thread-locally in nbEpochSys
    }

    void nbEpochSys::on_epoch_end(uint64_t c){
        // take modular, in case of dedicated epoch advancer calling this function.
        int curr_thread = EpochSys::tid % gtc->task_num;
        curr_thread = persisted_epochs->next_thread_to_persist(c-1, curr_thread);
        // check the top of mindicator to get the last persisted epoch globally
        while(curr_thread >= 0){
            // traverse mindicator to persist each leaf lagging behind, until the top meets requirement
            local_descs[curr_thread]->try_abort(c-1); // lazily abort ongoing transactions
            to_be_persisted->persist_epoch_local(c-1, curr_thread);
            persisted_epochs->after_persist_epoch(c-1, curr_thread);
            curr_thread = persisted_epochs->next_thread_to_persist(c-1, curr_thread);
        }
        // a lock-prefixed instruction (CAS) must have taken place inside Mindicator,
        // so no need to explicitly issue fence here.
        // persist_func::sfence();

        // old implementation:
        // try to abort all ongoing transactions
        // for(int i=0; i<task_num; i++){
        //     local_descs[i]->try_abort(c-1);
        // }
        // // Persist all modified blocks from 1 epoch ago
        // to_be_persisted->persist_epoch(c-1);
        // persist_func::sfence();
    }

    std::unordered_map<uint64_t, PBlk*>* nbEpochSys::recover(const int rec_thd) {
        std::unordered_map<uint64_t, PBlk*>* in_use = new std::unordered_map<uint64_t, PBlk*>();
        std::unordered_map<uint64_t, sc_desc_t*> descs;  //tid->desc
        uint64_t max_tid = 0;
        uint64_t max_epoch = 0;
#ifndef MNEMOSYNE
        bool clean_start;
        auto itr_raw = _ral->recover(rec_thd);
        sys_mode = RECOVER;
        // set system mode to RECOVER -- all PDELETE_DATA and PDELETE becomes no-ops.
        epoch_container = nullptr;
        if (itr_raw[0].is_dirty()) {
            clean_start = false;
            std::cout << "dirty restart" << std::endl;
            // dirty restart, epoch system and app need to handle
        } else {
            std::cout << "clean restart" << std::endl;
            clean_start = true;
            // clean restart, epoch system and app may still need iter to do something
        }

        std::atomic<int> curr_reporting;
        curr_reporting = 0;
        pthread_barrier_t sync_point;
        pthread_barrier_init(&sync_point, NULL, rec_thd);
        std::vector<std::thread> workers;

        std::unordered_set<uint64_t> deleted_ids;

        for (int rec_tid = 0; rec_tid < rec_thd; rec_tid++) {
            workers.emplace_back(std::thread([&, rec_tid]() {
                hwloc_set_cpubind(gtc->topology,
                                  gtc->affinities[rec_tid]->cpuset,
                                  HWLOC_CPUBIND_THREAD);
                thread_local uint64_t max_epoch_local = 0;
                thread_local uint64_t max_tid_local = -1;
                thread_local std::vector<PBlk*> anti_nodes_local;
                thread_local std::unordered_set<uint64_t> deleted_ids_local;
                thread_local std::unordered_map<uint64_t, sc_desc_t*> descs_local;
                // make the first whole pass thorugh all blocks, find the epoch block
                // and help Ralloc fully recover by completing the pass.
                for (; !itr_raw[rec_tid].is_last(); ++itr_raw[rec_tid]) {
                    PBlk* curr_blk = (PBlk*)*itr_raw[rec_tid];
                    if (curr_blk->blktype == EPOCH) {
                        epoch_container = (Epoch*)curr_blk;
                        global_epoch = &epoch_container->global_epoch;
                        max_epoch_local = std::max(global_epoch->load(), max_epoch_local);
                    } else if (curr_blk->blktype == DESC) {
                        // since sc_desc_t isn't a derived class of PBlk
                        // anymore, we have to reinterpret cast instead of
                        // dynamic cast
                        auto* tmp = reinterpret_cast<sc_desc_t*>(curr_blk);
                        assert(tmp != nullptr);
                        uint64_t curr_tid = tmp->get_tid();
                        max_tid_local = std::max(max_tid_local, curr_tid);
                        descs_local[curr_tid] = tmp;
                    } else if (curr_blk->blktype == DELETE) {
                        anti_nodes_local.push_back(curr_blk);
                        if (curr_blk->get_epoch() != NULL_EPOCH){
                            deleted_ids_local.insert(curr_blk->get_id());
                        }
                    }
                    max_epoch_local = std::max(max_epoch_local, curr_blk->get_epoch());
                }
                // report after the first pass:
                // calculate the maximum epoch number as the current epoch.
                // calculate the maximum tid
                // merge descs
                pthread_barrier_wait(&sync_point);
                if (!epoch_container) {
                    errexit("epoch container not found during recovery");
                }
                while (curr_reporting.load() != rec_tid)
                    ;
                max_epoch = std::max(max_epoch, max_epoch_local);
                max_tid = std::max(max_tid, max_tid_local);
                descs.merge(descs_local);
                if (rec_tid == rec_thd - 1) {
                    // some sanity check
                    // in data structures with background threads,
                    // these don't always hold
                    // assert(descs.size() == max_tid + 1);
                    // for (uint64_t i = 0; i < max_tid; i++) {
                    //     assert(descs.find(i) != descs.end());
                    // }
                }
                curr_reporting.store((rec_tid + 1) % rec_thd);

                uint64_t epoch_cap = max_epoch - 2;
                pthread_barrier_wait(&sync_point);
                // remove premature deleted_ids
                for (auto n : anti_nodes_local){
                    auto curr_tid = n->get_tid();
                    auto curr_sn = n->get_sn();
                    if ( // anti node belongs to an epoch too new
                        n->get_epoch() > epoch_cap ||
                        // transaction is not registered
                        curr_sn > descs[curr_tid]->get_sn() ||
                        // transaction registered but not committed
                        (curr_sn == descs[curr_tid]->get_sn() && !descs[curr_tid]->committed())){
                        deleted_ids_local.erase(n->get_id());
                    }
                }
                // merge the results of deleted_ids_local
                pthread_barrier_wait(&sync_point);
                while (curr_reporting.load() != rec_tid)
                    ;
                deleted_ids.merge(deleted_ids_local);
                curr_reporting.store((rec_tid + 1) % rec_thd);

                // make a second pass through all pblks
                pthread_barrier_wait(&sync_point);
                if (rec_tid == 0) {
                    itr_raw = _ral->recover(rec_thd);
                }
                pthread_barrier_wait(&sync_point);
                
                thread_local std::vector<PBlk*> not_in_use_local;
                thread_local std::unordered_map<uint64_t, PBlk*> in_use_local;
                for (; !itr_raw[rec_tid].is_last(); ++itr_raw[rec_tid]) {
                    PBlk* curr_blk = (PBlk*)*itr_raw[rec_tid];
                    auto curr_tid = curr_blk->get_tid();
                    auto curr_sn = curr_blk->get_sn();
                    // put all premature pblks and those marked by
                    // deleted_ids in not_in_use
                    if (  // leave DESC blocks untouched for now.
                        curr_blk->blktype != DESC &&
                        // DELETE blocks are already put into anti_nodes_local.
                        curr_blk->blktype != DELETE && (
                            // block without epoch number, probably just inited
                            curr_blk->epoch == NULL_EPOCH ||
                            // premature pblk
                            curr_blk->epoch > epoch_cap ||
                            // marked deleted by some anti-block
                            deleted_ids.find(curr_blk->get_id()) != deleted_ids.end() ||
                            // premature transaction: not registered in descs
                            curr_sn > descs[curr_tid]->get_sn() ||
                            // premature transaction: registered but not committed
                            (curr_sn == descs[curr_tid]->get_sn() && !descs[curr_tid]->committed()))) {
                        not_in_use_local.push_back(curr_blk);
                    } else {
                        // put all others in in_use while resolve conflict
                        switch (curr_blk->blktype) {
                            case OWNED:
                                errexit(
                                    "OWNED isn't a valid blktype in this "
                                    "version.");
                                break;
                            case ALLOC: {
                                auto insert_res =
                                    in_use_local.insert({curr_blk->id, curr_blk});
                                if (insert_res.second == false) {
                                    if (clean_start) {
                                        errexit(
                                            "more than one record with the "
                                            "same id after a clean exit.");
                                    }
                                    not_in_use_local.push_back(curr_blk);
                                }
                            } break;
                            case UPDATE: {
                                auto search = in_use_local.find(curr_blk->id);
                                if (search != in_use_local.end()) {
                                    if (clean_start) {
                                        errexit(
                                            "more than one record with the "
                                            "same id after a clean exit.");
                                    }
                                    if (curr_blk->epoch >
                                        search->second->epoch) {
                                        not_in_use_local.push_back(search->second);
                                        search->second =
                                            curr_blk;  // TODO: double-check if
                                                       // this is right.
                                    } else {
                                        not_in_use_local.push_back(curr_blk);
                                    }
                                } else {
                                    in_use_local.insert({curr_blk->id, curr_blk});
                                }
                            } break;
                            case DELETE:
                            case EPOCH:
                            case DESC:
                                break;
                            default:
                                errexit("wrong type of pblk discovered");
                                break;
                        }
                    }
                }
                // merge the results of in_use, resolve conflict
                pthread_barrier_wait(&sync_point);
                while (curr_reporting.load() != rec_tid)
                    ;
                for (auto itr : in_use_local) {
                    auto found = in_use->find(itr.first);
                    if (found == in_use->end()) {
                        in_use->insert({itr.first, itr.second});
                    } else if (found->second->get_epoch() <
                               itr.second->get_epoch()) {
                        not_in_use_local.push_back(found->second);
                        found->second = itr.second;
                    } else {
                        not_in_use_local.push_back(itr.second);
                    }
                }
                curr_reporting.store((rec_tid + 1) % rec_thd);
                // clean up not_in_use and anti-nodes
                for (auto itr : not_in_use_local) {
                    itr->set_epoch(NULL_EPOCH);
                    _ral->deallocate(itr, rec_tid);
                }
                for (auto itr : anti_nodes_local) {
                    itr->set_epoch(NULL_EPOCH);
                    _ral->deallocate(itr, rec_tid);
                }
            }));  // workers.emplace_back()
        }  // for (rec_thd)
        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }

        // set system mode back to online
        sys_mode = ONLINE;
        reset();

        std::cout << "returning from EpochSys Recovery." << std::endl;
#endif /* !MNEMOSYNE */
        return in_use;
    }


}// namespace pds