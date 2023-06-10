#ifndef RECOVERABLE_HPP
#define RECOVERABLE_HPP

#include "TestConfig.hpp"
#include "EpochSys.hpp"
#include <immintrin.h>
// TODO: report recover errors/exceptions

class Recoverable;

namespace pds{
    ////////////////////////////////////////
    // counted pointer-related structures //
    ////////////////////////////////////////

    /*
    * Macro VISIBLE_READ determines which version of API will be used.
    * Macro USE_TSX determines whether TSX (Intel HTM) will be used.
    * 
    * We highly recommend you to use default invisible read version,
    * since it doesn't need you to handle EpochVerifyException and you
    * can call just load rather than load_verify throughout your program
    * 
    * We provides following double-compare-single-swap (DCSS) API for
    * nonblocking data structures to use: 
    * 
    *  atomic_lin_var<T=uint64_t>: atomic double word for storing pointers
    *  that point to nodes, which link payloads in. It contains following
    *  functions:
    * 
    *      store(T val): 
    *          store 64-bit long data without sync; cnt doesn't increment
    * 
    *      store(lin_var d): store(d.val)
    * 
    *      lin_var load(): 
    *          load var without verifying epoch
    * 
    *      lin_var load_verify(): 
    *          load var and verify epoch, used as lin point; 
    *          for invisible reads this won't verify epoch
    * 
    *      bool CAS(lin_var expected, T desired): 
    *          CAS in desired value and increment cnt if expected 
    *          matches current var
    * 
    *      bool CAS_verify(lin_var expected, T desired): 
    *          CAS in desired value and increment cnt if expected 
    *          matches current var and global epoch doesn't change
    *          since BEGIN_OP
    */

    struct EpochVerifyException : public std::exception {
        const char * what () const throw () {
            return "Epoch in which operation wants to linearize has passed; retry required.";
        }
    };
}

class Recoverable{
    friend struct pds::sc_desc_t;
    template <class T>
    friend class pds::atomic_lin_var;

    
public:
    pds::EpochSys* _esys = nullptr;
    bool _preallocated_esys = false;

    // return num of blocks recovered.
    virtual int recover(bool simulated = false) = 0;
    Recoverable(GlobalTestConfig* gtc);
    virtual ~Recoverable();

    void init_thread(GlobalTestConfig*, LocalTestConfig* ltc);
    void init_thread(int tid);
    bool check_epoch(){
        return _esys->check_epoch();
    }
    bool check_epoch(uint64_t c){
        return _esys->check_epoch(c);
    }
    void begin_op(){
        _esys->begin_op();
    }
    void end_op(){
        _esys->end_op();
    }
    void end_readonly_op(){
        _esys->end_readonly_op();
    }
    void abort_op(){
        _esys->abort_op();
    }
    
    bool is_inside_txn(){
        return _esys->is_inside_txn();
    }
    void set_start_rolling_CAS(){
        _esys->set_start_rolling_CAS();
    }
    void reset_start_rolling_CAS(){
        _esys->reset_start_rolling_CAS();
    }
    bool is_rolling_CAS(){
        return _esys->is_rolling_CAS();
    }
    bool is_during_abort(){
        return _esys->is_during_abort();
    }


    struct tx_op_separator{
        pds::EpochSys* esys = nullptr;
        tx_op_separator(Recoverable* ds){
            esys=ds->_esys;
            esys->reset_start_rolling_CAS();
            if (!esys->is_inside_txn()){
                esys->tracker_start_op();
            } else {
                esys->reset_pending_reads();
            }
        }
        ~tx_op_separator(){
            if (!esys->is_inside_txn()){
                esys->tracker_end_op();
            }
        }
    };
    
    class MontageOpHolder{
        Recoverable* ds = nullptr;
    public:
        MontageOpHolder(Recoverable* ds_): ds(ds_){
            ds->begin_op();
        }
        ~MontageOpHolder(){
            ds->end_op();
        }
    };
    class MontageOpHolderReadOnly{
        Recoverable* ds = nullptr;
    public:
        MontageOpHolderReadOnly(Recoverable* ds_): ds(ds_){
            ds->begin_op();
        }
        ~MontageOpHolderReadOnly(){
            ds->end_readonly_op();
        }
    };
    pds::PBlk* pmalloc(size_t sz)
    {
        return _esys->pmalloc(sz);
    }
    template <typename T, typename... Types> 
    T* pnew(Types... args) 
    {
        return _esys->pnew<T>(args...);
    }

    template<typename T>
    void register_update_pblk(T* b){
        _esys->register_update_pblk(b);
    }
    // for lock-based
    template<typename T>
    void pdelete(T* b){
        _esys->pdelete(b);
    }
    // for nonblocking
    /* 
     * pretire() must be called BEFORE lin point, i.e., CAS_verify()!
     *
     * For nonblocking persistence, retirement will be initiated (by
     * craeating anti-node) at (if called before) begin_op, and will
     * be committed in end_op or withdrew (by deleting anti-node) at
     * abort_op;
     *
     * For blocking persistence, retirement will be buffered until
     * end_op, at which it will be initiated and commited (by creating
     * anti-node), or will withdrew (by deleting anti-node) at
     * abort_op.
     */
    template<typename T>
    void pretire(T* b){
       _esys->pretire(b);
    }
    // for nonblocking
    /*
     * Really reclaim a persistent payload in Montage data structures
     */
    template<typename T>
    void preclaim(T* b){
        _esys->preclaim(b);
    }


    void* tmalloc(size_t sz) {
        return _esys->tmalloc(sz);
    }
    template <typename T, typename... Types> 
    T* tnew(Types... args) {
        return _esys->tnew<T>(args...);
    }

    void tfree(void* obj) {
        _esys->tfree(obj);
    }
    template <typename T> 
    void tdelete(T* obj) {
        _esys->tdelete(obj);
    }
    template <typename T>
    void tretire(T* obj) {
        _esys->tretire(obj);
    }
    template <typename T>
    void txn_tretire(T* obj) {
        _esys->txn_tretire(obj);
    }

    template<typename T>
    const T* openread_pblk(const T* b){
        return _esys->openread_pblk(b);
    }
    template<typename T>
    const T* openread_pblk_unsafe(const T* b){
        // Wentao: skip checking epoch here since this may be called
        // during recovery, which may not have epochs[tid]
        // if (epochs[pds::EpochSys::tid].ui != NULL_EPOCH){
        //     return _esys->openread_pblk_unsafe(b);
        // } else {
            return b;
        // }
    }
    template<typename T>
    T* openwrite_pblk(T* b){
        return _esys->openwrite_pblk(b);
    }
    std::unordered_map<uint64_t, pds::PBlk*>* recover_pblks(const int rec_thd=10){
        return _esys->recover(rec_thd);
    }
    void sync(){
        _esys->sync();
    }
    void recover_mode(){
        _esys->sys_mode = pds::RECOVER; // PDELETE -> nop
    }
    void online_mode(){
        _esys->sys_mode = pds::ONLINE;
    }
    void flush(){
        for (int i = 0; i < 2; i++){
            sync();
        }
        // _esys->flush();
    }
    void simulate_crash(){
        _esys->simulate_crash();
    }

    pds::sc_desc_t* get_dcss_desc(){
        return _esys->get_dcss_desc();
    }
    uint64_t get_local_epoch(){
        return _esys->get_local_epoch();
    }



    template <class T>
    void addToReadSet(pds::atomic_lin_var<T>* addr, T val){
        return _esys->addToReadSet(
            reinterpret_cast<pds::atomic_lin_var<uint64_t>*>(addr), 
            reinterpret_cast<uint64_t>(val));
    }
    template <class T>
    void removeFromReadSet(pds::atomic_lin_var<T>* addr){
        return _esys->removeFromReadSet(
            reinterpret_cast<pds::atomic_lin_var<uint64_t>*>(addr));
    }
    void addToCleanups(std::function<void()> lambda){
        return _esys->addToCleanups(lambda);
    }
    void addToUndos(std::function<void()> lambda){
        return _esys->addToUndos(lambda);
    }
    void addToUnlocks(std::function<void()> lambda){
        return _esys->addToUnlocks(lambda);
    }
};

/////////////////////////////
// field generation macros //
/////////////////////////////

// macro for concatenating two tokens into a new token
#define TOKEN_CONCAT(a,b)  a ## b

/**
 *  using the type t and the name n, generate a protected declaration for the
 *  field, as well as public getters and setters
 */
#define GENERATE_FIELD(t, n, T)\
/* declare the field, with its name prefixed by m_ */\
protected:\
    t TOKEN_CONCAT(m_, n);\
public:\
/* get method open a pblk for read. */\
t TOKEN_CONCAT(get_, n)(Recoverable* ds) const{\
    return ds->openread_pblk(this)->TOKEN_CONCAT(m_, n);\
}\
/* get method open a pblk for read. Allows old-see-new reads. */\
t TOKEN_CONCAT(get_unsafe_, n)(Recoverable* ds) const{\
    return ds->openread_pblk_unsafe(this)->TOKEN_CONCAT(m_, n);\
}\
/* set method open a pblk for write. return a new copy when necessary */\
template <class in_type>\
T* TOKEN_CONCAT(set_, n)(Recoverable* ds, const in_type& TOKEN_CONCAT(tmp_, n)){\
    assert(ds->get_local_epoch() != NULL_EPOCH);\
    auto ret = ds->openwrite_pblk(this);\
    ret->TOKEN_CONCAT(m_, n) = TOKEN_CONCAT(tmp_, n);\
    ds->register_update_pblk(ret);\
    return ret;\
}\
/* set the field by the parameter. called only outside BEGIN_OP and END_OP */\
template <class in_type>\
void TOKEN_CONCAT(set_unsafe_, n)(Recoverable* ds, const in_type& TOKEN_CONCAT(tmp_, n)){\
    TOKEN_CONCAT(m_, n) = TOKEN_CONCAT(tmp_, n);\
}

/**
 *  using the type t, the name n and length s, generate a protected
 *  declaration for the field, as well as public getters and setters
 */
#define GENERATE_ARRAY(t, n, s, T)\
/* declare the field, with its name prefixed by m_ */\
protected:\
    t TOKEN_CONCAT(m_, n)[s];\
/* get method open a pblk for read. */\
t TOKEN_CONCAT(get_, n)(Recoverable* ds, int i) const{\
    return ds->openread_pblk(this)->TOKEN_CONCAT(m_, n)[i];\
}\
/* get method open a pblk for read. Allows old-see-new reads. */\
t TOKEN_CONCAT(get_unsafe_, n)(Recoverable* ds, int i) const{\
    return ds->openread_pblk_unsafe(this)->TOKEN_CONCAT(m_, n)[i];\
}\
/* set method open a pblk for write. return a new copy when necessary */\
T* TOKEN_CONCAT(set_, n)(Recoverable* ds, int i, t TOKEN_CONCAT(tmp_, n)){\
    assert(ds->get_local_epoch() != NULL_EPOCH);\
    auto ret = ds->openwrite_pblk(this);\
    ret->TOKEN_CONCAT(m_, n)[i] = TOKEN_CONCAT(tmp_, n);\
    ds->register_update_pblk(ret);\
    return ret;\
}

// TX_OP_SEPARATOR macros to be called at the beginning of every
// operation in Recoverable
#define TX_OP_SEPARATOR()\
    tx_op_separator sep(this);   

namespace pds{

#ifdef VISIBLE_READ
    // implementation of load, store, and cas for visible reads

    template<typename T>
    void atomic_lin_var<T>::store(Recoverable* ds,const T& desired){
        lin_var r;
        while(true){
            r = var.load();
            lin_var new_r(reinterpret_cast<uint64_t>(desired),r.cnt+1);
            if(var.compare_exchange_strong(r, new_r))
                break;
        }
    }

    template<typename T>
    void atomic_lin_var<T>::store_verify(Recoverable* ds,const T& desired){
        lin_var r;
        while(true){
            r = var.load();
            if(ds->check_epoch()){
                lin_var new_r(reinterpret_cast<uint64_t>(desired),r.cnt+1);
                if(var.compare_exchange_strong(r, new_r)){
                    break;
                }
            } else {
                throw EpochVerifyException();
            }
        }
    }

    template<typename T>
    lin_var atomic_lin_var<T>::load(EpochSys* esys){
        lin_var r;
        while(true){
            r = var.load();
            lin_var ret(r.val,r.cnt+1);
            if(var.compare_exchange_strong(r, ret))
                return ret;
        }
    }

    template<typename T>
    lin_var atomic_lin_var<T>::load(Recoverable* ds){
        return load(ds->_esys);
    }

    template<typename T>
    lin_var atomic_lin_var<T>::load_verify(Recoverable* ds){
        assert(ds->get_local_epoch() != NULL_EPOCH);
        lin_var r;
        while(true){
            r = var.load();
            if(ds->check_epoch()){
                lin_var ret(r.val,r.cnt+1);
                if(var.compare_exchange_strong(r, ret)){
                    return r;
                }
            } else {
                throw EpochVerifyException();
            }
        }
    }

    template<typename T>
    bool atomic_lin_var<T>::CAS_verify(Recoverable* ds, lin_var expected, const T& desired){
        bool not_in_operation = false;
        if(ds->get_local_epoch() == NULL_EPOCH){
            ds->begin_op();
            not_in_operation = true;
        }
        assert(ds->get_local_epoch() != NULL_EPOCH);
        if(ds->check_epoch()){
            lin_var new_r(reinterpret_cast<uint64_t>(desired),expected.cnt+1);
            bool ret = var.compare_exchange_strong(expected, new_r);
            if(ret == true){
                if(not_in_operation) ds->end_op();
            } else {
                if(not_in_operation) ds->abort_op();
            }
            return ret;
        } else {
            if(not_in_operation) ds->abort_op();
            return false;
        }
    }

    template<typename T>
    bool atomic_lin_var<T>::CAS(lin_var expected, const T& desired){
        lin_var new_r(reinterpret_cast<uint64_t>(desired),expected.cnt+1);
        return var.compare_exchange_strong(expected, new_r);
    }

#else /* !VISIBLE_READ */
    /* implementation of load and cas for invisible reads */

    template<typename T>
    void atomic_lin_var<T>::store(Recoverable* ds,const T& desired){
        lin_var r;
        while(true){
            r = var.load();
            if(r.is_desc()) {
                sc_desc_t* D = r.get_desc();
                D->helper_try_complete(ds, var, r);
                r.cnt &= (~0x3ULL);
                r.cnt+=4;
            }
            lin_var new_r(reinterpret_cast<uint64_t>(desired),r.cnt+4);
            if(var.compare_exchange_strong(r, new_r)){
                if(ds->is_inside_txn()){
                    // at here, the CAS succeeds, we note the new val in
                    // pending_reads
                    ds->_esys->addToPendingReads(this, new_r);
                }
                break;
            }
        }
        
    }

    template<typename T>
    void atomic_lin_var<T>::store_verify(Recoverable* ds,const T& desired){
        lin_var r;
        while(true){
            r = var.load();
            if(r.is_desc()){
                sc_desc_t* D = r.get_desc();
                D->helper_try_complete(ds, var, r);
                r.cnt &= (~0x3ULL);
                r.cnt+=4;
            }
            if(ds->check_epoch()){
                lin_var new_r(reinterpret_cast<uint64_t>(desired),r.cnt+4);
                if(var.compare_exchange_strong(r, new_r)){
                    break;
                }
            } else {
                throw EpochVerifyException();
            }
        }
    }

    template<typename T>
    lin_var atomic_lin_var<T>::full_load(EpochSys* esys){
        lin_var r = var.load();
        assert(!r.is_desc() || r.get_desc() != esys->get_dcss_desc());
        return r;
    }

    template<typename T>
    T atomic_lin_var<T>::load(EpochSys* esys){
        lin_var r;
        do { 
            r = var.load();
            if(r.is_desc()) {
                sc_desc_t* D = r.get_desc();
                assert(D != esys->get_dcss_desc());
                D->helper_try_complete(esys, var, r);
            }
        } while(r.is_desc());
        return reinterpret_cast<T>(r.val);
    }

    template<typename T>
    T atomic_lin_var<T>::load(Recoverable* ds){
        return load(ds->_esys);
    }

    template<typename T>
    T atomic_lin_var<T>::load_verify(Recoverable* ds){
        // invisible read doesn't need to verify epoch even if it's a
        // linearization point
        // this saves users from catching EpochVerifyException
        return load(ds);
    }

    template<typename T>
    bool atomic_lin_var<T>::CAS_verify(Recoverable* ds, T expected, const T& desired){
        bool not_in_operation = false;
        if(ds->get_local_epoch() == NULL_EPOCH){
            ds->begin_op();
            not_in_operation = true;
        }
        assert(ds->get_local_epoch() != NULL_EPOCH);
#ifdef USE_TSX
        // total_cnt.fetch_add(1);
        unsigned status = _xbegin();
        if (status == _XBEGIN_STARTED) {
            lin_var r = var.load();
            if(!r.is_desc()){
                if( r.val!=reinterpret_cast<uint64_t>(expected) ||
                    !ds->check_epoch()){
                    _xend();
                    if(not_in_operation) ds->abort_op();
                    return false;
                } else {
                    lin_var new_r (reinterpret_cast<uint64_t>(desired), r.cnt+4);
                    var.store(new_r);
                    _xend();
                    if(not_in_operation) ds->end_op();
                    return true;
                }
            } else {
                // we only help complete descriptor, but not retry
                _xend();
                r.get_desc()->helper_try_complete(ds, var, r);
                if(not_in_operation) ds->abort_op();
                return false;
            }
            // execution won't reach here; program should have returned
            assert(0);
        }
        // abort_cnt.fetch_add(1);
#endif
        // txn fails; fall back routine
        lin_var r = var.load();
        if(r.is_desc()){
            sc_desc_t* D = r.get_desc();
            D->helper_try_complete(ds, var, r);
            if(not_in_operation) ds->abort_op();
            return false;
        } else {
            if( r.val!=reinterpret_cast<uint64_t>(expected)) {
                if(not_in_operation) ds->abort_op();
                return false;
            }
        }

        // now r.cnt must be ..00, and r.cnt+1 is ..01, which means "var
        // contains a descriptor"
        assert((r.cnt & 3UL) == 0UL);
        bool added_to_write_set = ds->get_dcss_desc()->add_to_write_set(reinterpret_cast<atomic_lin_var<uint64_t>*>(this), r.cnt, reinterpret_cast<uint64_t>(expected), reinterpret_cast<uint64_t>(desired));
        assert(added_to_write_set);

        // set desc from in_prep to in_prog
        ds->get_dcss_desc()->set_ready();

        // install desc to var
        lin_var new_r(reinterpret_cast<uint64_t>(ds->get_dcss_desc()), r.cnt+1);
        if(!var.compare_exchange_strong(r,new_r)){
            if(not_in_operation) ds->abort_op();
            return false;
        }

        ds->get_dcss_desc()->owner_try_complete(ds);
        if(ds->get_dcss_desc()->committed()) {
            if(not_in_operation) ds->end_op();
            return true;
        }
        else {
            if(not_in_operation) ds->abort_op();
            return false;
        }
    }

    template<typename T>
    bool atomic_lin_var<T>::CAS(Recoverable* ds, T expected, const T& desired){
        // CAS doesn't check epoch; just cas ptr to desired, with cnt+=4
        // assert(!expected.is_desc());
        lin_var r = var.load();
        if(r.is_desc()){
            sc_desc_t* D = r.get_desc();
            assert(D != ds->_esys->get_dcss_desc());
            D->helper_try_complete(ds, var, r);
            return false;
        }
        lin_var old_r(reinterpret_cast<uint64_t>(expected), r.cnt);
        lin_var new_r(reinterpret_cast<uint64_t>(desired), r.cnt + 4);
        if(!var.compare_exchange_strong(old_r,new_r)){
            return false;
        }
        if(ds->is_inside_txn()){
            // at here, the CAS succeeds, we note the new val in
            // pending_reads
            ds->_esys->addToPendingReads(this, new_r);
        }
        return true;
    }

#endif /* !VISIBLE_READ */

    template<typename T>
    T atomic_lin_var<T>::nbtc_load(Recoverable* ds){
        bool is_speculative=false;
        return nbtc_load(ds, is_speculative);
    }
    template<typename T>
    T atomic_lin_var<T>::nbtc_load(Recoverable* ds, bool& is_speculative){
        if (!ds->is_inside_txn()){
            // given that load_verify is equivalent to load in
            // invisible reader version, we directly call load.
            is_speculative = false;
            return load(ds);
        }
        // nbtc load main body
        lin_var r;
        do { 
            r = var.load();
            if(r.is_desc()) {
                sc_desc_t* D = r.get_desc();
                if (D == ds->get_dcss_desc()){
                    //we saw our own desc
                    ds->set_start_rolling_CAS();
                    is_speculative = true;
                    assert(r.cnt-1 == D->get_cnt_from_write_set(
                        reinterpret_cast<atomic_lin_var<uint64_t>*>(this)));
                    return reinterpret_cast<T>(
                        D->get_val_from_write_set(
                            reinterpret_cast<atomic_lin_var<uint64_t>*>(this), 
                            true));
                } else {
                    D->helper_try_complete(ds, var, r);
                }
            }
        } while(r.is_desc());
        is_speculative = false;
        ds->_esys->addToPendingReads(this, r);
        return reinterpret_cast<T>(r.val);
    }
    template<typename T>
    int atomic_lin_var<T>::nbtc_CAS(Recoverable* ds, T expected, const T& desired, bool pub_point, bool lin_point){
        if (!ds->is_inside_txn()){
            if (lin_point)
                return CAS_verify(ds, expected, desired);
            else
                return CAS(ds, expected, desired);
        }
        // nbtc CAS main body
        if (pub_point) ds->set_start_rolling_CAS();

        lin_var r = var.load();
        if(r.is_desc()){
            sc_desc_t* D = r.get_desc();
            if (D == ds->get_dcss_desc()){
                // we saw our own desc; this usually happens in load
                // rather than CAS though.
                ds->set_start_rolling_CAS();
                assert (
                    ds->get_dcss_desc()->get_val_from_write_set(
                        reinterpret_cast<atomic_lin_var<uint64_t>*>(this), 
                        true) == 
                    reinterpret_cast<uint64_t>(expected));
            } else {
                D->helper_try_complete(ds, var, r);
                return 0;
            }
        } else {
            if( r.val!=reinterpret_cast<uint64_t>(expected)) {
                return 0;
            }
        }

        if (ds->is_rolling_CAS()){
            bool added_to_write_set = ds->get_dcss_desc()->add_to_write_set(
                reinterpret_cast<atomic_lin_var<uint64_t>*>(this), 
                r.cnt & ~3ULL, 
                reinterpret_cast<uint64_t>(expected), 
                reinterpret_cast<uint64_t>(desired));
            if (!added_to_write_set)
                ds->_esys->tx_abort();
            // install desc to var if not itself's desc
            int ret = 0;
            if (!r.is_desc()){
                lin_var new_r(reinterpret_cast<uint64_t>(ds->get_dcss_desc()), r.cnt+1);
                if (var.compare_exchange_strong(r,new_r))
                    ret = 2;
            } else {
                assert(r.get_desc() == ds->get_dcss_desc());
                ret = 2;
            }
            if (ret == 0){
                // CAS failed; remove entry from write set
                ds->get_dcss_desc()->remove_from_write_set(
                    reinterpret_cast<atomic_lin_var<uint64_t>*>(this));
            }
            if (lin_point && ret != 0){
                // lin point succeeds; turn off rolling_CAS
                ds->reset_start_rolling_CAS();
            }
            return ret;
        } else {
            return CAS(ds, expected, desired);
        }
    }

    template<typename T>
    void atomic_lin_var<T>::nbtc_store(Recoverable* ds,const T& desired){
        if (!ds->is_inside_txn()){
            return store_verify(ds, desired);
        }
        // nbtc store main body
        lin_var r;
        while(true){
            r = var.load();
            if(r.is_desc()){
                sc_desc_t* D = r.get_desc();
                if (D == ds->get_dcss_desc()){
                    // we saw our own desc
                    // update write set and return
                    ds->set_start_rolling_CAS();
                    ds->get_dcss_desc()->add_to_write_set_no_revalidation(
                        reinterpret_cast<atomic_lin_var<uint64_t>*>(this), 
                        r.cnt & ~0x3ULL, 
                        reinterpret_cast<uint64_t>(desired));
                    break;
                } else {
                    D->helper_try_complete(ds, var, r);
                    continue; // retry until it's no longer desc
                }
            }
            bool added_to_write_set = ds->get_dcss_desc()->add_to_write_set(
                reinterpret_cast<atomic_lin_var<uint64_t>*>(this), 
                r.cnt, 
                r.val, 
                reinterpret_cast<uint64_t>(desired));
            if(!added_to_write_set)
                ds->_esys->tx_abort();

            // install desc to var
            lin_var new_r(
                reinterpret_cast<uint64_t>(ds->get_dcss_desc()), 
                r.cnt+1);
            if(var.compare_exchange_strong(r, new_r)){
                break;
            }
        }
    }
} // namespace pds

#endif