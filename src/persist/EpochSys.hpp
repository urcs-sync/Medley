#ifndef EPOCH_HPP
#define EPOCH_HPP

#include <atomic>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <map>
#include <thread>
#include <condition_variable>
#include <string>
#include <exception>
#include <type_traits>

#include "TestConfig.hpp"
#include "ConcurrentPrimitives.hpp"
#include "PersistFunc.hpp"
#include "HarnessUtils.hpp"
#include "Persistent.hpp"
#include "persist_utils.hpp"
#include "RCUTracker.hpp"

#include "common_macros.hpp"
#include "TransactionTrackers.hpp"
#include "PerThreadContainers.hpp"
#include "ToBePersistedContainers.hpp"
#include "ToBeFreedContainers.hpp"
#include "EpochAdvancers.hpp"
#include "PersistTrackers.hpp"
#include "LockfreeSlabHashTable.hpp"

class Recoverable;

namespace pds{
// extern std::atomic<size_t> abort_cnt;
// extern std::atomic<size_t> total_cnt;
struct OldSeeNewException : public std::exception {
    const char * what () const throw () {
        return "OldSeeNewException not handled.";
    }
};

enum PBlkType : uint64_t {INIT, ALLOC, UPDATE, DELETE, RECLAIMED, EPOCH, OWNED, DESC};

class EpochSys;

/////////////////////////////
// PBlk-related structures //
/////////////////////////////

class PBlk{
    friend class EpochSys;
    friend class nbEpochSys;
    friend class Recoverable;
protected:
    // Wentao: the first word should NOT be any persistent value for
    // epoch-system-level recovery (i.e., epoch), as Ralloc repurposes the first
    // word for block free list, which may interfere with the recovery.
    // Currently we use (transient) "reserved" as the first word. If we decide to
    // remove this field, we need to either prepend another dummy word, or
    // change the block free list in Ralloc.

    /* the first field is 64bit vptr */

    // transient.
    PBlk* retire = nullptr;

    /* 
     * Wentao: Please keep the order of the members below consistent
     * with those in sc_desc_t! There will be a reinterpret_cast
     * between these two during recovery.
     */
    uint64_t epoch = NULL_EPOCH;
    PBlkType blktype = INIT;
    // 14MSB for tid, 48 for sn, 2LSB unused; for nbEpochSys
    uint64_t tid_sn = 0;
    uint64_t id = 0;

public:
    void set_epoch(uint64_t e){
        // only for testing
        epoch=e;
    }
    uint64_t get_epoch(){
        return epoch;
    }
    // id gets inited by EpochSys instance.
    PBlk(): retire(nullptr), epoch(NULL_EPOCH), blktype(INIT)/*, owner_id(0)*/{}
    // id gets inited by EpochSys instance.
    PBlk(const PBlk* owner):
        retire(nullptr), blktype(OWNED)/*, owner_id(owner->blktype==OWNED? owner->owner_id : owner->id)*/ {}
    PBlk(const PBlk& oth): retire(nullptr), blktype(oth.blktype==OWNED? OWNED:INIT)/*, owner_id(oth.owner_id)*/, id(oth.id) {}
    inline uint64_t get_id() {return id;}
    virtual pptr<PBlk> get_data() {return nullptr;}
    virtual ~PBlk(){
        // Wentao: we need to zeroize epoch and flush it, avoiding it left after free
        epoch = NULL_EPOCH;
        // persist_func::clwb(&epoch);
    }

    /* functions for nbEpochSys */
    inline uint64_t get_tid() const {
        return (0x3fffULL<<50 & tid_sn)>>50;
    }
    inline uint64_t get_sn() const {
        return (0x3ffffffffffffULL & tid_sn)>>2;
    }
    inline void set_tid_sn(uint64_t tid, uint64_t sn){
        assert(tid < 16384 && sn <= 0xffffffffffffULL);
        tid_sn = (tid<<50) | ((sn & 0xffffffffffffULL)<<2);
    }
    inline void set_tid(uint64_t tid){
        assert(tid < 16384);
        tid_sn = (tid<<50) | (tid_sn & 0x3ffffffffffffULL);
    }
    inline void increment_sn(){
        assert(((tid_sn&0x3ffffffffffffULL)>>2) != 0xffffffffffffULL);
        tid_sn+=4;
    }
    inline bool retired(){
        return retire != nullptr;
    }
};

template<typename T>
class PBlkArray : public PBlk{
    friend class EpochSys;
    friend class nbEpochSys;
    size_t size;
    // NOTE: see EpochSys::alloc_pblk_array() for its sementical allocators.
    PBlkArray(): PBlk(){}
    PBlkArray(PBlk* owner) : PBlk(owner), content((T*)((char*)this + sizeof(PBlkArray<T>))){}
public:
    PBlkArray(const PBlkArray<T>& oth): PBlk(oth), size(oth.size),
        content((T*)((char*)this + sizeof(PBlkArray<T>))){}
    virtual ~PBlkArray(){};
    T* content; //transient ptr
    inline size_t get_size()const{return size;}
};

struct Epoch : public PBlk{
    std::atomic<uint64_t> global_epoch;
    void persist(){}
    Epoch(){
        global_epoch.store(NULL_EPOCH, std::memory_order_relaxed);
    }
};

//////////////////
// Epoch System //
//////////////////

enum SysMode {ONLINE, RECOVER};


struct sc_desc_t;

template <class T>
class atomic_lin_var;
class lin_var{
    template <class T>
    friend class atomic_lin_var;
    inline bool is_desc() const {
        return (cnt & 3ULL) == 1ULL;
    }
    inline sc_desc_t* get_desc() const {
        assert(is_desc());
        return reinterpret_cast<sc_desc_t*>(val);
    }
public:
    uint64_t val;
    uint64_t cnt;
    template <typename T=uint64_t>
    inline T get_val() const {
        static_assert(sizeof(T) == sizeof(uint64_t), "sizes do not match");
        return reinterpret_cast<T>(val);
    }
    lin_var(uint64_t v, uint64_t c = 0) : val(v), cnt(c) {};
    lin_var() : lin_var(0, 0) {};

    inline bool operator==(const lin_var & b) const{
        return val==b.val && cnt==b.cnt;
    }
    inline bool operator!=(const lin_var & b) const{
        return !operator==(b);
    }
}__attribute__((aligned(16)));

template <class T = uint64_t>
class atomic_lin_var{
    static_assert(sizeof(T) == sizeof(uint64_t), "sizes do not match");
public:
    // for cnt in var:
    // desc: ....01
    // real val: ....00
    std::atomic<lin_var> var;

    lin_var full_load(EpochSys* esys);
    T load(EpochSys* esys);
    T load(Recoverable* ds);
    T load_verify(Recoverable* ds);
    bool CAS_verify(Recoverable* ds, T expected, const T& desired);
    // CAS doesn't check epoch nor cnt
    bool CAS(Recoverable* ds, T expected, const T& desired);
    void store(Recoverable* ds,const T& desired);
    void store_verify(Recoverable* ds,const T& desired);

    // nbtc operations
    T nbtc_load(Recoverable* ds);
    T nbtc_load(Recoverable* ds, bool& is_speculative);
    // 0: fail; 1: succeed immediately; 2: succeed speculatively
    int nbtc_CAS(Recoverable* ds, T expected, const T& desired, bool pub_point, bool lin_point);
    void nbtc_store(Recoverable* ds,const T& desired);

    atomic_lin_var(const T& v) : var(lin_var(reinterpret_cast<uint64_t>(v), 0)){};
    atomic_lin_var() : atomic_lin_var(T()){};
};

struct alignas(64) sc_desc_t{
protected:
    friend class EpochSys;
    friend class nbEpochSys;
    friend class Recoverable;
    template <class T>
    friend class atomic_lin_var;
    struct write_set_entry {
        uint64_t old_cnt;
        uint64_t old_val;
        uint64_t new_val;
    };
    // Wentao: the first word should NOT be any persistent value for
    // epoch-system-level recovery (i.e., epoch), as Ralloc repurposes the first
    // word for block free list, which may interfere with the recovery.
    // Currently we use (transient) "reserved" as the first word. If we decide to
    // remove this field, we need to either prepend another dummy word, or
    // change the block free list in Ralloc.
    using ReadSetType = 
        LockfreeSlabHashTable<atomic_lin_var<uint64_t>*, lin_var, 127>;
    using WriteSetType = 
        LockfreeSlabHashTable<atomic_lin_var<uint64_t>*, write_set_entry, 102>;
    using ReadSetIterator = ReadSetType::ItemIterator;
    using WriteSetIterator = WriteSetType::ItemIterator;

    // transient.
    ReadSetType* read_set;
    // new value and old cnt
    WriteSetType* write_set;

    /* 
     * Wentao: Please keep the order of the members below consistent
     * with those in PBlk! There will be a reinterpret_cast
     * between these two during recovery.
     */
    uint64_t epoch = NULL_EPOCH;
    PBlkType blktype = DESC;
    // 14MSB for tid, 48 for sn, 2LSB for status; for nbEpochSys
    // in progress: ....01
    // committed: ....10 
    // aborted: ....11
    std::atomic<uint64_t> tid_sn = 0;

    inline uint64_t combine_tid_sn_status (uint64_t tid, uint64_t sn, uint64_t status){
        return (tid<<50) | ((sn & 0xffffffffffffULL)<<2) | (status & 0x3ULL);
    }
    inline bool abort(uint64_t _d){
        // bring cnt from ..0x to ..11
        uint64_t expected = _d & ~0x2ULL; // in prep or in progress
        uint64_t desired = expected;
        desired |= 0x3ULL;
        return tid_sn.compare_exchange_strong(expected, desired);
    }
    inline bool commit(uint64_t _d){
        // bring cnt from ..01 to ..10
        uint64_t expected = (_d & ~0x3ULL) | 1ULL; // in progress
        uint64_t desired = expected;
        desired += 1;
        return tid_sn.compare_exchange_strong(expected, desired);
    }
    inline bool in_prep(uint64_t _d) const {
        return (_d & 0x3ULL) == 0ULL;
    }
    inline bool in_prep() const {
        return in_prep(tid_sn.load());
    }
    inline bool in_progress(uint64_t _d) const {
        return (_d & 0x3ULL) == 1ULL;
    }
    inline bool in_progress() const {
        return in_progress(tid_sn.load());
    }
    inline bool committed(uint64_t _d) const {
        return (_d & 0x3ULL) == 2ULL;
    }
    inline bool committed() const {
        return committed(tid_sn.load());
    }
    inline bool aborted(uint64_t _d) const {
        return (_d & 0x3ULL) == 3ULL;
    }
    inline bool aborted() const {
        return aborted(tid_sn.load());
    }
    // Check if they correspond to the same txn instance
    inline bool match(uint64_t old_d, uint64_t new_d) const {
        return ((old_d & ~0x3ULL) == (new_d & ~0x3ULL));
    }

    // This signature is called only by helper threads
    void helper_uninstall_desc(uint64_t old_d);
    // This signature is called only by the owner thread of the desc
    void owner_uninstall_desc();

    // this signature is called only by the owner thread of the desc
    bool owner_validate_reads(EpochSys* esys);
    // this signature is called only by try_complete, which must be
    // called by helper threads
    bool helper_validate_reads(EpochSys* esys, uint64_t _d);
public:
    void set_epoch(uint64_t e){
        // only for testing
        epoch=e;
    }
    uint64_t get_epoch(){
        return epoch;
    }

    void add_to_write_set_no_revalidation(atomic_lin_var<uint64_t>* addr, uint64_t old_cnt, uint64_t new_val){
        // this routine is only used in nbtc_store when txn encounters
        // its own descriptor.
        auto res = write_set->get(addr);
        assert (res.has_value());
        assert(res->old_cnt == old_cnt || aborted());
        write_set->put(addr, {res->old_cnt, res->old_val, new_val});
        // res->new_val = new_val;
    }

    bool add_to_write_set(atomic_lin_var<uint64_t>* addr, uint64_t old_cnt, uint64_t old_val, uint64_t new_val){
        auto w_res = write_set->get(addr);
        if (w_res.has_value()){
            // exist; update
            if(w_res->new_val != old_val || w_res->old_cnt != old_cnt) {
                assert(aborted());
                return false;
            }
            assert(!read_set->get(addr).has_value() || read_set->get(addr).value().cnt == old_cnt);
            write_set->put(addr, {w_res->old_cnt, w_res->old_val, new_val});
            // w_res->new_val = new_val;
        } else {
            // not exist; add
            auto r_res = read_set->get(addr);
            if(r_res.has_value()){
                // if it exists in read set and is inconsistent with 
                // old_cnt, then we should abort early
                if (r_res->val != old_val || r_res->cnt != old_cnt)
                    return false;
            }
            write_set->put(addr, {old_cnt, old_val, new_val});
        }
        return true;
    }

    uint64_t get_cnt_from_write_set(atomic_lin_var<uint64_t>* addr){
        auto res = write_set->get(addr);
        assert (res.has_value());
        return res->old_cnt;
    }

    // if for_new_val is true, return new val of the entry; otherwise,
    // return old value
    // assume that the entry must exist
    uint64_t get_val_from_write_set(atomic_lin_var<uint64_t>* addr, bool for_new_val){
        auto res = write_set->get(addr);
        assert (res.has_value());
        if(for_new_val) return res->new_val;
        else return res->old_val;
    }

    void remove_from_write_set(atomic_lin_var<uint64_t>* addr){
        write_set->remove(addr);
    }

    // return false if read mismatches and needs to abort
    bool add_to_read_set(atomic_lin_var<uint64_t>* addr, const lin_var& var){
        // First check if in write set
        auto w_res = write_set->get(addr);
        if (w_res.has_value()) {
            // it's a speculative read; don't add to read set and
            // directly return true
            if (var.cnt == w_res->old_cnt && var.val == w_res->new_val) {
                return true;
            } else {
                assert(aborted());
                return false;
            }
        }

        auto res = read_set->get(addr);
        if (res.has_value()){
            // exist; check if equal
            return *res == var;
        } else {
            // not exist; add
            read_set->put(addr, var);
            return true;
        }
    }
    
    void remove_from_read_set(atomic_lin_var<uint64_t>* addr){
        read_set->remove(addr);
    }

    inline bool set_ready(){
        // bring cnt from ..00 to ..01
        // if fails, the txn has been aborted before it's ready
        uint64_t _d = tid_sn.load();
        uint64_t expected = (_d & ~0x3ULL); // in preparation
        uint64_t desired = expected;
        desired += 1;
        return tid_sn.compare_exchange_strong(expected, desired);
    }

    inline bool set_unready(){
        // bring cnt from .x01 to ..(x+1)00
        uint64_t _d = tid_sn.load();
        uint64_t expected = (_d & ~0x3ULL) | 1ULL; // in progress
        uint64_t desired = expected + 3;
        return tid_sn.compare_exchange_strong(expected, desired);
    }

    ~sc_desc_t(){
        // Wentao: we need to zeroize epoch and flush it, avoiding it
        // left after free
        // shouldn't be called
        epoch = NULL_EPOCH;
        // persist_func::clwb(&epoch);
    }

    /* functions for nbEpochSys */
    inline uint64_t get_tid_sn_status() const {
        return tid_sn.load();
    }
    inline uint64_t get_tid() const {
        return (0x3fffULL<<50 & tid_sn.load())>>50;
    }
    inline uint64_t get_sn() const {
        return (0x3ffffffffffffULL & tid_sn.load())>>2;
    }
    inline void set_tid_sn(uint64_t tid, uint64_t sn){
        assert(tid < 16384 && sn <= 0xffffffffffffULL);
        tid_sn.store((tid<<50) | ((sn & 0xffffffffffffULL)<<2));
    }
    inline void set_tid(uint64_t tid){
        assert(tid < 16384);
        tid_sn.store((tid<<50) | (tid_sn.load() & 0x3ffffffffffffULL));
    }
    inline void increment_sn_reset_status(){
        assert(((tid_sn.load()&0x3ffffffffffffULL)>>2) != 0xffffffffffffULL);
        tid_sn.store((tid_sn.load()+4) & ~0x3ULL);
    }

    // helper_try_complete is called by non-owner threads
    void helper_try_complete(EpochSys* esys, std::atomic<lin_var>& obj, lin_var obj_var);
    void helper_try_complete(Recoverable* ds, std::atomic<lin_var>& obj, lin_var obj_var);
    // owner call owner_try_complete during single non-transactional CAS
    void owner_try_complete(Recoverable* ds);

    void try_abort(uint64_t expected_e);

    void reinit();
    inline void set_up_epoch(uint64_t e){
        // set up epoch in begin_op
        epoch = e;
    }
    sc_desc_t(uint64_t t) : epoch(NULL_EPOCH), blktype(DESC), tid_sn(0) {
        set_tid_sn(t, 0);
        read_set = new ReadSetType();
        write_set = new WriteSetType();
    };
    // sc_desc_t() : sc_desc_t(0){};
};
static_assert(sizeof(sc_desc_t)==64, "the size of sc_desc_t exceeds 64!");

struct TransactionAborted : public std::exception{ };
struct AbortDuringCommit : public TransactionAborted { };
struct AbortBeforeCommit : public TransactionAborted { };

class EpochSys{
protected:
    // persistent fields:
    Epoch* epoch_container = nullptr;
    std::atomic<uint64_t>* global_epoch = nullptr;
    // local descriptors for DCSS
    sc_desc_t** local_descs = nullptr;

    // semi-persistent fields:
    // TODO: set a periodic-updated persistent boundary to recover to.
    UIDGenerator uid_generator;

    // transient fields:
    TransactionTracker* trans_tracker = nullptr;
    ToBePersistContainer* to_be_persisted = nullptr;
    ToBeFreedContainer* to_be_freed = nullptr;
    EpochAdvancer* epoch_advancer = nullptr;
    PersistTracker* persisted_epochs = nullptr;

    RCUTracker tracker;
    GlobalTestConfig* gtc = nullptr;
    Ralloc* _ral = nullptr;
    int task_num;
    static std::atomic<int> esys_num;

    /* containers from Recoverable */ 
    // current epoch of each thread.
    padded<uint64_t>* epochs = nullptr;
    // last epoch of each thread, for sync().
    padded<uint64_t>* last_epochs = nullptr;
    // containers for pending persistent allocations
    padded<std::vector<PBlk*>>* pending_allocs = nullptr;
    // pending retires; each pair is <original payload, anti-payload>
    padded<std::vector<std::pair<PBlk*,PBlk*>>>* pending_retires = nullptr;
    padded<std::unordered_map<atomic_lin_var<uint64_t>*, lin_var>>* pending_reads = nullptr;

    /* containers for transactional composition */
    padded<std::vector<std::function<void()>>>* cleanups; // for nb ops. executed FIFO
    padded<std::vector<std::function<void()>>>* undos; // for b ops. executed LIFO
    // Given possible multiple calls to the same lock in a txn, consider recursive_lock
    padded<std::vector<std::function<void()>>>* unlocks; // each element is a unlock lambda. executed LIFO.
    padded<std::unordered_map<void*, std::function<void(void*)>>>* allocs; // transient allocs and its delete func
    struct alignas(64) Flags{
        bool start_rolling_CAS = false;
        bool inside_txn = false;
        bool is_during_abort = false;
    };
    Flags* flags = nullptr;
public:

    /* static */
    static thread_local int tid;
    
    // system mode that toggles on/off PDELETE for recovery purpose.
    SysMode sys_mode = ONLINE;


    EpochSys(GlobalTestConfig* _gtc) : uid_generator(_gtc->task_num), tracker(_gtc->task_num, 100, 1000, true), gtc(_gtc) {
        // init main thread
        pds::EpochSys::init_thread(0);
        std::string heap_name = get_ralloc_heap_name();
        // task_num+1 to construct Ralloc for dedicated epoch advancer
        _ral = new Ralloc(_gtc->task_num+1,heap_name.c_str(),REGION_SIZE);
        local_descs = new sc_desc_t* [gtc->task_num];
        // [wentao] FIXME: this may need to change if recovery reuses
        // existing descs
        for(int i=0;i<gtc->task_num;i++){
            // Wentao: although in blocking Montage, descriptors don't
            // need to be persistent at all, we still allocate them in
            // NVM for simplicity. By some experiments, we confirmed
            // that allocating them in DRAM or NVM doesn't affect the performance.
            local_descs[i] = new sc_desc_t(i);
            assert(local_descs[i]!=nullptr);
        }
        
        epochs = new padded<uint64_t>[gtc->task_num];
        last_epochs = new padded<uint64_t>[_gtc->task_num];
        for(int i = 0; i < gtc->task_num; i++){
            epochs[i].ui = NULL_EPOCH;
            last_epochs[i].ui = NULL_EPOCH;
        }
        pending_allocs = new padded<std::vector<PBlk*>>[gtc->task_num];
        pending_retires = new padded<std::vector<std::pair<PBlk*,PBlk*>>>[gtc->task_num];
        pending_reads = new padded<std::unordered_map<atomic_lin_var<uint64_t>*, lin_var>>[gtc->task_num];

        cleanups = new padded<std::vector<std::function<void()>>>[_gtc->task_num]();
        undos = new padded<std::vector<std::function<void()>>>[_gtc->task_num]();
        unlocks = new padded<std::vector<std::function<void()>>>[_gtc->task_num]();
        allocs = new padded<std::unordered_map<void*, std::function<void(void*)>>>[_gtc->task_num]();
        flags = new Flags[_gtc->task_num]();

        persist_func::sfence();
        reset(); // TODO: change to recover() later on.
    }

    // void flush(){
    //     for (int i = 0; i < 2; i++){
    //         sync(NULL_EPOCH);
    //     }
    // }

    virtual ~EpochSys(){
        // std::cout<<"epochsys descructor called"<<std::endl;
        trans_tracker->finalize();
        // flush(); // flush is done in epoch_advancer's destructor.
        if (epoch_advancer){
            delete epoch_advancer;
        }
        if(local_descs){
            delete local_descs;
        }
        if (gtc->verbose){
            std::cout<<"final epoch:"<<global_epoch->load()<<std::endl;
        }
        
        delete trans_tracker;
        delete persisted_epochs;
        delete to_be_persisted;
        delete to_be_freed;
        delete _ral;

        delete pending_allocs;
        delete pending_retires;
        delete pending_reads;
        delete epochs;
        delete last_epochs;

        delete cleanups; 
        delete undos;
        delete unlocks;
        delete allocs;
        // std::cout<<"Aborted:Total = "<<abort_cnt.load()<<":"<<total_cnt.load()<<std::endl;
    }

    virtual void parse_env();

    std::string get_ralloc_heap_name(){
        if (!gtc->checkEnv("HeapName")){
            int esys_id = esys_num.fetch_add(1);
            assert(esys_id<=0xfffff);
            char* heap_prefix = (char*) malloc(L_cuserid+10);
            cuserid(heap_prefix);
            char* heap_suffix = (char*) malloc(12);
            sprintf(heap_suffix, "_mon_%06X", esys_id);
            strcat(heap_prefix, heap_suffix);
            gtc->setEnv("HeapName", std::string(heap_prefix));
        }
        std::string ret;
        if (gtc->checkEnv("HeapName")){
            ret = gtc->getEnv("HeapName");
        }
        return ret;
    }

    virtual void reset(){
        task_num = gtc->task_num;
        if (!epoch_container){
            epoch_container = new_pblk<Epoch>();
            epoch_container->blktype = EPOCH;
            global_epoch = &epoch_container->global_epoch;
        }
        global_epoch->store(INIT_EPOCH, std::memory_order_relaxed);
        parse_env();
    }

    void simulate_crash(){
        assert(tid==0 && "simulate_crash can only be called by main thread");
        // if(tid==0){
            delete epoch_advancer;
            epoch_advancer = nullptr;
        // }
        _ral->simulate_crash();
    }

    ////////////////
    // Operations //
    ////////////////

    static void init_thread(int _tid){
        EpochSys::tid = _tid;
        Ralloc::set_tid(_tid);
    }

    void* malloc_pblk(size_t sz){
        return _ral->allocate(sz);
    }

    // malloc_pblk with pending_allocs stuff
    PBlk* pmalloc(size_t sz){
        PBlk* ret = (PBlk*)malloc_pblk(sz);
        if (epochs[tid].ui == NULL_EPOCH){
            pending_allocs[tid].ui.push_back(ret);
        } else {
            register_alloc_pblk(ret, epochs[tid].ui);
        }
        return ret;
    }

    // allocate a T-typed block on Ralloc and
    // construct using placement new
    template <class T, typename... Types>
    T* new_pblk(Types... args){
        T* ret = (T*)_ral->allocate(sizeof(T));
        new (ret) T (args...);
        return ret;
    }

    // new_pblk with pending_allocs stuff
    template <typename T, typename... Types> 
    T* pnew(Types... args) 
    {
        T* ret = new_pblk<T>(args...);
        if (epochs[tid].ui == NULL_EPOCH){
            pending_allocs[tid].ui.push_back(ret);
        } else {
            register_alloc_pblk(ret, epochs[tid].ui);
        }
        return ret;
    }

    // deallocate pblk, giving it back to Ralloc
    template <class T>
    void delete_pblk(T* pblk, uint64_t c){
        pblk->~T();
        _ral->deallocate(pblk);
        if (sys_mode == ONLINE && c != NULL_EPOCH){
            if (tid >= gtc->task_num){
                // if this thread does not have to-be-presisted buffer
                persist_func::clwb(pblk);
            } else {
                to_be_persisted->register_persist_raw((PBlk*)pblk, c);
            }
        }
    }

    // delete_pblk with pending_allocs stuff
    template<typename T>
    void pdelete(T* b){
        ASSERT_DERIVE(T, PBlk);
        ASSERT_COPY(T);

        if (sys_mode == ONLINE){
            if (epochs[tid].ui != NULL_EPOCH){
                free_pblk(b, epochs[tid].ui);
            } else {
                if (((PBlk*)b)->get_epoch() == NULL_EPOCH){
                    std::reverse_iterator pos = std::find(pending_allocs[tid].ui.rbegin(),
                        pending_allocs[tid].ui.rend(), b);
                    assert(pos != pending_allocs[tid].ui.rend());
                    pending_allocs[tid].ui.erase((pos+1).base());
                }
                delete_pblk(b, epochs[tid].ui);
            }
        }
    }

    template<typename T>
    void pretire(T* b){
        if(epochs[tid].ui == NULL_EPOCH){
            // buffer retirement in pending_retires; it will be
            // initiated at begin_op
            pending_retires[tid].ui.emplace_back(b, nullptr);
        } else {
            // for nonblocking, place anti-nodes and retires into
            // pending_retires and set tid_sn
            // for blocking, buffer retire request in pending_retires
            // to be committed at end_op or withdrew at abort_op
            prepare_retire_pblk(b, epochs[tid].ui, pending_retires[tid].ui);
        }
    }

    void clear_pending_retires(){
        if(!pending_retires[tid].ui.empty()){
            for(const auto& r : pending_retires[tid].ui){
                withdraw_retire_pblk(r.second,epochs[tid].ui);
            }
            pending_retires[tid].ui.clear();
        }
    }

    template<typename T>
    void preclaim(T* b){
        if (sys_mode == ONLINE){
            bool not_in_operation = false;
            if (epochs[tid].ui == NULL_EPOCH){
                not_in_operation = true;
                epochs[tid].ui = begin_reclaim_transaction();
                if(!b->retired()){
                    // WARNING!!!
                    // Wentao: here we optimize so that only nodes not
                    // ever published (and thus not ever retired) will
                    // get into this part. This is only applicable for
                    // nonblocking data structures. For blocking ones,
                    // don't call nb-specific pretire and preclaim,
                    // but instead call pdelete!
                    std::reverse_iterator pos = std::find(pending_allocs[tid].ui.rbegin(),
                            pending_allocs[tid].ui.rend(), b);
                    if(pos != pending_allocs[tid].ui.rend()){
                        pending_allocs[tid].ui.erase((pos+1).base());
                    }
                }
            }
            reclaim_pblk(b, epochs[tid].ui);
            if (not_in_operation){
                end_reclaim_transaction(epochs[tid].ui);
            }
        }
    }

    // check if global is the same as c.
    bool check_epoch(uint64_t c);
    bool check_epoch();
    
    uint64_t get_local_epoch();

    // return thread-local dcss desc
    inline sc_desc_t* get_dcss_desc(){
        return local_descs[tid];
    }

    // start op in the current epoch c.
    // prevent current epoch advance from c+1 to c+2.
    // Wentao: for nonblocking persistence, epoch may advance and
    // ongoing progress will be aborted
    virtual void begin_op();
 
    // end op, release the holding of epoch increments.
    virtual void end_op();

    // auto begin and end a reclaim-only transaction
    virtual uint64_t begin_reclaim_transaction();
    virtual void end_reclaim_transaction(uint64_t c);

    // end read only transaction, release the holding of epoch increments.
    virtual void end_readonly_op();

    // abort transaction, release the holding of epoch increments without other traces.
    virtual void abort_op();

    // validate an access in epoch c. throw exception if last update is newer than c.
    void validate_access(const PBlk* b, uint64_t c);

    // register the allocation of a PBlk during a transaction.
    // called for new blocks at both pnew (holding them in
    // pending_allocs) and begin_op (registering them with the
    // acquired epoch).
    virtual void register_alloc_pblk(PBlk* b, uint64_t c);

    template<typename T>
    T* reset_alloc_pblk(T* b, uint64_t c);

    template<typename T>
    PBlkArray<T>* alloc_pblk_array(size_t s, uint64_t c);

    template<typename T>
    PBlkArray<T>* alloc_pblk_array(PBlk* owenr, size_t s, uint64_t c);

    template<typename T>
    PBlkArray<T>* copy_pblk_array(const PBlkArray<T>* oth, uint64_t c);

    // register update of a PBlk during a transaction.
    // called by the API.
    void register_update_pblk(PBlk* b);

    // free a PBlk during a transaction.
    template<typename T>
    void free_pblk(T* b, uint64_t c);

    // for blocking persistence, buffer retire requests.
    // First version is called in pretire
    virtual void prepare_retire_pblk(PBlk* b, const uint64_t& c, std::vector<std::pair<PBlk*,PBlk*>>& pending_retires);
    // Second version is called in begin_op and tx_end
    virtual void prepare_retire_pblk(std::pair<PBlk*,PBlk*>& pending_retire, const uint64_t& c);

    virtual void withdraw_retire_pblk(PBlk* b, uint64_t c);

    // for blocking persistence, retire a PBlk during a transaction.
    virtual void retire_pblk(PBlk* b, uint64_t c, PBlk* anti=nullptr);

    // reclaim a retired PBlk.
    template<typename T>
    void reclaim_pblk(T* b, uint64_t c);

    // read a PBlk during a transaction.
    template<typename T>
    const T* openread_pblk(const T* b);

    // read a PBlk during a transaction, without ever throwing OldSeeNew.
    template<typename T>
    const T* openread_pblk_unsafe(const T* b);

    // get a writable copy of a PBlk.
    template<typename T>
    T* openwrite_pblk(T* b);

    // block, call for persistence of epoch c, and wait until finish.
    void sync(){
        assert(epochs[tid].ui == NULL_EPOCH);
        epoch_advancer->sync(last_epochs[tid].ui);
    }

    /////////////////
    // Bookkeeping //
    /////////////////

    // get the current global epoch number.
    uint64_t get_epoch();

    bool epoch_CAS(uint64_t& expected, const uint64_t& desired){
        return global_epoch->compare_exchange_strong(expected, desired);
    }

    // // try to advance global epoch, helping others along the way.
    // void advance_epoch(uint64_t c);

    // // a version of advance_epoch for a SINGLE bookkeeping thread.
    // void advance_epoch_dedicated();

    // The following bookkeeping methods are for a SINGLE bookkeeping thread:

    // atomically set the current global epoch number
    void set_epoch(uint64_t c);

    // stuff to do at the beginning of epoch c
    virtual void on_epoch_begin(uint64_t c);

    // stuff to do at the end of epoch c
    virtual void on_epoch_end(uint64_t c);

    ////////////////////////////////////
    // prologue and epilogue for nbtc //
    ////////////////////////////////////
    void prologue(){
        // to be called at the beginning of tx_begin
        cleanups[tid].ui.clear();
        undos[tid].ui.clear();
        unlocks[tid].ui.clear();
        allocs[tid].ui.clear();
    }

    // to be called at the end of tx_end
    virtual void commit_epilogue(); 

    // to be called at tx_end if aborted technically
    // single-operational execution doesn't need it so in abort_op we
    // don't call it
    // tx_abort shouldn't call it either, as it handles epoch stuff
    // while tx_abort doesn't fetch epoch at all
    virtual void abort_epilogue(); 

    /////////////
    // Recover //
    /////////////
    
    // recover all PBlk decendants. return an iterator.
    virtual std::unordered_map<uint64_t, PBlk*>* recover(const int rec_thd = 2);

    ///////////////////////////////
    // Transactional Composition //
    ///////////////////////////////
    void tx_begin();
    virtual void tx_end(); // directly return tx_abort if need to abort, so that the remaining part in tx_end may only handle committed case
    virtual void tx_abort(); //give up txn that is still in preparation

    bool is_inside_txn(){
        return flags[tid].inside_txn;
    }
    bool is_rolling_CAS(){
        return flags[tid].start_rolling_CAS;
    }
    bool is_during_abort(){
        return flags[tid].is_during_abort;
    }
    void set_start_rolling_CAS(){
        flags[tid].start_rolling_CAS = true;
    }
    void reset_start_rolling_CAS(){
        flags[tid].start_rolling_CAS = false;
    }
    void set_during_abort(){
        flags[tid].is_during_abort = true;
    }
    void reset_during_abort(){
        flags[tid].is_during_abort = false;
    }

    void tracker_start_op(){
        tracker.start_op(tid);
    }
    void tracker_end_op(){
        tracker.end_op(tid);
    }

    void reset_pending_reads(){
        pending_reads[tid].ui.clear();
    }
    template <class T>
    void addToPendingReads(atomic_lin_var<T>* _addr, lin_var var){
        assert (flags[tid].inside_txn);
        atomic_lin_var<uint64_t>* addr = reinterpret_cast<atomic_lin_var<uint64_t>*>(_addr);
        pending_reads[tid].ui[addr] = var;
    }

    void addToReadSet(atomic_lin_var<uint64_t>* _addr, uint64_t val){
        if (!flags[tid].inside_txn) return;
        atomic_lin_var<uint64_t>* addr = 
            reinterpret_cast<atomic_lin_var<uint64_t>*>(_addr);
        // It must be either in write set or in pending_reads.
        auto w_res = 
            local_descs[tid]->write_set->get(addr);
        if(w_res.has_value() && w_res->new_val==val) {
            return;
        }
        assert((pending_reads[tid].ui.count(addr)>0 && 
            pending_reads[tid].ui.find(addr)->second.val==val));

        lin_var val_cnt=pending_reads[tid].ui.find(addr)->second;

        if(!local_descs[tid]->add_to_read_set(addr, val_cnt))
            tx_abort(); // abort txn and throw abort exception
    }
    void removeFromReadSet(atomic_lin_var<uint64_t>* addr){
        assert(0&&"Abandoned routine!");
        if (!flags[tid].inside_txn) return;
        local_descs[tid]->remove_from_read_set(addr);
    }
    void addToCleanups(std::function<void()> lambda){
        cleanups[tid].ui.push_back(lambda);
    }
    void addToUndos(std::function<void()> lambda){
        if (!flags[tid].inside_txn) return;
        undos[tid].ui.push_back(lambda);
    }
    void addToUnlocks(std::function<void()> lambda){
        unlocks[tid].ui.push_back(lambda);
    }

    void* tmalloc(size_t sz){
        if (!flags[tid].inside_txn) return malloc(sz);
        void* ret = malloc(sz);
        allocs[tid].ui.emplace(ret, [](void* obj){ free(obj); });
        return ret;
    }
    template <typename T, typename... Types> 
    T* tnew(Types... args) {
        if constexpr(std::is_base_of<PBlk,T>::value){
            // XXX(wentao): very dirty hack for allocating payload via
            // pnew inside transaction
            if (!flags[tid].inside_txn) return pnew<T>(args...);
            T* ret = pnew<T> (args...);
            allocs[tid].ui.emplace(ret, [&](void* obj){ this->preclaim(reinterpret_cast<T*>(obj)); });
            return ret;
        } else {
            if (!flags[tid].inside_txn) return new T(args...);
            T* ret = new T (args...);
            allocs[tid].ui.emplace(ret, [](void* obj){ delete(reinterpret_cast<T*>(obj)); });
            return ret;
        }
    }

    void tfree(void* obj){
        if (!flags[tid].inside_txn) return free(obj);
        _tdelete(obj,[](void* o){ free (o); });
    }
    template <typename T> 
    void tdelete(T* obj) {
        if constexpr(std::is_base_of<PBlk,T>::value){
            // XXX(wentao): very dirty hack for deallocating payload via
            // preclaim inside transaction
            if (!flags[tid].inside_txn) return this->preclaim(obj);
            _tdelete(obj,[&](void* o){ this->preclaim (reinterpret_cast<T*>(o)); });
        } else {
            if (!flags[tid].inside_txn) return delete(obj);
            _tdelete(obj,[](void* o){ delete (reinterpret_cast<T*>(o)); });
        }
    }
    template <typename T>
    void tretire(T* obj) {
        if constexpr(std::is_base_of<PBlk,T>::value){
            if (!flags[tid].inside_txn) return tracker.retire(
                obj, 
                tid, 
                [&](void* o){
                    this->preclaim(reinterpret_cast<T*>(o));
                }
            );
            _tdelete(obj,[&](void* o){ 
                tracker.retire(
                    reinterpret_cast<T*>(o), 
                    tid, 
                    [&](void* o){
                        this->preclaim(reinterpret_cast<T*>(o));
                    }
                ); 
            });
        } else {
            if (!flags[tid].inside_txn) return tracker.retire(obj, tid);
            _tdelete(obj,[&](void* o){ tracker.retire(reinterpret_cast<T*>(o), tid); });
        }
    }
    // transient retire to be withdrawn at tracker.abort_op
    template <typename T>
    void txn_tretire(T* obj) {
        if constexpr(std::is_base_of<PBlk,T>::value){
            assert (flags[tid].inside_txn);
            _tdelete(obj,[&](void* o){ 
                tracker.temp_retire(
                    reinterpret_cast<T*>(o), 
                    tid,
                    [&](void* o){
                        this->preclaim(reinterpret_cast<T*>(o));
                    }
                ); 
            });
        } else {
            assert (flags[tid].inside_txn);
            _tdelete(obj,[&](void* o){ tracker.temp_retire(reinterpret_cast<T*>(o), tid); });
        }
    }
private:
    template <typename T> 
    void _tdelete(T* obj, std::function<void(void*)> dealloc_func){
        auto iter = allocs[tid].ui.find(reinterpret_cast<void*>(obj));
        if(iter != allocs[tid].ui.end()){
            iter->second(reinterpret_cast<void*>(obj));//delete
            allocs[tid].ui.erase(iter);
        } else {
            dealloc_func(reinterpret_cast<void*>(obj));
        }
    }

    void _tdelete(void* obj, std::function<void(void*)> dealloc_func){
        auto iter = allocs[tid].ui.find(obj);
        if(iter != allocs[tid].ui.end()){
            iter->second(obj);//delete
            allocs[tid].ui.erase(iter);
        } else {
            dealloc_func(obj);
        }
    }

};

class nbEpochSys : public EpochSys {
    // clean up thread-local to_be_freed buckets in begin_op starts in epoch c
    void local_free(uint64_t c);
    // clean up thread-local to_be_persisted buckets in begin_op
    // starts in epoch c; this must contain only reset payloads
    void local_persist(uint64_t c);
   public:
    virtual void reset() override;
    virtual void parse_env() override;
    virtual void begin_op() override;
    virtual void end_op() override;
    virtual uint64_t begin_reclaim_transaction() override;
    virtual void end_reclaim_transaction(uint64_t c) override;
    virtual void end_readonly_op() override;
    virtual void abort_op() override;
    virtual void on_epoch_begin(uint64_t c) override;
    virtual void on_epoch_end(uint64_t c) override;
    virtual std::unordered_map<uint64_t, PBlk*>* recover(const int rec_thd = 2) override;
    /*{assert(0&&"not implemented yet"); return {};}*/

    virtual void register_alloc_pblk(PBlk* b, uint64_t c) override;
   // for nonblocking persistence, prepare to retire a PBlk during a transaction.
    virtual void prepare_retire_pblk(PBlk* b, const uint64_t& c, std::vector<std::pair<PBlk*,PBlk*>>& pending_retires) override;
    virtual void prepare_retire_pblk(std::pair<PBlk*,PBlk*>& pending_retire, const uint64_t& c) override;
    virtual void withdraw_retire_pblk(PBlk* b, uint64_t c) override;

    // for nonblocking persistence, retire a PBlk during a transaction.
    virtual void retire_pblk(PBlk* b, uint64_t c, PBlk* anti=nullptr) override;

    // nbtc
    virtual void commit_epilogue() override; 
    virtual void abort_epilogue() override; 
    virtual void tx_end() override;
    virtual void tx_abort() override;

    nbEpochSys(GlobalTestConfig* _gtc) : EpochSys(_gtc){
#ifdef VISIBLE_READ
        // ensure nbEpochSys is used only when VISIBLE_READ is not
        assert(0&&"nbEpochSys is incompatible with VISIBLE_READ!");
#endif
    for(int i=0;i<gtc->task_num;i++){
            // Wentao: although in blocking Montage, descriptors don't
            // need to be persistent at all, we still allocate them in
            // NVM for simplicity. By some experiments, we confirmed
            // that allocating them in DRAM or NVM doesn't affect the
            // performance.
            delete(local_descs[i]);
            local_descs[i] = new_pblk<sc_desc_t>(i);
            assert(local_descs[i]!=nullptr);
            persist_func::clwb_range_nofence(local_descs[i],sizeof(sc_desc_t));
        }
    };
};

template<typename T>
T* EpochSys::reset_alloc_pblk(T* b, uint64_t c){
    ASSERT_DERIVE(T, PBlk);
    ASSERT_COPY(T);
    PBlk* blk = b;
    blk->epoch = NULL_EPOCH;
    assert(blk->blktype == ALLOC); 
    blk->blktype = INIT;
    // bufferedly persist the first cache line of b
    to_be_persisted->register_persist_raw(blk, c);
    PBlk* data = blk->get_data();
    if (data){
        reset_alloc_pblk(data,c);
    }
    return b;
}

template<typename T>
PBlkArray<T>* EpochSys::alloc_pblk_array(size_t s, uint64_t c){
    PBlkArray<T>* ret = static_cast<PBlkArray<T>*>(
        _ral->allocate(sizeof(PBlkArray<T>) + s*sizeof(T)));
    new (ret) PBlkArray<T>();
    // Wentao: content initialization has been moved into PBlkArray constructor
    ret->size = s;
    T* p = ret->content;
    for (int i = 0; i < s; i++){
        new (p) T();
        p++;
    }
    register_alloc_pblk(ret);
    // temporarily removed the following persist:
    // we have to persist it after modifications anyways.
    // to_be_persisted->register_persist(ret, _ral->malloc_size(ret), c);
    return ret;
}

template<typename T>
PBlkArray<T>* EpochSys::alloc_pblk_array(PBlk* owner, size_t s, uint64_t c){
    PBlkArray<T>* ret = static_cast<PBlkArray<T>*>(
        _ral->allocate(sizeof(PBlkArray<T>) + s*sizeof(T)));
    new (ret) PBlkArray<T>(owner);
    ret->size = s;
    T* p = ret->content;
    for (size_t i = 0; i < s; i++){
        new (p) T();
        p++;
    }
    register_alloc_pblk(ret);
    // temporarily removed the following persist:
    // we have to persist it after modifications anyways.
    // to_be_persisted->register_persist(ret, c);
    return ret;
}

template<typename T>
PBlkArray<T>* EpochSys::copy_pblk_array(const PBlkArray<T>* oth, uint64_t c){
    PBlkArray<T>* ret = static_cast<PBlkArray<T>*>(
        _ral->allocate(sizeof(PBlkArray<T>) + oth->size*sizeof(T)));
    new (ret) PBlkArray<T>(*oth);
    memcpy(ret->content, oth->content, oth->size*sizeof(T));
    ret->epoch = c;
    to_be_persisted->register_persist(ret, c);
    return ret;
}

template<typename T>
void EpochSys::free_pblk(T* b, uint64_t c){
    ASSERT_DERIVE(T, PBlk);
    ASSERT_COPY(T);
    
    PBlk* blk = b;
    uint64_t e = blk->epoch;
    PBlkType blktype = blk->blktype;
    // we can safely deallocate b directly if it hasn't been tagged
    // with any epoch, and return
    if(e==NULL_EPOCH){
        delete_pblk(b, c);
        return;
    }
    if (e > c){
        throw OldSeeNewException();
    } else if (e == c){
        if (blktype == ALLOC){
            delete_pblk(b, c);
            return;
        } else if (blktype == UPDATE){
            blk->blktype = DELETE;
        } else if (blktype == DELETE) {
            errexit("double free error.");
        }
    } else {
        // NOTE: The deletion node will be essentially "leaked" during online phase,
        // which may fundamentally confuse valgrind.
        // Consider keeping reference of all delete node for debugging purposes.
        PBlk* del = new_pblk<PBlk>(*blk);
        del->blktype = DELETE;
        del->epoch = c;
        // to_be_persisted[c%4].push(del);
        to_be_persisted->register_persist(del, c);
        // to_be_freed[(c+1)%4].push(del);
        to_be_freed->register_free(del, c+1);
    }
    // to_be_freed[c%4].push(b);
    to_be_freed->register_free(b, c);
}

template<typename T>
void EpochSys::reclaim_pblk(T* b, uint64_t c){
    ASSERT_DERIVE(T, PBlk);
    ASSERT_COPY(T);
    if(c==NULL_EPOCH){
        errexit("reclaiming a block in NULL epoch");
    }
    PBlk* blk = b;
    uint64_t e = blk->epoch;
    if (e > c){
        errexit("reclaiming a block created in a newer epoch");
    }
    if (blk->retire == nullptr){
        if (blk->blktype != DELETE){
            // errexit("reclaiming an unretired PBlk.");
            // this PBlk is not retired. we PDELETE it here.
            free_pblk(b, c);
        } else if (e < c-1){ // this block was retired at least two epochs ago.
            delete_pblk(b, c);
        } else {// this block was retired less than two epochs ago.
            // NOTE: putting b in c's to-be-free is safe, but not e's,
            // since if e==c-1, global epoch may go to c+1 and c-1's to-be-freed list
            // may be considered cleaned before we putting b in it.
            to_be_freed->register_free(blk, c);
        }
    } else {
        uint64_t e_retire = blk->retire->epoch;
        if (e_retire > c){
            errexit("reclaiming a block retired in a newer epoch");
        }
        if (e < c-1){
            // this block was retired at least two epochs ago.
            // Note that reclamation of retire node need to be deferred after a fence.
            to_be_freed->register_free(blk->retire, c);
            delete_pblk(b, c);
        } else {
            // retired in recent epoch, act like a free_pblk.
            to_be_freed->register_free(blk->retire, c+1);
            to_be_freed->register_free(b, c);
        }
    }
}

template<typename T>
const T* EpochSys::openread_pblk(const T* b){
    assert(epochs[tid].ui != NULL_EPOCH);

    ASSERT_DERIVE(T, PBlk);
    ASSERT_COPY(T);

    validate_access(b, epochs[tid].ui);
    return openread_pblk_unsafe(b);
}

template<typename T>
const T* EpochSys::openread_pblk_unsafe(const T* b){
    ASSERT_DERIVE(T, PBlk);
    ASSERT_COPY(T);

    return b;
}

template<typename T>
T* EpochSys::openwrite_pblk(T* b){
    assert(epochs[tid].ui != NULL_EPOCH);
    ASSERT_DERIVE(T, PBlk);
    ASSERT_COPY(T);
    
    validate_access(b, epochs[tid].ui);
    PBlk* blk = b;
    if (blk->epoch < epochs[tid].ui){
        // to_be_freed[epochs[tid].ui%4].push(b);
        to_be_freed->register_free(b, epochs[tid].ui);
        b = new_pblk<T>(*b);
        PBlk* blk = b;
        assert(blk);
        blk->epoch = epochs[tid].ui;
        blk->blktype = UPDATE;
    }
    // cannot put b in to-be-persisted list here (only) before the actual modification,
    // because help() may grab and flush it before the modification. This is currently
    // done by the API module.
    return b;
}

}

#endif