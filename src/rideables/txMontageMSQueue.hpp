#ifndef MONTAGE_MS_QUEUE
#define MONTAGE_MS_QUEUE

#include <iostream>
#include <atomic>
#include <algorithm>
#include "HarnessUtils.hpp"
#include "ConcurrentPrimitives.hpp"
#include "RQueue.hpp"
// #include "RCUTracker.hpp"
#include "CustomTypes.hpp"
#include "Recoverable.hpp"

template<typename T>
class txMontageMSQueue : public RQueue<T>, public Recoverable{
public:
    class Payload : public pds::PBlk{
        GENERATE_FIELD(T, val, Payload);
        GENERATE_FIELD(uint64_t, sn, Payload); 
    public:
        Payload(): pds::PBlk(){}
        Payload(T v): pds::PBlk(), m_val(v), m_sn(0){}
        Payload(const Payload& oth): pds::PBlk(oth), m_sn(0), m_val(oth.m_val){}
        void persist(){}
    };

private:
    struct Node{
        txMontageMSQueue* ds = nullptr;
        pds::atomic_lin_var<Node*> next;
        Payload* payload;

        Node(): next(nullptr), payload(nullptr){}
        Node(txMontageMSQueue* ds_): ds(ds_), next(nullptr), payload(nullptr){}
        Node(txMontageMSQueue* ds_, T v): ds(ds_), next(nullptr), payload(ds_->pnew<Payload>(v)){
            // assert(ds->epochs[EpochSys::tid].ui == NULL_EPOCH);
        }

        void set_sn(uint64_t s){
            assert(payload!=nullptr && "payload shouldn't be null");
            payload->set_unsafe_sn(ds,s);
        }
        ~Node(){
            if (payload){
                ds->preclaim(payload);
            }
        }
    };

public:
    std::atomic<uint64_t> global_sn;

private:
    // dequeue pops node from head
    pds::atomic_lin_var<Node*> head;
    // enqueue pushes node to tail
    std::atomic<Node*> tail;
    // RCUTracker tracker;

public:
    txMontageMSQueue(GlobalTestConfig* gtc): 
        Recoverable(gtc), global_sn(0), head(nullptr), tail(nullptr)
        // , tracker(gtc->task_num, 100, 1000, true)
    {

        Node* dummy = new Node(this);
        head.store(this,dummy);
        tail.store(dummy);
    }

    void init_thread(GlobalTestConfig* gtc, LocalTestConfig* ltc){
        Recoverable::init_thread(gtc, ltc);
    }

    int recover(bool simulated){
        errexit("recover of txMontageMSQueue not implemented.");
        return 0;
    }

    ~txMontageMSQueue(){};

    void enqueue(T val, int tid);
    optional<T> dequeue(int tid);
};

template<typename T>
void txMontageMSQueue<T>::enqueue(T v, int tid){
    TX_OP_SEPARATOR();

    Node* new_node = tnew<Node>(this,v);
    Node* cur_tail = nullptr;
    // tracker.start_op(tid);
    while(true){
        // Node* cur_head = head.load();
        cur_tail = tail.load();
        uint64_t s = global_sn.fetch_add(1);
        bool is_speculative = false;
        // if `next` is a speculative read, we won't help swing the
        // tail below
        Node* next = cur_tail->next.nbtc_load(this, is_speculative);
        if(cur_tail == tail.load()){
            if(next == nullptr) {
                // directly set m_sn and BEGIN_OP will flush it
                new_node->set_sn(s);
                // begin_op();
                /* set_sn must happen before PDELETE of payload since it's 
                 * before linearization point.
                 * Also, this must set sn in place since we still remain in
                 * the same epoch.
                 */
                // new_node->set_sn(s);
                if((cur_tail->next).nbtc_CAS(this, next, new_node, true, true)){
                    // end_op();
                    break;
                }
                // abort_op();
            } else {
                // help swing only if `next` isn't speculative
                if(!is_speculative)
                    tail.compare_exchange_strong(cur_tail, next); // try to swing tail to next node
            }
        }
    }
    auto cleanup = [=]()mutable{
        this->tail.compare_exchange_strong(cur_tail, new_node); // try to swing tail to inserted node
    };
    if (is_inside_txn()) {
        addToCleanups(cleanup);
    } else {
        cleanup();//execute cleanup in place
    }

    // tracker.end_op(tid);
}

template<typename T>
optional<T> txMontageMSQueue<T>::dequeue(int tid){
    TX_OP_SEPARATOR();

    optional<T> res = {};
    // tracker.start_op(tid);
    while(true){
        Node* cur_head = head.nbtc_load(this);
        Node* cur_tail = tail.load();
        bool is_speculative = false;
        Node* next = cur_head->next.nbtc_load(this, is_speculative);

        if(cur_head == head.nbtc_load(this)){
            if(cur_head == cur_tail){
                // queue is empty
                if(next == nullptr) {
                    addToReadSet(&(cur_head->next),next);
                    res.reset();
                    break;
                }
                if (!is_speculative)
                    tail.compare_exchange_strong(cur_tail, next); // tail is falling behind; try to update
            } else {
                // begin_op();
                Payload* payload = next->payload;// get payload for PDELETE
                if (!is_inside_txn()) pretire(payload); // semantically we are tentatively removing next from queue
                if(head.nbtc_CAS(this, cur_head, next, true, true)){
                    res = (T)payload->get_unsafe_val(this);// old see new is impossible
                    // end_op();
                    auto cleanup = [=]()mutable{
                        cur_head->payload = payload; // let payload have same lifetime as dummy node
                        this->tretire(cur_head);
                    };
                    if (is_inside_txn()){
                        pretire(payload);
                        addToCleanups(cleanup);
                    } else {
                        cleanup();//execute cleanup in place
                    }
                    break;
                }
                // abort_op();
            }
        }
    }
    // tracker.end_op(tid);
    return res;
}

template <class T> 
class txMontageMSQueueFactory : public RideableFactory{
    Rideable* build(GlobalTestConfig* gtc){
        return new txMontageMSQueue<T>(gtc);
    }
};

/* Specialization for strings */
#include <string>
#include "InPlaceString.hpp"
template <>
class txMontageMSQueue<std::string>::Payload : public pds::PBlk{
    GENERATE_FIELD(pds::InPlaceString<TESTS_VAL_SIZE>, val, Payload);
    GENERATE_FIELD(uint64_t, sn, Payload); 

public:
    Payload(std::string v) : m_val(this, v), m_sn(0){}
    Payload(const Payload& oth) : pds::PBlk(oth), m_val(this, oth.m_val), m_sn(oth.m_sn){}
    void persist(){}
};

#endif