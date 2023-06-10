#include "Recoverable.hpp"
#include "PersistFunc.hpp"
// std::atomic<size_t> pds::abort_cnt(0);
// std::atomic<size_t> pds::total_cnt(0);
Recoverable::Recoverable(GlobalTestConfig* gtc) : 
    _preallocated_esys(gtc->_preallocated_esys) {
    // init epoch system
    if(_preallocated_esys){
        // epoch system already allocated; assign to this recoverable
        _esys = reinterpret_cast<pds::EpochSys*>(gtc->_esys);
    } else {
        if(gtc->checkEnv("Liveness")){
            string env_liveness = gtc->getEnv("Liveness");
            if(env_liveness == "Nonblocking"){
                _esys = new pds::nbEpochSys(gtc);
            } else if (env_liveness == "Blocking"){
                _esys = new pds::EpochSys(gtc);
            } else {
                errexit("unrecognized 'Liveness' environment");
            }
        } else {
            gtc->setEnv("Liveness", "Blocking");
            _esys = new pds::EpochSys(gtc);
        }
    }
}
Recoverable::~Recoverable(){
    if(!_preallocated_esys)
        delete _esys;
    Persistent::finalize();
}
void Recoverable::init_thread(GlobalTestConfig*, LocalTestConfig* ltc){
    pds::EpochSys::init_thread(ltc->tid);
}

void Recoverable::init_thread(int tid){
    pds::EpochSys::init_thread(tid);
}
