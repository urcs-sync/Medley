#ifndef TPCC_LOADER_HPP
#define TPCC_LOADER_HPP

#include <random>
#include <cctype>
#include <set>
#include <vector>
#include <string>

#include "RMap.hpp"
#include "EpochSys.hpp"
#include "TPCCMacro.hpp"
#include "TestConfig.hpp"
#include "helpers.hpp"

namespace tpcc{
class tpcc_loader{
public:
    tpcc_loader(unsigned long seed,
                GlobalTestConfig* _gtc)
    : r(seed), gtc(_gtc){
    };

    void start(){
        thd = std::move(std::thread(&tpcc_loader::run, this));
    }

    void join(){
        thd.join();
    }

    static void reset_thread_info(){
        thd_cnt.store(0);
    }

    virtual ~tpcc_loader(){}

protected:
    virtual void load() = 0;
    static std::atomic<int> thd_cnt;
    void run(){
        tid = thd_cnt.fetch_add(1, std::memory_order_relaxed);
        pds::EpochSys::init_thread(tid);
        this->load();
    }

    std::thread thd;
    size_t tid;
    fast_random r;
    GlobalTestConfig* gtc;
};

std::atomic<int> tpcc_loader::thd_cnt = 0;

class tpcc_warehouse_loader : public tpcc_loader {
public:
    tpcc_warehouse_loader(unsigned long seed,
                          GlobalTestConfig* _gtc,
                          std::vector<RMap<warehouse::key, warehouse::value>*>& tables)
    : tpcc_loader(seed, _gtc), 
    tbl_warehouse(tables)
    {}

protected:
    virtual void
    load()
    {
        uint64_t warehouse_total_sz = 0, n_warehouses = 0;
        std::vector<warehouse::value> warehouses;
        for (uint i = 1; i <= NumWarehouses; i++) {
            const warehouse::key k(i);

            const std::string w_name = RandomStr(r, RandomNumber(r, 6, 10));
            const std::string w_street_1 = RandomStr(r, RandomNumber(r, 10, 20));
            const std::string w_street_2 = RandomStr(r, RandomNumber(r, 10, 20));
            const std::string w_city = RandomStr(r, RandomNumber(r, 10, 20));
            const std::string w_state = RandomStr(r, 3);
            const std::string w_zip = "123456789";

            warehouse::value v;
            v.w_ytd = 300000;
            v.w_tax = (float) RandomNumber(r, 0, 2000) / 10000.0;
            v.w_name.assign(w_name);
            v.w_street_1.assign(w_street_1);
            v.w_street_2.assign(w_street_2);
            v.w_city.assign(w_city);
            v.w_state.assign(w_state);
            v.w_zip.assign(w_zip);

            SanityCheckWarehouse(&k, &v);
            const size_t sz = sizeof(v);
            warehouse_total_sz += sz;
            n_warehouses++;
            tbl_warehouse[i-1]->insert(k, v, tid);

            warehouses.push_back(v);
        }
        for (uint i = 1; i <= NumWarehouses; i++) {
            const warehouse::key k(i);
            auto v = tbl_warehouse[i-1]->get(k, tid);
            assert(v.has_value());
            assert(warehouses[i - 1] == *v);

            SanityCheckWarehouse(&k, &(*v));
        }
        if (gtc->verbose) {
            std::cerr << "[INFO] finished loading warehouse" << std::endl;
            std::cerr << "[INFO]   * average warehouse record length: "
                << (double(warehouse_total_sz)/double(n_warehouses)) << " bytes" << std::endl;
        }
    }
private:
    std::vector<RMap<warehouse::key, warehouse::value>*>& tbl_warehouse;
};

class tpcc_item_loader : public tpcc_loader {
public:
  tpcc_item_loader(unsigned long seed,
                   GlobalTestConfig* _gtc,
                   std::vector<RMap<item::key, item::value>*>& tables)
    : tpcc_loader(seed, _gtc),
      tbl_item(tables)
    {}

protected:
    virtual void
    load()
    {
        uint64_t total_sz = 0;
        for (uint i = 1; i <= NumItems; i++) {
            // items don't "belong" to a certain warehouse, so no pinning
            const item::key k(i);

            item::value v;
            const std::string i_name = RandomStr(r, RandomNumber(r, 14, 24));
            v.i_name.assign(i_name);
            v.i_price = (float) RandomNumber(r, 100, 10000) / 100.0;
            const int len = RandomNumber(r, 26, 50);
            if (RandomNumber(r, 1, 100) > 10) {
                const std::string i_data = RandomStr(r, len);
                v.i_data.assign(i_data);
            } else {
                const int startOriginal = RandomNumber(r, 2, (len - 8));
                const std::string i_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 7);
                v.i_data.assign(i_data);
            }
            v.i_im_id = RandomNumber(r, 1, 10000);

            SanityCheckItem(&k, &v);
            const size_t sz = sizeof(v);
            total_sz += sz;
            tbl_item[0]->insert(k, v, tid); 
        }
        if (gtc->verbose) {
            std::cerr << "[INFO] finished loading item" << std::endl;
            std::cerr << "[INFO]   * average item record length: "
                << (double(total_sz)/double(NumItems)) << " bytes" << std::endl;
        }
    }

private:
    std::vector<RMap<item::key, item::value>*>& tbl_item;
};

class tpcc_stock_loader : public tpcc_loader {
public:
    tpcc_stock_loader(unsigned long seed,
                      GlobalTestConfig* _gtc,
                      std::vector<RMap<stock::key, stock::value>*>& stock_tables,
                      std::vector<RMap<stock_data::key, stock_data::value>*>& stock_data_tables,
                      ssize_t warehouse_id)
        : tpcc_loader(seed, _gtc),
        tbl_stock(stock_tables),
        tbl_stock_data(stock_data_tables),
        warehouse_id(warehouse_id)
    {
        assert(warehouse_id == -1 ||
                   (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses));
    }


protected:
    virtual void
    load()
    {
        uint64_t stock_total_sz = 0, n_stocks = 0;
        const uint w_start = (warehouse_id == -1) ?
            1 : static_cast<uint>(warehouse_id);
        const uint w_end   = (warehouse_id == -1) ?
            NumWarehouses : static_cast<uint>(warehouse_id);

        for (uint w = w_start; w <= w_end; w++) {
            const size_t batchsize = NumItems;
            const size_t nbatches = 1;

            for (uint b = 0; b < nbatches;) {
                const size_t iend = std::min((b + 1) * batchsize + 1, NumItems);
                for (uint i = (b * batchsize + 1); i <= iend; i++) {
                    const stock::key k(w, i);
                    const stock_data::key k_data(w, i);

                    stock::value v;
                    v.s_quantity = RandomNumber(r, 10, 100);
                    v.s_ytd = 0;
                    v.s_order_cnt = 0;
                    v.s_remote_cnt = 0;

                    stock_data::value v_data;
                    const int len = RandomNumber(r, 26, 50);
                    if (RandomNumber(r, 1, 100) > 10) {
                        const std::string s_data = RandomStr(r, len);
                        v_data.s_data.assign(s_data);
                    } else {
                        const int startOriginal = RandomNumber(r, 2, (len - 8));
                        const std::string s_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 7);
                        v_data.s_data.assign(s_data);
                    }
                    v_data.s_dist_01.assign(RandomStr(r, 24));
                    v_data.s_dist_02.assign(RandomStr(r, 24));
                    v_data.s_dist_03.assign(RandomStr(r, 24));
                    v_data.s_dist_04.assign(RandomStr(r, 24));
                    v_data.s_dist_05.assign(RandomStr(r, 24));
                    v_data.s_dist_06.assign(RandomStr(r, 24));
                    v_data.s_dist_07.assign(RandomStr(r, 24));
                    v_data.s_dist_08.assign(RandomStr(r, 24));
                    v_data.s_dist_09.assign(RandomStr(r, 24));
                    v_data.s_dist_10.assign(RandomStr(r, 24));

                    SanityCheckStock(&k, &v);
                    const size_t sz = sizeof(v);
                    stock_total_sz += sz;
                    n_stocks++;
                    tbl_stock[w-1]->insert(k, v, tid);
                    tbl_stock_data[w-1]->insert(k_data, v_data, tid);
                }
                b++;
            }
        }

        if (gtc->verbose) {
            if (warehouse_id == -1) {
                std::cerr << "[INFO] finished loading stock" << std::endl;
                std::cerr << "[INFO]   * average stock record length: "
                    << (double(stock_total_sz)/double(n_stocks)) << " bytes" << std::endl;
            } else {
                std::cerr << "[INFO] finished loading stock (w=" << warehouse_id << ")" << std::endl;
            }
        }
    }

private:
    std::vector<RMap<stock::key, stock::value>*>& tbl_stock;
    std::vector<RMap<stock_data::key, stock_data::value>*>& tbl_stock_data;
    ssize_t warehouse_id;
};

class tpcc_district_loader : public tpcc_loader {
public:
    tpcc_district_loader(unsigned long seed,
                         GlobalTestConfig* _gtc,
                         std::vector<RMap<district::key, district::value>*>& tables)
        : tpcc_loader(seed, _gtc),
        tbl_district(tables)
    {}

protected:
    virtual void
    load()
    {
        uint64_t district_total_sz = 0, n_districts = 0;
        uint cnt = 0;
        for (uint w = 1; w <= NumWarehouses; w++) {
            for (uint d = 1; d <= NumDistrictsPerWarehouse; d++, cnt++) {
                const district::key k(w, d);

                district::value v;
                v.d_ytd = 30000;
                v.d_tax = (float) (RandomNumber(r, 0, 2000) / 10000.0);
                v.d_next_o_id = 3001;
                v.d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
                v.d_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                v.d_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                v.d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                v.d_state.assign(RandomStr(r, 3));
                v.d_zip.assign("123456789");

                SanityCheckDistrict(&k, &v);
                const size_t sz = sizeof(v);
                district_total_sz += sz;
                n_districts++;
                tbl_district[w-1]->insert(k, v, tid);
            }
        }

        if (gtc->verbose) {
            std::cerr << "[INFO] finished loading district" << std::endl;
            std::cerr << "[INFO]   * average district record length: "
                << (double(district_total_sz)/double(n_districts)) << " bytes" << std::endl;
        }
    }

private:
    std::vector<RMap<district::key, district::value>*>& tbl_district;
};

class tpcc_customer_loader : public tpcc_loader {
public:
    tpcc_customer_loader(unsigned long seed,
                         GlobalTestConfig* _gtc,
                         std::vector<RMap<customer::key, customer::value>*>& customer_tables,
                         std::vector<RMap<customer_name_idx::key, customer_name_idx::value>*>& customer_name_idx_tables,
                         std::vector<RMap<history::key, history::value>*>& history_tables,
                         ssize_t warehouse_id)
        : tpcc_loader(seed, _gtc),
        tbl_customer(customer_tables),
        tbl_customer_name_idx(customer_name_idx_tables),
        tbl_history(history_tables),
        warehouse_id(warehouse_id)
    {
        assert(warehouse_id == -1 ||
                    (warehouse_id >= 1 &&
                    static_cast<size_t>(warehouse_id) <= NumWarehouses));
    }

protected:
    virtual void
    load()
    {
        const uint w_start = (warehouse_id == -1) ?
            1 : static_cast<uint>(warehouse_id);
        const uint w_end   = (warehouse_id == -1) ?
            NumWarehouses : static_cast<uint>(warehouse_id);
        const size_t batchsize = NumCustomersPerDistrict;
        const size_t nbatches = 1;
        // std::cerr << "num batches: " << nbatches << std::endl;

        uint64_t total_sz = 0;

        for (uint w = w_start; w <= w_end; w++) {
            for (uint d = 1; d <= NumDistrictsPerWarehouse; d++) {
                for (uint batch = 0; batch < nbatches;) {
                    const size_t cstart = batch * batchsize;
                    const size_t cend = std::min((batch + 1) * batchsize, NumCustomersPerDistrict);
                    for (uint cidx0 = cstart; cidx0 < cend; cidx0++) {
                        const uint c = cidx0 + 1;
                        const customer::key k(w, d, c);

                        customer::value v;
                        v.c_discount = (float) (RandomNumber(r, 1, 5000) / 10000.0);
                        if (RandomNumber(r, 1, 100) <= 10)
                            v.c_credit.assign("BC");
                        else
                            v.c_credit.assign("GC");

                        if (c <= 1000)
                            v.c_last.assign(GetCustomerLastName(c - 1));
                        else
                            v.c_last.assign(GetNonUniformCustomerLastNameLoad(r));

                        v.c_first.assign(RandomStr(r, RandomNumber(r, 8, 16)));
                        v.c_credit_lim = 50000;

                        v.c_balance = -10;
                        v.c_ytd_payment = 10;
                        v.c_payment_cnt = 1;
                        v.c_delivery_cnt = 0;

                        v.c_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                        v.c_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                        v.c_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                        v.c_state.assign(RandomStr(r, 3));
                        v.c_zip.assign(RandomNStr(r, 4) + "11111");
                        v.c_phone.assign(RandomNStr(r, 16));
                        v.c_since = GetCurrentTimeMillis();
                        v.c_middle.assign("OE");
                        v.c_data.assign(RandomStr(r, RandomNumber(r, 300, 500)));

                        SanityCheckCustomer(&k, &v);
                        const size_t sz = sizeof(v);
                        total_sz += sz;
                        tbl_customer[w-1]->insert(k, v, tid);

                        // customer name index
                        const customer_name_idx::key k_idx(k.c_w_id, k.c_d_id, v.c_last.str(true));
                        CUSTOMER_ID_IDX_MAP_TYPE *v_idx_temp = nullptr;
                        auto curr_v_idx = tbl_customer_name_idx[w-1]->get(k_idx, tid);
                        if(curr_v_idx.has_value()) {
                            v_idx_temp = curr_v_idx->c_id_idx;
                        } else {
                            v_idx_temp = new CUSTOMER_ID_IDX_MAP_TYPE ();
                            const customer_name_idx::value v_idx(v_idx_temp);
                            tbl_customer_name_idx[w-1]->insert(k_idx, v_idx, tid);
                        }
                        // index structure is:
                        // (c_w_id, c_d_id, c_last) -> (vector of (c_id -> c_first))
                        const customer_id_idx::key k_id_idx(k.c_id);
                        const customer_id_idx::value v_id_idx(v.c_first.str(true));
                        v_idx_temp->emplace_back(k_id_idx, v_id_idx);

                        history::key k_hist;
                        k_hist.h_c_id = c;
                        k_hist.h_c_d_id = d;
                        k_hist.h_c_w_id = w;
                        k_hist.h_d_id = d;
                        k_hist.h_w_id = w;
                        k_hist.h_date = GetCurrentTimeMillis();

                        history::value v_hist;
                        v_hist.h_amount = 10;
                        v_hist.h_data.assign(RandomStr(r, RandomNumber(r, 10, 24)));

                        tbl_history[w-1]->insert(k_hist, v_hist, tid);
                    }
                    batch++;
                }
            }
        }

        if (gtc->verbose) {
            if (warehouse_id == -1) {
                std::cerr << "[INFO] finished loading customer" << std::endl;
                std::cerr << "[INFO]   * average customer record length: "
                    << (double(total_sz)/double(NumWarehouses*NumDistrictsPerWarehouse*NumCustomersPerDistrict))
                    << " bytes " << std::endl;
            } else {
                std::cerr << "[INFO] finished loading customer (w=" << warehouse_id << ")" << std::endl;
            }
        }
    }

private:
    std::vector<RMap<customer::key, customer::value>*>& tbl_customer;
    std::vector<RMap<customer_name_idx::key, customer_name_idx::value>*>& tbl_customer_name_idx;
    std::vector<RMap<history::key, history::value>*>& tbl_history;
    ssize_t warehouse_id;
};

class tpcc_order_loader : public tpcc_loader {
public:
    tpcc_order_loader(unsigned long seed,
                      GlobalTestConfig* _gtc,
                      std::vector<RMap<oorder::key, oorder::value>*>& oorder_tables,
                      std::vector<RMap<oorder_c_id_idx::key, oorder_c_id_idx::value>*>& oorder_c_id_idx_tables,
                      std::vector<RMap<new_order::key, new_order::value>*>& new_order_tables,
                      std::vector<RMap<order_line::key, order_line::value>*>& order_line_tables,
                      ssize_t warehouse_id)
        : tpcc_loader(seed, _gtc),
        tbl_oorder(oorder_tables),
        tbl_oorder_c_id_idx(oorder_c_id_idx_tables),
        tbl_new_order(new_order_tables),
        tbl_order_line(order_line_tables),
        warehouse_id(warehouse_id)
    {
        assert(warehouse_id == -1 ||
                    (warehouse_id >= 1 &&
                    static_cast<size_t>(warehouse_id) <= NumWarehouses));
    }

protected:
    virtual void
    load()
    {
        uint64_t order_line_total_sz = 0, n_order_lines = 0;
        uint64_t oorder_total_sz = 0, n_oorders = 0;
        uint64_t new_order_total_sz = 0, n_new_orders = 0;

        const uint w_start = (warehouse_id == -1) ?
            1 : static_cast<uint>(warehouse_id);
        const uint w_end   = (warehouse_id == -1) ?
            NumWarehouses : static_cast<uint>(warehouse_id);

        for (uint w = w_start; w <= w_end; w++) {
            for (uint d = 1; d <= NumDistrictsPerWarehouse; d++) {
                set<uint> c_ids_s;
                std::vector<uint> c_ids;
                while (c_ids.size() != NumCustomersPerDistrict) {
                    const auto x = (rand() % NumCustomersPerDistrict) + 1;
                    if (c_ids_s.count(x))
                        continue;
                    c_ids_s.insert(tid);
                    c_ids.emplace_back(x);
                }
                for (uint c = 1; c <= NumCustomersPerDistrict;) {
                    const oorder::key k_oo(w, d, c);

                    oorder::value v_oo;
                    v_oo.o_c_id = c_ids[c - 1];
                    if (k_oo.o_id < 2101)
                        v_oo.o_carrier_id = RandomNumber(r, 1, 10);
                    else
                        v_oo.o_carrier_id = 0;
                    v_oo.o_ol_cnt = RandomNumber(r, 5, 15);
                    v_oo.o_all_local = true;
                    v_oo.o_entry_d = GetCurrentTimeMillis();

                    SanityCheckOOrder(&k_oo, &v_oo);
                    const size_t sz = sizeof(v_oo);
                    oorder_total_sz += sz;
                    n_oorders++;
                    tbl_oorder[w-1]->insert(k_oo, v_oo, tid);

                    const oorder_c_id_idx::key k_oo_idx(k_oo.o_w_id, k_oo.o_d_id, v_oo.o_c_id, k_oo.o_id);
                    const oorder_c_id_idx::value v_oo_idx(0);

                    tbl_oorder_c_id_idx[w-1]->insert(k_oo_idx, v_oo_idx, tid);

                    if (c >= 2101) {
                        const new_order::key k_no(w, d, c);
                        const new_order::value v_no;

                        SanityCheckNewOrder(&k_no, &v_no);
                        const size_t sz = sizeof(v_no);
                        new_order_total_sz += sz;
                        n_new_orders++;
                        tbl_new_order[w-1]->insert(k_no, v_no, tid);
                    }

                    for (uint l = 1; l <= uint(v_oo.o_ol_cnt); l++) {
                        const order_line::key k_ol(w, d, c, l);

                        order_line::value v_ol;
                        v_ol.ol_i_id = RandomNumber(r, 1, 100000);
                        if (k_ol.ol_o_id < 2101) {
                            v_ol.ol_delivery_d = v_oo.o_entry_d;
                            v_ol.ol_amount = 0;
                        } else {
                            v_ol.ol_delivery_d = 0;
                            // random within [0.01 .. 9,999.99]
                            v_ol.ol_amount = (float) (RandomNumber(r, 1, 999999) / 100.0);
                        }

                        v_ol.ol_supply_w_id = k_ol.ol_w_id;
                        v_ol.ol_quantity = 5;
                        // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
                        //v_ol.ol_dist_info = RandomStr(r, 24);

                        SanityCheckOrderLine(&k_ol, &v_ol);
                        const size_t sz = sizeof(v_ol);
                        order_line_total_sz += sz;
                        n_order_lines++;
                        tbl_order_line[w-1]->insert(k_ol, v_ol, tid);
                    }
                    c++;
                }
            }
        }

        if (gtc->verbose) {
            if (warehouse_id == -1) {
                std::cerr << "[INFO] finished loading order" << std::endl;
                std::cerr << "[INFO]   * average order_line record length: "
                    << (double(order_line_total_sz)/double(n_order_lines)) << " bytes" << std::endl;
                std::cerr << "[INFO]   * average oorder record length: "
                    << (double(oorder_total_sz)/double(n_oorders)) << " bytes" << std::endl;
                std::cerr << "[INFO]   * average new_order record length: "
                    << (double(new_order_total_sz)/double(n_new_orders)) << " bytes" << std::endl;
            } else {
                std::cerr << "[INFO] finished loading order (w=" << warehouse_id << ")" << std::endl;
            }
        }
    }

private:
    std::vector<RMap<oorder::key, oorder::value>*>& tbl_oorder;
    std::vector<RMap<oorder_c_id_idx::key, oorder_c_id_idx::value>*>& tbl_oorder_c_id_idx;
    std::vector<RMap<new_order::key, new_order::value>*>& tbl_new_order;
    std::vector<RMap<order_line::key, order_line::value>*>& tbl_order_line;
    ssize_t warehouse_id;
};

}// namespace tpcc
#endif