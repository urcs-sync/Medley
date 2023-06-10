#ifndef TPCC_MACRO_HPP
#define TPCC_MACRO_HPP

#include <stdint.h>
#include <functional>
#include <vector>
#include <string>
#include <iomanip>

#include "tpcc/inline_str.hpp"
#include "RMap.hpp"

namespace tpcc {
template <typename T>
  struct typeutil { typedef const T & func_param_type; };
}


#ifndef PMAP_TYPE
  #define PMAP_TYPE txMontageLfHashTable
#endif

// XXX(Wentao): 
// The customer_id_idx tables don't have to be persistent; actually,
// they are just indexings that hold subset of info of customer tables
// and can be reconstructed after recovery.
// Since we use vector* as customer_id_idx::value, making it
// persistent doesn't make sense either.
// 
// See below FIXME for more details about vector* as value.
#ifndef TMAP_TYPE
  #define TMAP_TYPE MedleyLfHashTable
#endif

#define APPLY_X_AND_Y(x, y) x(y, y)

#define STRUCT_PARAM_FIRST_X(tpe, name) \
  typename typeutil< tpe >::func_param_type name

#define STRUCT_PARAM_REST_X(tpe, name) \
  , typename typeutil< tpe >::func_param_type name

#define STRUCT_INITLIST_FIRST_X(tpe, name) \
  name(name)

#define STRUCT_INITLIST_REST_X(tpe, name) \
  , name(name)

#define STRUCT_EQ_X(tpe, name) \
  if (this->name != other.name) \
    return false;

#define STRUCT_LESS_X(tpe, name) \
  if (this->name < other.name) \
    return true; \
  else if (this->name != other.name) \
    return false;

#define STRUCT_FIELDPOS_X(tpe, name) \
  name ## _field,

#define STRUCT_LAYOUT_X(tpe, name) \
  tpe name;

#define DO_STRUCT(name, keyfields, valuefields) \
  namespace tpcc { \
    struct name { \
      struct key { \
        inline key() {} \
        inline key(keyfields(STRUCT_PARAM_FIRST_X, STRUCT_PARAM_REST_X)) : \
          keyfields(STRUCT_INITLIST_FIRST_X, STRUCT_INITLIST_REST_X) {} \
        APPLY_X_AND_Y(keyfields, STRUCT_LAYOUT_X) \
        inline bool \
        operator==(const struct key &other) const \
        { \
          APPLY_X_AND_Y(keyfields, STRUCT_EQ_X) \
          return true; \
        } \
        inline bool \
        operator!=(const struct key &other) const \
        { \
          return !operator==(other); \
        } \
        inline bool \
        operator<(const struct key &other) const \
        { \
          APPLY_X_AND_Y(keyfields, STRUCT_LESS_X) \
          return false; \
        } \
        inline bool \
        operator>=(const struct key &other) const \
        { \
          return !operator<(other); \
        } \
        inline bool \
        operator>(const struct key &other) const \
        { \
          return other.operator<(*this); \
        } \
        inline bool \
        operator<=(const struct key &other) const \
        { \
          return !operator>(other); \
        } \
        enum { \
          APPLY_X_AND_Y(keyfields, STRUCT_FIELDPOS_X) \
          NFIELDS \
        }; \
      }; \
      struct value { \
        inline value() {} \
        inline value(valuefields(STRUCT_PARAM_FIRST_X, STRUCT_PARAM_REST_X)) : \
          valuefields(STRUCT_INITLIST_FIRST_X, STRUCT_INITLIST_REST_X) {} \
        APPLY_X_AND_Y(valuefields, STRUCT_LAYOUT_X) \
        inline bool \
        operator==(const struct value &other) const \
        { \
          APPLY_X_AND_Y(valuefields, STRUCT_EQ_X) \
          return true; \
        } \
        inline bool \
        operator!=(const struct value &other) const \
        { \
          return !operator==(other); \
        } \
        enum { \
          APPLY_X_AND_Y(valuefields, STRUCT_FIELDPOS_X) \
          NFIELDS \
        }; \
      }; \
    }; \
  } \
  namespace std { \
    template <> \
    struct hash<typename ::tpcc::name::key> { \
      hash<string_view> hash_fn; \
      size_t operator()(const typename ::tpcc::name::key &_k) const \
      { \
        const char* k = reinterpret_cast<const char*>(&_k); \
        string_view str(k, sizeof(::tpcc::name::key)); \
        return hash_fn(str); \
      } \
    }; \
  }

#define CUSTOMER_KEY_FIELDS(x, y) \
  x(int32_t,c_w_id) \
  y(int32_t,c_d_id) \
  y(int32_t,c_id)
#define CUSTOMER_VALUE_FIELDS(x, y) \
  x(float,c_discount) \
  y(inline_str_fixed<2>,c_credit) \
  y(inline_str_8<16>,c_last) \
  y(inline_str_8<16>,c_first) \
  y(float,c_credit_lim) \
  y(float,c_balance) \
  y(float,c_ytd_payment) \
  y(int32_t,c_payment_cnt) \
  y(int32_t,c_delivery_cnt) \
  y(inline_str_8<20>,c_street_1) \
  y(inline_str_8<20>,c_street_2) \
  y(inline_str_8<20>,c_city) \
  y(inline_str_fixed<2>,c_state) \
  y(inline_str_fixed<9>,c_zip) \
  y(inline_str_fixed<16>,c_phone) \
  y(uint32_t,c_since) \
  y(inline_str_fixed<2>,c_middle) \
  y(inline_str_16<500>,c_data)
DO_STRUCT(customer, CUSTOMER_KEY_FIELDS, CUSTOMER_VALUE_FIELDS)

#define CUSTOMER_ID_IDX_KEY_FIELDS(x, y) \
  x(int32_t,c_id) 
#define CUSTOMER_ID_IDX_VALUE_FIELDS(x, y) \
  x(inline_str_fixed<16>,c_first) 
DO_STRUCT(customer_id_idx, CUSTOMER_ID_IDX_KEY_FIELDS, CUSTOMER_ID_IDX_VALUE_FIELDS)

// FIXME(Wentao): super dirty hack
//
// We use std::vector here to hold customer id and first name. It's
// safe because in TPC-C customer_name_idx is mutable only during
// prefill, and is read-only (traversed) during test. 
// Namely, no customer will be added or removed after prefill.
//
// Change ::std::vector to a transactional, traversable container if
// later the workload needs to add or remove customers. 
// By traversable, we mean it has size() and a random access
// operator[] that returns i-th item.

#define CUSTOMER_ID_IDX_MAP_TYPE ::std::vector<::std::pair<customer_id_idx::key,customer_id_idx::value>>

#define CUSTOMER_NAME_IDX_KEY_FIELDS(x, y) \
  x(int32_t,c_w_id) \
  y(int32_t,c_d_id) \
  y(inline_str_fixed<16>,c_last) 
#define CUSTOMER_NAME_IDX_VALUE_FIELDS(x, y) \
  x(CUSTOMER_ID_IDX_MAP_TYPE*,c_id_idx)
DO_STRUCT(customer_name_idx, CUSTOMER_NAME_IDX_KEY_FIELDS, CUSTOMER_NAME_IDX_VALUE_FIELDS)

#define DISTRICT_KEY_FIELDS(x, y) \
  x(int32_t,d_w_id) \
  y(int32_t,d_id)
#define DISTRICT_VALUE_FIELDS(x, y) \
  x(float,d_ytd) \
  y(float,d_tax) \
  y(int32_t,d_next_o_id) \
  y(inline_str_8<10>,d_name) \
  y(inline_str_8<20>,d_street_1) \
  y(inline_str_8<20>,d_street_2) \
  y(inline_str_8<20>,d_city) \
  y(inline_str_fixed<2>,d_state) \
  y(inline_str_fixed<9>,d_zip)
DO_STRUCT(district, DISTRICT_KEY_FIELDS, DISTRICT_VALUE_FIELDS)

#define HISTORY_KEY_FIELDS(x, y) \
  x(int32_t,h_c_id) \
  y(int32_t,h_c_d_id) \
  y(int32_t,h_c_w_id) \
  y(int32_t,h_d_id) \
  y(int32_t,h_w_id) \
  y(uint32_t,h_date)
#define HISTORY_VALUE_FIELDS(x, y) \
  x(float,h_amount) \
  y(inline_str_8<24>,h_data)
DO_STRUCT(history, HISTORY_KEY_FIELDS, HISTORY_VALUE_FIELDS)

#define ITEM_KEY_FIELDS(x, y) \
  x(int32_t,i_id)
#define ITEM_VALUE_FIELDS(x, y) \
  x(inline_str_8<24>,i_name) \
  y(float,i_price) \
  y(inline_str_8<50>,i_data) \
  y(int32_t,i_im_id)
DO_STRUCT(item, ITEM_KEY_FIELDS, ITEM_VALUE_FIELDS)

#define NEW_ORDER_KEY_FIELDS(x, y) \
  x(int32_t,no_w_id) \
  y(int32_t,no_d_id) \
  y(int32_t,no_o_id)
// need dummy b/c our btree cannot have empty values.
// we also size value so that it can fit a key
#define NEW_ORDER_VALUE_FIELDS(x, y) \
  x(inline_str_fixed<12>,no_dummy)
DO_STRUCT(new_order, NEW_ORDER_KEY_FIELDS, NEW_ORDER_VALUE_FIELDS)

#define OORDER_KEY_FIELDS(x, y) \
  x(int32_t,o_w_id) \
  y(int32_t,o_d_id) \
  y(int32_t,o_id)
#define OORDER_VALUE_FIELDS(x, y) \
  x(int32_t,o_c_id) \
  y(int32_t,o_carrier_id) \
  y(int8_t,o_ol_cnt) \
  y(bool,o_all_local) \
  y(uint32_t,o_entry_d)
DO_STRUCT(oorder, OORDER_KEY_FIELDS, OORDER_VALUE_FIELDS)

#define OORDER_C_ID_IDX_KEY_FIELDS(x, y) \
  x(int32_t,o_w_id) \
  y(int32_t,o_d_id) \
  y(int32_t,o_c_id) \
  y(int32_t,o_o_id)
#define OORDER_C_ID_IDX_VALUE_FIELDS(x, y) \
	x(uint8_t,o_dummy)
DO_STRUCT(oorder_c_id_idx, OORDER_C_ID_IDX_KEY_FIELDS, OORDER_C_ID_IDX_VALUE_FIELDS)

#define ORDER_LINE_KEY_FIELDS(x, y) \
  x(int32_t,ol_w_id) \
  y(int32_t,ol_d_id) \
  y(int32_t,ol_o_id) \
  y(int32_t,ol_number)
#define ORDER_LINE_VALUE_FIELDS(x, y) \
  x(int32_t,ol_i_id) \
  y(uint32_t,ol_delivery_d) \
  y(float,ol_amount) \
  y(int32_t,ol_supply_w_id) \
  y(int8_t,ol_quantity)
DO_STRUCT(order_line, ORDER_LINE_KEY_FIELDS, ORDER_LINE_VALUE_FIELDS)

#define STOCK_KEY_FIELDS(x, y) \
  x(int32_t,s_w_id) \
  y(int32_t,s_i_id)
#define STOCK_VALUE_FIELDS(x, y) \
  x(int16_t,s_quantity) \
  y(float,s_ytd) \
  y(int32_t,s_order_cnt) \
  y(int32_t,s_remote_cnt)
DO_STRUCT(stock, STOCK_KEY_FIELDS, STOCK_VALUE_FIELDS)

#define STOCK_DATA_KEY_FIELDS(x, y) \
  x(int32_t,s_w_id) \
  y(int32_t,s_i_id)
#define STOCK_DATA_VALUE_FIELDS(x, y) \
  x(inline_str_8<50>,s_data) \
  y(inline_str_fixed<24>,s_dist_01) \
  y(inline_str_fixed<24>,s_dist_02) \
  y(inline_str_fixed<24>,s_dist_03) \
  y(inline_str_fixed<24>,s_dist_04) \
  y(inline_str_fixed<24>,s_dist_05) \
  y(inline_str_fixed<24>,s_dist_06) \
  y(inline_str_fixed<24>,s_dist_07) \
  y(inline_str_fixed<24>,s_dist_08) \
  y(inline_str_fixed<24>,s_dist_09) \
  y(inline_str_fixed<24>,s_dist_10)
DO_STRUCT(stock_data, STOCK_DATA_KEY_FIELDS, STOCK_DATA_VALUE_FIELDS)

#define WAREHOUSE_KEY_FIELDS(x, y) \
  x(int32_t,w_id)
#define WAREHOUSE_VALUE_FIELDS(x, y) \
  x(float,w_ytd) \
  y(float,w_tax) \
  y(inline_str_8<10>,w_name) \
  y(inline_str_8<20>,w_street_1) \
  y(inline_str_8<20>,w_street_2) \
  y(inline_str_8<20>,w_city) \
  y(inline_str_fixed<2>,w_state) \
  y(inline_str_fixed<9>,w_zip)
DO_STRUCT(warehouse, WAREHOUSE_KEY_FIELDS, WAREHOUSE_VALUE_FIELDS)

#define TPCC_TABLE_LIST(x) \
  x(customer, PMAP_TYPE) \
  x(customer_name_idx, TMAP_TYPE) \
  x(district, PMAP_TYPE) \
  x(history, PMAP_TYPE) \
  x(item, PMAP_TYPE) \
  x(new_order, PMAP_TYPE) \
  x(oorder, PMAP_TYPE) \
  x(oorder_c_id_idx, PMAP_TYPE) \
  x(order_line, PMAP_TYPE) \
  x(stock, PMAP_TYPE) \
  x(stock_data, PMAP_TYPE) \
  x(warehouse, PMAP_TYPE)

#define TPCC_TABLE_DECLARE(name, type) \
  ::std::vector<RMap<name::key, name::value>*> tbl_ ## name;

#define TPCC_TABLE_INIT(name, type) \
  for (uint i=0; i<NumWarehouses; i++) { \
    tbl_ ## name.push_back(new type<name::key, name::value>(gtc)); \
    gtc->allocatedRideables.push_back(tbl_ ## name.back()); \
  } 

#define TPCC_TABLE_CLEANUP(name, type) \
  for (uint i=0; i<NumWarehouses; i++) { \
    delete tbl_ ## name[i]; \
  } \
  tbl_ ## name.clear();

#define TPCC_TABLE_PRINT_SIZE(name, type) \
  ::std::cout << "size of " << ::std::left << ::std::setfill(' ') \
  << ::std::setw(18) << #name \
  << " key:" << ::std::setw(4) << sizeof(name::key) \
  << " value:" << ::std::setw(4) << sizeof(name::value) \
  << ::std::endl;


#if 0
// This is an example expansion of customer struct, for easier
// reference.  
struct customer { 
    struct key { 
        inline key() {} 
        inline key(
          typename typeutil< int32_t >::func_param_type c_w_id, 
          typename typeutil< int32_t >::func_param_type c_d_id, 
          typename typeutil< int32_t >::func_param_type c_id
          ) : c_w_id(c_w_id), c_d_id(c_d_id), c_id(c_id) {}

        int32_t c_w_id;
        int32_t c_d_id;
        int32_t c_id;

        inline bool operator==(const struct key &other) const { 
            if (this->c_w_id != other.c_w_id) return false;
            if (this->c_d_id != other.c_d_id) return false;
            if (this->c_id != other.c_id) return false;
            return true; 
        } 

        inline bool operator!=(const struct key &other) const { 
            return !operator==(other); 
        } 

        inline bool operator<(const struct key &other) const { 
            if (this->c_w_id < other.c_w_id) return true;
            else if (this->c_w_id != other.c_w_id) return false;
            if (this->c_d_id < other.c_d_id) return true;
            else if (this->c_d_id != other.c_d_id) return false;
            if (this->c_id < other.c_id) return true;
            else if (this->c_id != other.c_id) return false;
            return false; 
        } 
        
        inline bool operator>=(const struct key &other) const { 
            return !operator<(other); 
        } 

        enum { 
            c_w_id_field,
            c_d_id_field,
            c_id_field, 
            NFIELDS 
        }; 
    }; 
    
    struct value { 
        inline value() {} 
        inline value(
          typename typeutil< float >::func_param_type c_discount,
          typename typeutil< inline_str_fixed<2> >::func_param_type c_credit,
          typename typeutil< inline_str_8<16> >::func_param_type c_last,
          typename typeutil< inline_str_8<16> >::func_param_type c_first,
          typename typeutil< float >::func_param_type c_credit_lim,
          typename typeutil< float >::func_param_type c_balance,
          typename typeutil< float >::func_param_type c_ytd_payment,
          typename typeutil< int32_t >::func_param_type c_payment_cnt,
          typename typeutil< int32_t >::func_param_type c_delivery_cnt,
          typename typeutil< inline_str_8<20> >::func_param_type c_street_1,
          typename typeutil< inline_str_8<20> >::func_param_type c_street_2,
          typename typeutil< inline_str_8<20> >::func_param_type c_city,
          typename typeutil< inline_str_fixed<2> >::func_param_type c_state,
          typename typeutil< inline_str_fixed<9> >::func_param_type c_zip,
          typename typeutil< inline_str_fixed<16> >::func_param_type c_phone,
          typename typeutil< uint32_t >::func_param_type c_since,
          typename typeutil< inline_str_fixed<2> >::func_param_type c_middle,
          typename typeutil< inline_str_16<500> >::func_param_type c_data
        ) : 
        c_discount(c_discount), 
        c_credit(c_credit),
        c_last(c_last),
        c_first(c_first),
        c_street_1(c_street_1),
        c_street_2(c_street_2),
        c_city(c_city),
        c_state(c_state),
        c_zip(c_zip),
        c_phone(c_phone),
        c_middle(c_middle),
        c_data(c_data) {} 
        
        
        float c_discount;
        inline_str_fixed<2> c_credit;
        inline_str_8<16> c_last;
        inline_str_8<16> c_first;
        float c_credit_lim;
        float c_balance;
        float c_ytd_payment;
        int32_t c_payment_cnt;
        int32_t c_delivery_cnt;
        inline_str_8<20> c_street_1;
        inline_str_8<20> c_street_2;
        inline_str_8<20> c_city;
        inline_str_fixed<2> c_state;
        inline_str_fixed<9> c_zip;
        inline_str_fixed<16> c_phone;
        uint32_t c_since;
        inline_str_fixed<2> c_middle;
        inline_str_16<500> c_dat;

 
        inline bool operator==(const struct value &other) const { 
            if (this->c_discount != other.c_discount) return false; 
            if (this->c_credit != other.c_credit) return false; 
            if (this->c_last != other.c_last) return false; 
            if (this->c_first != other.c_first) return false; 
            if (this->c_credit_lim != other.c_credit_lim) return false; 
            if (this->c_balance != other.c_balance) return false; 
            if (this->c_ytd_payment != other.c_ytd_payment) return false; 
            if (this->c_payment_cnt != other.c_payment_cnt) return false; 
            if (this->c_delivery_cnt != other.c_delivery_cnt) return false; 
            if (this->c_street_1 != other.c_street_1) return false; 
            if (this->c_street_2 != other.c_street_2) return false; 
            if (this->c_city != other.c_city) return false; 
            if (this->c_state != other.c_state) return false; 
            if (this->c_zip != other.c_zip) return false; 
            if (this->c_phone != other.c_phone) return false; 
            if (this->c_since != other.c_since) return false; 
            if (this->c_middle != other.c_middle) return false; 
            if (this->c_data != other.c_data) return false;
            return true; 
        } 
        inline bool operator!=(const struct value &other) const { 
            return !operator==(other); 
        } 
        
        enum { 
            c_discount_field, 
            c_credit_field, 
            c_last_field, 
            c_first_field, 
            c_credit_lim_field, 
            c_balance_field, 
            c_ytd_payment_field, 
            c_payment_cnt_field, 
            c_delivery_cnt_field, 
            c_street_1_field, 
            c_street_2_field, 
            c_city_field, 
            c_state_field, 
            c_zip_field, 
            c_phone_field, 
            c_since_field, 
            c_middle_field, 
            c_data_field,
            NFIELDS 
        }; 
    }; 
};

#endif //ifdef 0

#endif