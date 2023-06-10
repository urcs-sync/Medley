#ifndef TPCC_HELPERS_HPP
#define TPCC_HELPERS_HPP
#include <random>
#include <cctype>
#include <set>
#include <vector>
#include <string>

#include "TPCCMacro.hpp"

namespace tpcc{
    constexpr int g_uniform_item_dist = 0;
    constexpr int g_disable_xpartition_txn = 0;
    constexpr int g_new_order_fast_id_gen = 0;
    constexpr int g_new_order_remote_item_pct = 1;

    constexpr size_t NumItems = 100000;
    constexpr size_t NumDistrictsPerWarehouse = 10;
    constexpr size_t NumCustomersPerDistrict = 3000;
    constexpr size_t NMaxCustomerIdxScanElems = 512;

    size_t NumWarehouses = 1; // Number of tables 

    // not thread-safe
    //
    // taken from java:
    //   http://developer.classpath.org/doc/java/util/Random-source.html
    class fast_random {
    public:
        fast_random(unsigned long seed)
            : seed(0)
        {
            set_seed0(seed);
        }

        inline unsigned long
        next()
        {
            return ((unsigned long) next(32) << 32) + next(32);
        }

        inline uint32_t
        next_u32()
        {
            return next(32);
        }

        inline uint16_t
        next_u16()
        {
            return next(16);
        }

        /** [0.0, 1.0) */
        inline double
        next_uniform()
        {
            return (((unsigned long) next(26) << 27) + next(27)) / (double) (1L << 53);
        }

        inline char
        next_char()
        {
            return next(8) % 256;
        }

        inline char
        next_readable_char()
        {
            static const char readables[] = "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
            return readables[next(6)];
        }

        inline std::string
        next_string(size_t len)
        {
            std::string s(len, 0);
            for (size_t i = 0; i < len; i++)
            s[i] = next_char();
            return s;
        }

        inline std::string
        next_readable_string(size_t len)
        {
            std::string s(len, 0);
            for (size_t i = 0; i < len; i++)
            s[i] = next_readable_char();
            return s;
        }

        inline unsigned long
        get_seed()
        {
            return seed;
        }

        inline void
        set_seed(unsigned long seed)
        {
            this->seed = seed;
        }

    private:
        inline void
        set_seed0(unsigned long seed)
        {
            this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
        }

        inline unsigned long
        next(unsigned int bits)
        {
            seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
            return (unsigned long) (seed >> (48 - bits));
        }

        unsigned long seed;
    };

    static inline uint32_t
    GetCurrentTimeMillis()
    {
        //struct timeval tv;
        //ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
        //return tv.tv_sec * 1000;

        // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
        // for now, we just give each core an increasing number

        static __thread uint32_t tl_hack = 0;
        return tl_hack++;
    }

    static inline int
    CheckBetweenInclusive(int v, int lower, int upper)
    {
        assert(v >= lower);
        assert(v <= upper);
        return v;
    }

    static inline int
    RandomNumber(fast_random& r, int min, int max)
    {
        return CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
    }

    static inline int
    NonUniformRandom(fast_random& r, int A, int C, int min, int max)
    {
        return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
    }

    static inline int
    GetItemId(fast_random& r)
    {
        return CheckBetweenInclusive(
            g_uniform_item_dist ?
            RandomNumber(r, 1, NumItems) :
            NonUniformRandom(r, 8191, 7911, 1, NumItems),
            1, NumItems);
    }

    static inline int
    GetCustomerId(fast_random& r)
    {
        return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 1, NumCustomersPerDistrict), 1, NumCustomersPerDistrict);
    }

    // pick a number between [start, end)
    static inline unsigned
    PickWarehouseId(fast_random& r, unsigned start, unsigned end)
    {
        assert(start < end);
        const unsigned diff = end - start;
        if (diff == 1)
            return start;
        return (r.next() % diff) + start;
    }

    static string NameTokens[] = 
    {
        string("BAR"),
        string("OUGHT"),
        string("ABLE"),
        string("PRI"),
        string("PRES"),
        string("ESE"),
        string("ANTI"),
        string("CALLY"),
        string("ATION"),
        string("EING"),
    };

    // all tokens are at most 5 chars long
    static constexpr size_t CustomerLastNameMaxSize = 5 * 3;

    static inline size_t
    GetCustomerLastName(uint8_t *buf, int num)
    {
        const std::string &s0 = NameTokens[num / 100];
        const std::string &s1 = NameTokens[(num / 10) % 10];
        const std::string &s2 = NameTokens[num % 10];
        uint8_t *const begin = buf;
        const size_t s0_sz = s0.size();
        const size_t s1_sz = s1.size();
        const size_t s2_sz = s2.size();
        memcpy(buf, s0.data(), s0_sz); buf += s0_sz;
        memcpy(buf, s1.data(), s1_sz); buf += s1_sz;
        memcpy(buf, s2.data(), s2_sz); buf += s2_sz;
        return buf - begin;
    }

    static inline size_t
    GetCustomerLastName(char *buf, int num)
    {
        return GetCustomerLastName((uint8_t *) buf, num);
    }

    static inline std::string
    GetCustomerLastName(int num)
    {
        std::string ret;
        ret.resize(CustomerLastNameMaxSize);
        ret.resize(GetCustomerLastName((uint8_t *) &ret[0], num));
        return ret;
    }

    static inline std::string
    GetNonUniformCustomerLastNameLoad(fast_random& r)
    {
        return GetCustomerLastName(NonUniformRandom(r, 255, 157, 0, 999));
    }

    static inline size_t
    GetNonUniformCustomerLastNameRun(fast_random& r, uint8_t *buf)
    {
        return GetCustomerLastName(buf, NonUniformRandom(r, 255, 223, 0, 999));
    }

    static inline size_t
    GetNonUniformCustomerLastNameRun(fast_random& r, char *buf)
    {
        return GetNonUniformCustomerLastNameRun(r, (uint8_t *) buf);
    }

    static inline std::string
    GetNonUniformCustomerLastNameRun(fast_random& r)
    {
        return GetCustomerLastName(NonUniformRandom(r, 255, 223, 0, 999));
    }

    static inline std::string
    RandomStr(fast_random& r, uint len)
    {
        // this is a property of the oltpbench implementation...
        if (!len)
        return "";

        uint i = 0;
        std::string buf(len - 1, 0);
        while (i < (len - 1)) {
            const char c = (char) r.next() % 256;
            // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
            // is a less restrictive filter than isalnum()
            if (!std::isalnum(c))
                continue;
            buf[i++] = c;
        }
        return buf;
    }

    // RandomNStr() actually produces a string of length len
    static inline string
    RandomNStr(fast_random& r, uint len)
    {
        const char base = '0';
        string buf(len, 0);
        for (uint i = 0; i < len; i++)
            buf[i] = (char)(base + (r.next() % 10));
        return buf;
    }

    static inline void
    SanityCheckCustomer(const customer::key *k, const customer::value *v)
    {
        assert(k->c_w_id >= 1 && static_cast<size_t>(k->c_w_id) <= NumWarehouses);
        assert(k->c_d_id >= 1 && static_cast<size_t>(k->c_d_id) <= NumDistrictsPerWarehouse);
        assert(k->c_id >= 1 && static_cast<size_t>(k->c_id) <= NumCustomersPerDistrict);
        assert(v->c_credit == "BC" || v->c_credit == "GC");
        assert(v->c_middle == "OE");
    }

    static inline void
    SanityCheckWarehouse(const warehouse::key *k, const warehouse::value *v)
    {
        assert(k->w_id >= 1 && static_cast<size_t>(k->w_id) <= NumWarehouses);
        assert(v->w_state.size() == 2);
        assert(v->w_zip == "123456789");
    }

    static inline void
    SanityCheckDistrict(const district::key *k, const district::value *v)
    {
        assert(k->d_w_id >= 1 && static_cast<size_t>(k->d_w_id) <= NumWarehouses);
        assert(k->d_id >= 1 && static_cast<size_t>(k->d_id) <= NumDistrictsPerWarehouse);
        assert(v->d_next_o_id >= 3001);
        assert(v->d_state.size() == 2);
        assert(v->d_zip == "123456789");
    }

    static inline void
    SanityCheckItem(const item::key *k, const item::value *v)
    {
        assert(k->i_id >= 1 && static_cast<size_t>(k->i_id) <= NumItems);
        assert(v->i_price >= 1.0 && v->i_price <= 100.0);
    }

    static inline void
    SanityCheckStock(const stock::key *k, const stock::value *v)
    {
        assert(k->s_w_id >= 1 && static_cast<size_t>(k->s_w_id) <= NumWarehouses);
        assert(k->s_i_id >= 1 && static_cast<size_t>(k->s_i_id) <= NumItems);
    }

    static inline void
    SanityCheckNewOrder(const new_order::key *k, const new_order::value *v)
    {
        assert(k->no_w_id >= 1 && static_cast<size_t>(k->no_w_id) <= NumWarehouses);
        assert(k->no_d_id >= 1 && static_cast<size_t>(k->no_d_id) <= NumDistrictsPerWarehouse);
    }

    static inline void
    SanityCheckOOrder(const oorder::key *k, const oorder::value *v)
    {
        assert(k->o_w_id >= 1 && static_cast<size_t>(k->o_w_id) <= NumWarehouses);
        assert(k->o_d_id >= 1 && static_cast<size_t>(k->o_d_id) <= NumDistrictsPerWarehouse);
        assert(v->o_c_id >= 1 && static_cast<size_t>(v->o_c_id) <= NumCustomersPerDistrict);
        assert(v->o_carrier_id >= 0 && static_cast<size_t>(v->o_carrier_id) <= NumDistrictsPerWarehouse);
        assert(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
    }

    static inline void
    SanityCheckOrderLine(const order_line::key *k, const order_line::value *v)
    {
        assert(k->ol_w_id >= 1 && static_cast<size_t>(k->ol_w_id) <= NumWarehouses);
        assert(k->ol_d_id >= 1 && static_cast<size_t>(k->ol_d_id) <= NumDistrictsPerWarehouse);
        assert(k->ol_number >= 1 && k->ol_number <= 15);
        assert(v->ol_i_id >= 1 && static_cast<size_t>(v->ol_i_id) <= NumItems);
    }

} // namespace tpcc

#endif