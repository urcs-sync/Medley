#ifndef TPCC_INLINE_STR_HPP
#define TPCC_INLINE_STR_HPP

#include <stdint.h>
#include <string.h>

#include <string>
#include <ostream>

namespace tpcc{
template <typename IntSizeType, unsigned int N>
class inline_str_base {
public:

  inline_str_base() : sz(0) {}

  inline_str_base(const char *s)
  {
    assign(s);
  }

  inline_str_base(const char *s, size_t n)
  {
    assign(s, n);
  }

  inline_str_base(const std::string &s)
  {
    assign(s);
  }

  inline_str_base(const inline_str_base &that)
    : sz(that.sz)
  {
    memcpy(&buf[0], &that.buf[0], sz);
  }

  inline_str_base &
  operator=(const inline_str_base &that)
  {
    if (this == &that)
      return *this;
    sz = that.sz;
    memcpy(&buf[0], &that.buf[0], sz);
    return *this;
  }

  inline size_t
  max_size() const
  {
    return N;
  }

  inline const char *
  c_str() const
  {
    buf[sz] = 0;
    return &buf[0];
  }

  inline std::string
  str(bool zeropad = false) const
  {
    if (zeropad) {
      assert(N >= sz);
      std::string r(N, 0);
      memcpy((char *) r.data(), &buf[0], sz);
      return r;
    } else {
      return std::string(&buf[0], sz);
    }
  }

  inline const char *
  data() const
  {
    return &buf[0];
  }

  inline size_t
  size() const
  {
    return sz;
  }

  inline void
  assign(const char *s)
  {
    assign(s, strlen(s));
  }

  inline void
  assign(const char *s, size_t n)
  {
    assert(n <= N);
    memcpy(&buf[0], s, n);
    sz = n;
  }

  inline void
  assign(const std::string &s)
  {
    assign(s.data(), s.size());
  }

  inline void
  resize(size_t n, char c = 0)
  {
    assert(n <= N);
    if (n > sz)
      memset(&buf[sz], c, n - sz);
    sz = n;
  }

  inline void
  resize_junk(size_t n)
  {
    assert(n <= N);
    sz = n;
  }

  inline bool
  operator==(const inline_str_base &other) const
  {
    return memcmp(buf, other.buf, sz) == 0;
  }

  inline bool
  operator!=(const inline_str_base &other) const
  {
    return !operator==(other);
  }

  inline bool
  operator<(const inline_str_base &other) const
  {
    return memcmp(buf, other.buf, sz) < 0;
  }

  inline bool
  operator>=(const inline_str_base &other) const
  {
    return !operator<(other);
  }

private:
  IntSizeType sz;
  mutable char buf[N + 1];
};

template <typename IntSizeType, unsigned int N>
inline std::ostream &
operator<<(std::ostream &o, const inline_str_base<IntSizeType, N> &s)
{
  o << std::string(s.data(), s.size());
  return o;
}

template <unsigned int N>
class inline_str_8 : public inline_str_base<uint8_t, N> {
  typedef inline_str_base<uint8_t, N> super_type;
public:
  inline_str_8() : super_type() {}
  inline_str_8(const char *s) : super_type(s) {}
  inline_str_8(const char *s, size_t n) : super_type(s, n) {}
  inline_str_8(const std::string &s) : super_type(s) {}
};

template <unsigned int N>
class inline_str_16 : public inline_str_base<uint16_t, N> {
  typedef inline_str_base<uint16_t, N> super_type;
public:
  inline_str_16() : super_type() {}
  inline_str_16(const char *s) : super_type(s) {}
  inline_str_16(const char *s, size_t n) : super_type(s, n) {}
  inline_str_16(const std::string &s) : super_type(s) {}
};

// equiavlent to CHAR(N)
template <unsigned int N, char FillChar = ' '>
class inline_str_fixed {
public:
  inline_str_fixed()
  {
    memset(&buf[0], FillChar, N);
  }

  inline_str_fixed(const char *s)
  {
    assign(s, strlen(s));
  }

  inline_str_fixed(const char *s, size_t n)
  {
    assign(s, n);
  }

  inline_str_fixed(const std::string &s)
  {
    assign(s.data(), s.size());
  }

  inline_str_fixed(const inline_str_fixed &that)
  {
    memcpy(&buf[0], &that.buf[0], N);
  }

  inline_str_fixed &
  operator=(const inline_str_fixed &that)
  {
    if (this == &that)
      return *this;
    memcpy(&buf[0], &that.buf[0], N);
    return *this;
  }

  inline std::string
  str() const
  {
    return std::string(&buf[0], N);
  }

  inline const char *
  data() const
  {
    return &buf[0];
  }

  inline size_t
  size() const
  {
    return N;
  }

  inline void
  assign(const char *s)
  {
    assign(s, strlen(s));
  }

  inline void
  assign(const char *s, size_t n)
  {
    assert(n <= N);
    memcpy(&buf[0], s, n);
    if ((N - n) > 0) // to suppress compiler warning
      memset(&buf[n], FillChar, N - n); // pad with spaces
  }

  inline void
  assign(const std::string &s)
  {
    assign(s.data(), s.size());
  }

  inline bool
  operator==(const inline_str_fixed &other) const
  {
    return memcmp(buf, other.buf, N) == 0;
  }

  inline bool
  operator!=(const inline_str_fixed &other) const
  {
    return !operator==(other);
  }

  inline bool
  operator<(const inline_str_fixed &other) const
  {
    return memcmp(buf, other.buf, N) < 0;
  }

  inline bool
  operator>=(const inline_str_fixed &other) const
  {
    return !operator<(other);
  }

private:
  char buf[N];
};

template <unsigned int N, char FillChar>
inline std::ostream &
operator<<(std::ostream &o, const inline_str_fixed<N, FillChar> &s)
{
  o << std::string(s.data(), s.size());
  return o;
}
}; // namespace tpcc
#endif /* TPCC_INLINE_STR_HPP */
