#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#include <cstdint>
#include <malloc.h>
#include <atomic>
#include <cassert>
#include <sys/mman.h>
#include <errno.h>
#include <common/assert.h>

#include <iostream>

template<typename DataType>
class Allocator 
{
public:
    Allocator(uint64_t totalBytes, uint64_t threadCount, uint64_t typeSize)
    {
        // std::cout << totalBytes << "bytes requested" << std::endl;
        m_totalBytes = totalBytes;
        m_threadCount = threadCount;
        m_typeSize = typeSize;
        m_ticket = 0;
        // m_pool = (char*)memalign(m_typeSize, totalBytes);
        m_pool = (char*)mmap(0, totalBytes, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
        if(m_pool==nullptr){
            printf("map failed failed %d\n", errno);
            exit(1);
        }
        assert(m_pool && "Memory pool initialization failed.");
        // ASSERT(m_pool, "Memory pool initialization failed.");
    }

    ~Allocator()
    {
        // free(m_pool);
        munmap(m_pool, m_totalBytes);
    }

    //Every thread need to call init once before any allocation
    void Init()
    {
        uint64_t threadId = __sync_fetch_and_add(&m_ticket, 1);
        assert(threadId < m_threadCount && "ThreadId specified should be smaller than thread count.");

        m_base = m_pool + threadId * m_totalBytes / m_threadCount;
        m_freeIndex = 0;
    }

    void Uninit()
    { }

    DataType* Alloc()
    {
        assert(m_freeIndex < m_totalBytes / m_threadCount && "out of capacity.");
        char* ret = m_base + m_freeIndex;
        m_freeIndex += m_typeSize;

        return (DataType*)ret;
    }

private:
    char* m_pool;
    uint64_t m_totalBytes;      //number of elements T in the pool
    uint64_t m_threadCount;
    uint64_t m_ticket;
    uint64_t m_typeSize;

    static __thread char* m_base;
    static __thread uint64_t m_freeIndex;
};

template<typename T>
__thread char* Allocator<T>::m_base;

template<typename T>
__thread uint64_t Allocator<T>::m_freeIndex;

#endif /* end of include guard: ALLOCATOR_H */
