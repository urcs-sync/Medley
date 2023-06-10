#ifndef LFTT_SETADAPTOR_H
#define LFTT_SETADAPTOR_H

#include "translink/list/translist.h"
#include "translink/skiplist/lftttransskip.h"
#include "common/allocator.h"
#include "common/macros.h"
#include "ostm/skiplist/stmskip.h"
#include "obslink/list/obslist.h"
#include "obslink/skiplist/obsskip.h"

enum SetOpType
{
    FIND = 0,
    INSERT,
    DELETE
};

struct SetOperator
{
    uint8_t type;
    setkey_t key;
    setval_t val;
};

enum SetOpStatus
{
    LIVE = 0,
    COMMITTED,
    ABORTED
};

typedef std::vector<SetOperator> SetOpArray;

template<typename T>
class SetAdaptor
{
};

template<>
class SetAdaptor<TransList>
{
public:
    SetAdaptor(uint64_t cap, uint64_t threadCount, uint32_t transSize)
        : m_descAllocator(cap * threadCount * TransList::Desc::SizeOf(transSize), threadCount, TransList::Desc::SizeOf(transSize))
        , m_nodeAllocator(cap * threadCount *  sizeof(TransList::Node) * transSize, threadCount, sizeof(TransList::Node))
        , m_nodeDescAllocator(cap * threadCount *  sizeof(TransList::NodeDesc) * transSize, threadCount, sizeof(TransList::NodeDesc))
        , m_list(&m_nodeAllocator, &m_descAllocator, &m_nodeDescAllocator)
    { }

    void Init()
    {
        m_descAllocator.Init();
        m_nodeAllocator.Init();
        m_nodeDescAllocator.Init();
        m_list.ResetMetrics();
    }

    uint32_t Uninit(){
        return 0;
    }

    bool ExecuteOps(const SetOpArray& ops)
    {
        //TransList::Desc* desc = m_list.AllocateDesc(ops.size());
        TransList::Desc* desc = m_descAllocator.Alloc();
        desc->size = ops.size();
        desc->status = TransList::ACTIVE;

        for(uint32_t i = 0; i < ops.size(); ++i)
        {
            desc->ops[i].type = ops[i].type; 
            desc->ops[i].key = ops[i].key; 
        }

        return m_list.ExecuteOps(desc);
    }

private:
    Allocator<TransList::Desc> m_descAllocator;
    Allocator<TransList::Node> m_nodeAllocator;
    Allocator<TransList::NodeDesc> m_nodeDescAllocator;
    TransList m_list;
};

bool lftttransskip_execute_ops(trans_skip* l, Desc* desc);

template<>
class SetAdaptor<trans_skip>
{
public:
    SetAdaptor(uint64_t cap, uint64_t threadCount, uint32_t transSize)
        : m_descAllocator(cap * threadCount * Desc::SizeOf(transSize), threadCount, Desc::SizeOf(transSize))
        , m_nodeDescAllocator(cap * threadCount *  sizeof(NodeDesc) * transSize, threadCount, sizeof(NodeDesc))
    { 
        m_skiplist = transskip_alloc(&m_descAllocator, &m_nodeDescAllocator);
        init_transskip_subsystem(); 
    }

    ~SetAdaptor()
    {
        transskip_free(m_skiplist);
    }

    void Init()
    {
        m_descAllocator.Init();
        m_nodeDescAllocator.Init();
        ResetMetrics(m_skiplist);
    }

    uint32_t Uninit()
    {
        destroy_transskip_subsystem(); 
        return get_count_commit();
    }

    bool ExecuteOps(const SetOpArray& ops)
    {
        //TransList::Desc* desc = m_list.AllocateDesc(ops.size());
        Desc* desc = m_descAllocator.Alloc();
        desc->size = ops.size();
        desc->status = LIVE;

        for(uint32_t i = 0; i < ops.size(); ++i)
        {
            desc->ops[i].type = ops[i].type; 
            desc->ops[i].key = ops[i].key; 
        }

        return lftttransskip_execute_ops(m_skiplist, desc);
    }

private:
    Allocator<Desc> m_descAllocator;
    Allocator<NodeDesc> m_nodeDescAllocator;
    trans_skip* m_skiplist;
};

template<>
class SetAdaptor<ObsList>
{
public:
    SetAdaptor(uint64_t cap, uint64_t threadCount, uint32_t transSize)
        : m_descAllocator(cap * threadCount * ObsList::Desc::SizeOf(transSize), threadCount, ObsList::Desc::SizeOf(transSize))
        , m_nodeAllocator(cap * threadCount *  sizeof(ObsList::Node) * transSize, threadCount, sizeof(ObsList::Node))
        , m_nodeDescAllocator(cap * threadCount *  sizeof(ObsList::NodeDesc) * transSize, threadCount, sizeof(ObsList::NodeDesc))
        , m_list(&m_nodeAllocator, &m_descAllocator, &m_nodeDescAllocator)
    { }

    void Init()
    {
        m_descAllocator.Init();
        m_nodeAllocator.Init();
        m_nodeDescAllocator.Init();
        m_list.ResetMetrics();
    }

    uint32_t Uninit(){
        return 0;
    }

    bool ExecuteOps(const SetOpArray& ops)
    {
        //ObsList::Desc* desc = m_list.AllocateDesc(ops.size());
        ObsList::Desc* desc = m_descAllocator.Alloc();
        desc->size = ops.size();
        desc->status = ObsList::ACTIVE;

        for(uint32_t i = 0; i < ops.size(); ++i)
        {
            desc->ops[i].type = ops[i].type; 
            desc->ops[i].key = ops[i].key; 
        }

        return m_list.ExecuteOps(desc);
    }

private:
    Allocator<ObsList::Desc> m_descAllocator;
    Allocator<ObsList::Node> m_nodeAllocator;
    Allocator<ObsList::NodeDesc> m_nodeDescAllocator;
    ObsList m_list;
};

template<>
class SetAdaptor<obs_skip>
{
public:
    SetAdaptor(uint64_t cap, uint64_t threadCount, uint32_t transSize)
        : m_descAllocator(cap * threadCount * Desc_o::SizeOf(transSize), threadCount, Desc_o::SizeOf(transSize))
        , m_nodeDescAllocator(cap * threadCount *  sizeof(NodeDesc_o) * transSize, threadCount, sizeof(NodeDesc_o))
    { 
        m_skiplist = obsskip_alloc(&m_descAllocator, &m_nodeDescAllocator);
        init_obsskip_subsystem(); 
    }

    ~SetAdaptor()
    {
        obsskip_free(m_skiplist);
    }

    void Init()
    {
        m_descAllocator.Init();
        m_nodeDescAllocator.Init();
        ResetMetrics(m_skiplist);
    }

    uint32_t Uninit()
    {
        return 0;
        destroy_obsskip_subsystem(); 
    }

    bool ExecuteOps(const SetOpArray& ops)
    {
        //TransList::Desc_o* desc = m_list.AllocateDesc(ops.size());
        Desc_o* desc = m_descAllocator.Alloc();
        desc->size = ops.size();
        desc->status = LIVE;

        for(uint32_t i = 0; i < ops.size(); ++i)
        {
            desc->ops[i].type = ops[i].type; 
            desc->ops[i].key = ops[i].key; 
        }

        return execute_ops(m_skiplist, desc);
    }

private:
    Allocator<Desc_o> m_descAllocator;
    Allocator<NodeDesc_o> m_nodeDescAllocator;
    obs_skip* m_skiplist;
};

template<>
class SetAdaptor<stm_skip>
{
public:
    SetAdaptor()
    {
        init_stmskip_subsystem();
        m_list = stmskip_alloc();
    }
    
    ~SetAdaptor()
    {
        destory_stmskip_subsystem();
    }

    void Init()
    {
        ResetMetrics();
    }

    uint32_t Uninit()
    { 
        return 0;
    }

    bool ExecuteOps(const SetOpArray& ops)
    {
        bool ret = stmskip_execute_ops(m_list, (set_op*)ops.data(), ops.size());

        return ret;
    }

private:
    stm_skip* m_list;
};



#endif /* end of include guard: SETADAPTOR_H */
