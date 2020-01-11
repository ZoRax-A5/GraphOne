#pragma once

#include "graph_base.h"
#include "log.h"

template <class T>
class gview_t {
 public:
    pgraph_t<T>*    pgraph; 
    snapshot_t*     snapshot;
    snapshot_t*     prev_snapshot;
    pthread_t       thread;
    void*           algo_meta;//algorithm specific data
    vid_t           v_count;
    int             flag;
    blog_reader_t<T> reader;
    int             reg_id;
    typename callback<T>::sfunc   sstream_func; 
 public: 
    virtual degree_t get_nebrs_out(vid_t vid, T* ptr) {assert(0); return 0;}
    virtual degree_t get_nebrs_in (vid_t vid, T* ptr) {assert(0); return 0;}
    virtual degree_t get_degree_out(vid_t vid) {assert(0); return 0;}
    virtual degree_t get_degree_in (vid_t vid) {assert(0); return 0;}
    
    virtual delta_adjlist_t<T>* get_nebrs_archived_out(vid_t) {assert(0); return 0;}
    virtual delta_adjlist_t<T>* get_nebrs_archived_in(vid_t) {assert(0); return 0;}
    virtual index_t get_nonarchived_edges(edgeT_t<T>*& ptr) {assert(0); return 0;}
    
    virtual status_t    update_view() {assert(0); return eOK;}
    virtual void        update_view_done() {assert(0);}
    //void    init_view(pgraph_t<T>* pgraph, index_t a_flag);
    virtual bool has_vertex_changed_out(vid_t v) { assert(0); return false;}
    virtual bool has_vertex_changed_in(vid_t v) { assert(0); return false;}
    virtual int  is_unidir() {assert(0); return 0;}
    
    inline vid_t  get_vcount() { return v_count; }
    inline int    get_snapid() { if(snapshot) return snapshot->snap_id; else return 0;}
    inline int    get_prev_snapid() { if(prev_snapshot) return prev_snapshot->snap_id; else return 0;}
    inline void   set_algometa(void* a_meta) {algo_meta = a_meta;}
    inline void*  get_algometa() {return algo_meta;}
    inline index_t get_snapmarker() {
        if (snapshot) return snapshot->marker;
        return 0;
    }
    
    virtual index_t  get_compaction_marker() {
        if (prev_snapshot) return prev_snapshot->marker;
        return 0;
    }
    inline gview_t() {
        pgraph = 0;
        snapshot = 0;
        prev_snapshot = 0;
        algo_meta = 0;
        v_count = 0;
        flag = 0;
        reg_id = -1;
    } 
    inline virtual ~gview_t() {} 
};

//reading utility
template <class T>
inline void read_edge(blog_t<T>* blog, index_t i, edgeT_t<T>& edge)
{
    index_t e = (i & blog->blog_mask);
    bool rewind1 = !((i >> blog->blog_shift) & 0x1);
    bool rewind2 = IS_DEL(get_dst(blog->blog_beg[e]));
    while (rewind1 != rewind2) {
        usleep(10);
        rewind2 = IS_DEL(get_dst(blog->blog_beg[e]));
    }
    edge = blog->blog_beg[e];
    set_dst(edge, TO_SID(get_dst(edge)));
}
