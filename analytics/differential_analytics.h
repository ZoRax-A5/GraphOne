#pragma once
#include <omp.h>
#include "graph_view.h"
#include "sstream_analytics.h"
#include "diff_view.h"

void print_pr(float* rank_array)
{
    for (vid_t v = 0; v < 5; ++v) {
        cout << v << ":" << rank_array[v] << endl;
    }
}

template <class T>
void diff_streambfs(gview_t<T>* view)
{
    //
    double start = mywtime ();
    double end = 0;
    int update_count = 0;
    sstream_t<T>* viewh = dynamic_cast<sstream_t<T>*>(view);
   
    vid_t v_count = viewh->get_vcount();
    init_bfs(viewh);


    while (viewh->get_snapmarker() < _edge_count) {
        if (eOK != viewh->update_view()) continue;
        if (update_count == 0) {
            do_streambfs(viewh);
        } else {
            do_diffbfs(viewh);
        }
        ++update_count;
        cout << " update_count = " << update_count << endl;
    }
    print_bfs(viewh);
    cout << " update_count = " << update_count 
         << " snapshot count = " << viewh->get_snapid() << endl;
}

template <class T> 
void do_diffbfs(sstream_t<T>* viewh) 
{
    uint8_t* status = (uint8_t*)viewh->get_algometa();
    vid_t   v_count = viewh->get_vcount();
    uint8_t level = 1;//start from second iteration

    index_t frontier = 0;
    do {
        frontier = 0;
        //double start = mywtime();
        #pragma omp parallel num_threads (THD_COUNT) reduction(+:frontier)
        {
        sid_t sid;
        uint8_t backup_level = 0;
        uint8_t new_level = 255;
        degree_t nebr_count = 0;
        degree_t prior_sz = 65536;
        T* local_adjlist = (T*)malloc(prior_sz*sizeof(T));

        #pragma omp for nowait
        for (vid_t v = 0; v < v_count; v++) {
            if(false == viewh->has_vertex_changed_out(v) || status[v] < level ) continue;
            
            backup_level = status[v];
            new_level =  255;

            //handle the in-edges
            nebr_count = viewh->get_degree_in(v);
            if (nebr_count == 0) {
                continue;
            } else if (nebr_count > prior_sz) {
                prior_sz = nebr_count;
                free(local_adjlist);
                local_adjlist = (T*)malloc(prior_sz*sizeof(T));
            }

            viewh->get_nebrs_in(v, local_adjlist);

            for (degree_t i = 0; i < nebr_count; ++i) {
                sid = get_sid(local_adjlist[i]);
                new_level = min(new_level, status[sid]);
            }

            if (new_level+1 == level && backup_level >= level) {//upgrade case
                status[v] = level;
                viewh->reset_vertex_changed_out(v);
                ++frontier;
            } else if (new_level+1 != level &&  backup_level == level) { //infinity case
                status[v] == 255;
                viewh->reset_vertex_changed_out(v);
                ++frontier;
            } else if (backup_level > level && backup_level != 255) {
                ++frontier;
                continue;
            } else {
                continue;
            }

            nebr_count = viewh->get_degree_out(v);
            if (nebr_count == 0) {
                continue;
            } else if (nebr_count > prior_sz) {
                prior_sz = nebr_count;
                free(local_adjlist);
                local_adjlist = (T*)malloc(prior_sz*sizeof(T));
            }

            viewh->get_nebrs_out(v, local_adjlist);

            for (degree_t i = 0; i < nebr_count; ++i) {
                sid = get_sid(local_adjlist[i]);
                viewh->set_vertex_changed_out(sid);
            }
        }
        }
        ++level;
    } while (frontier);
}

template <class T> 
void init_pr(gview_t<T>* viewh) {
    vid_t v_count = viewh->get_vcount();

    index_t r = 5; 
    index_t c = v_count;
    index_t len = 0; 
    float *ptr, **arr; 
              
    len = sizeof(float *) *r + sizeof(float) * c * r; 
    arr = (float **)calloc(len, sizeof(uint8_t));

    // ptr is now pointing to the first element in 2D array 
    ptr = (float *)(arr + r);

    // for loop to point rows pointer to appropriate location in 2D array 
    for(vid_t i = 0; i < r; i++) {
        arr[i] = (ptr + c * i);
    }
    viewh->set_algometa(arr);
}

template <class T> 
void do_diffpr(diff_view_t<T>* viewh, float epsilon)
{
    vid_t v_count = viewh->get_vcount();
    float** pr_rank = (float**)viewh->get_algometa();
    float* rank_array = 0;
    float* prior_rank_array = 0;

    //these two are backup ranks
    float* rank_array1 = (float*)calloc(v_count,sizeof(float));

    int iter = 1;
    float* prior_rank_array1 = pr_rank[0];

    //float delta = epsilon + epsilon;//any value greater
	//while(delta > epsilon) 
    
    for (; iter < 5; ++iter)
    {
        //double start1 = mywtime();
        prior_rank_array = pr_rank[iter - 1];
        rank_array =  pr_rank[iter];
        //delta = 0;
        #pragma omp parallel num_threads(THD_COUNT) 
        {
            sid_t sid;
            degree_t  nebr_count = 0;
            degree_t total_count = 0;
            degree_t diff_degree = 0;

            degree_t prior_sz = 2048;
            T* local_adjlist = (T*) malloc(sizeof(T)*prior_sz);;

            float rank = 0.0; 
            float rank1 = 0.0; 
            float rank2 = 0.0; 
            
            #pragma omp for 
            for (vid_t v = 0; v < v_count; v++) {
                if (!viewh->has_vertex_changed_out(v)) continue;
                
                nebr_count = viewh->get_diff_degree_out(v);
                if (nebr_count == 0) {
                    continue;
                } else if (nebr_count > prior_sz) {
                    prior_sz = nebr_count;
                    free(local_adjlist);
                    local_adjlist = (T*)malloc(prior_sz*sizeof(T));
                }

                total_count = viewh->get_diff_nebrs_out(v, local_adjlist, diff_degree);

                rank1 = prior_rank_array[v]/viewh->get_degree_out(v); 
                rank2 = prior_rank_array1[v]/viewh->get_prior_degree_out(v); 
                rank = rank1 - rank2;

                for (degree_t i = 0; i < diff_degree; ++i) {//existing edge
                    sid = get_sid(local_adjlist[i]);
                    assert (!IS_DEL(sid)); 
                    qthread_dincr(rank_array1 + TO_VID(sid), rank);
                }
                for (degree_t i = diff_degree; i < total_count; ++i) {//new ones
                    sid = get_sid(local_adjlist[i]);
                    if (!IS_DEL(sid)) {
                        //newly added edge
                        qthread_dincr(rank_array1 + TO_VID(sid), rank1);
                    } else {
                        //deleted edges
                        qthread_dincr(rank_array1 + TO_VID(sid), -rank2);
                    }
                }
            }
            
            if (iter == 1) {
                prior_rank_array1 = (float*)malloc(v_count*sizeof(float));
            }
            double mydelta = 0;
            double new_rank = 0;
            degree_t degree_out;
            degree_t prior_degree_out = 0;
            
            #pragma omp for //reduction(+:delta)
            for (vid_t v = 0; v < v_count; v++ ) {
                if (rank_array1[v] == 0) continue;
                
                degree_out = viewh->get_degree_out(v);
                prior_degree_out = viewh->get_prior_degree_out(v);
                
                if (prior_degree_out != 0) {
                    new_rank = rank_array[v] + 0.85*rank_array1[v]; 
                    // old rank + new delta rank /degree_out; //normalized
                } else {
                    new_rank = 0.15 + 0.85*rank_array1[v];
                }
                /*
                mydelta =  new_rank - prior_rank_array[v];//diff
                if (mydelta < 0) mydelta = -mydelta;
                delta += mydelta;
                */

                prior_rank_array1[v] = rank_array[v];//used for next iter
                rank_array1[v] = 0;//to be used for next iter;
                rank_array[v] = new_rank;//updated rank 
                viewh->set_vertex_changed_out(v);
            } 
        }
        //++iter;
    }	
}
template <class T> 
void do_streampr(sstream_t<T>* viewh, float epsilon)
{
    vid_t v_count = viewh->get_vcount();
	//let's run the pagerank
    float** pr_rank = (float**)viewh->get_algometa();
    float* rank_array = pr_rank[0];
    
    //normalize the starting rank, it is like iteration 0;
    #pragma omp parallel num_threads(THD_COUNT)
    { 
        //degree_t degree_out = 0;
        #pragma omp for
        for (vid_t v = 0; v < v_count; ++v) {
            rank_array[v] = 1.0;///degree_out;//normalizing
        }
    }

    int iter = 1;
    float* prior_rank_array = 0;
    //float delta = epsilon + epsilon;
	//while(delta > epsilon) 
    for (; iter < 5; ++iter)
    {
        //double start1 = mywtime();
        prior_rank_array = pr_rank[iter - 1];
        rank_array =  pr_rank[iter];
        //delta = 0;
        #pragma omp parallel num_threads (THD_COUNT) 
        {
            sid_t sid;
            degree_t degree_out = 0;
            degree_t prior_sz = 2048;
            T* local_adjlist = (T*) malloc(sizeof(T)*prior_sz);;

            double rank = 0.0; 
            
            #pragma omp for 
            for (vid_t v = 0; v < v_count; v++) {
                degree_out = viewh->get_degree_out(v);
                if (degree_out == 0) {
                    continue;
                } else if (degree_out > prior_sz) {
                    prior_sz = degree_out;
                    free(local_adjlist);
                    local_adjlist = (T*)malloc(prior_sz*sizeof(T));
                }

                viewh->get_nebrs_out(v, local_adjlist);
                rank = prior_rank_array[v]/degree_out;

                for (degree_t i = 0; i < degree_out; ++i) {
                    sid = get_sid(local_adjlist[i]);
                    qthread_dincr(rank_array + sid, rank);
                }
            }
            
            double mydelta = 0;
            double new_rank = 0;
            
            #pragma omp for //reduction(+:delta)
            for (vid_t v = 0; v < v_count; v++ ) {
                degree_out = viewh->get_degree_out(v);
                if (degree_out == 0) continue;
                
                new_rank = (0.15 + 0.85*rank_array[v]);///degree_out;//normalized
                
                /*
                mydelta =  new_rank - prior_rank_array[v];
                if (mydelta < 0) mydelta = -mydelta;
                delta += mydelta;
                */

                rank_array[v] = new_rank;
            } 
        }
        //++iter;
    }	
}

template <class T>
void stream_pr(gview_t<T>* view)
{
    double start = mywtime ();
    double end = 0;
    int update_count = 0;
    
    float epsilon = 1e-6;
    sstream_t<T>* viewh = dynamic_cast<sstream_t<T>*>(view);
    vid_t v_count = viewh->get_vcount();
    init_pr(viewh);

    while (viewh->get_snapmarker() < _edge_count) {
        if (eOK != viewh->update_view()) continue;
    }
    {
        if (update_count == 0) {
            do_streampr(viewh, epsilon);
        } else {
            do_diffpr((diff_view_t<T>*)viewh, epsilon);
        }
        ++update_count;
        cout << " update_count = " << update_count << endl;
    }
    //print_bfs(viewh);
    cout << " update_count = " << update_count 
         << " snapshot count = " << viewh->get_snapid() << endl;
    float** arr = (float**) viewh->algo_meta;
    print_pr(arr[4]);
}

template <class T>
void diff_stream_pr(gview_t<T>* view)
{
    double start = mywtime ();
    double end = 0;
    int update_count = 0;
    
    float epsilon = 1e-6;
    diff_view_t<T>* viewh = dynamic_cast<diff_view_t<T>*>(view);
    vid_t v_count = viewh->get_vcount();
    init_pr(viewh);
    float** arr = (float**) viewh->algo_meta;
        
    sleep(1);

    while (viewh->get_snapmarker() < _edge_count) {
        if (eOK != viewh->update_view()) continue;
        if (update_count == 0) {
            do_streampr(viewh, epsilon);
        } else {
            do_diffpr(viewh, epsilon);
        }
        ++update_count;
        cout << " update_count = " << update_count << endl;
    }
    cout << " update_count = " << update_count 
         << " snapshot count = " << viewh->get_snapid() << endl;
    print_pr(arr[4]);
}
