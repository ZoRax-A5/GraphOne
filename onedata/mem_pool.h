#pragma once

#include <unistd.h>
#include <algorithm>
using std::cout;
using std::endl;
using std::max;

#include <libpmem.h>

static uint32_t adjfilecount = 0; //Rui
extern uint64_t vunit_size;
extern uint64_t snap_size;
extern uint64_t adjlist_size;


/*
template <class T>
index_t TO_CACHELINE<T>(index_t count, index_t meta_size)
{
    index_t total_size = count*sizeof(T) + meta_size;
    index_t cachelined_size = (total_size | CL_UPPER);
}
*/

inline void* alloc_huge(index_t size)
{   
    //void* buf = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB|MAP_HUGE_2MB, 0, 0);
    //if (buf== MAP_FAILED) {
    //    cout << "huge page failed" << endl;
    //}
    //return buf;
    
    return MAP_FAILED;
}

template <class T>
struct mem_t {
    vunit_t<T>* vunit_beg;
    snapT_t<T>* dlog_beg;
    // char*       adjlog_beg;
    int adjlog_beg = -1;
    index_t offset = -1;
	
	public:
    index_t    	delta_size1;
	uint32_t    vunit_count;
	uint32_t    dsnap_count;
#ifdef BULK
    index_t     degree_count;
#endif
	index_t    	delta_size;

};

template <class T>
class thd_mem_t {
    mem_t<T>* mem;  
    bool flag = false;
    bool iter = 0;
    char* buf;
 
 public:	
    inline vunit_t<T>* alloc_vunit() {
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
		if (mem1->vunit_count == 0) {
            vunit_bulk(1L << LOCAL_VUNIT_COUNT);
		}
		mem1->vunit_count--;
		return mem1->vunit_beg++;
	}
    
    inline status_t vunit_bulk(vid_t count) {
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
        mem1->vunit_count = count;
        mem1->vunit_beg = (vunit_t<T>*)alloc_huge(count*sizeof(vunit_t<T>));
        if (MAP_FAILED == mem1->vunit_beg) {
            mem1->vunit_beg = (vunit_t<T>*)calloc(sizeof(vunit_t<T>), count);
            assert(mem1->vunit_beg);
        }
        __sync_fetch_and_add(&vunit_size, sizeof(vunit_t<T>)*count);
        return eOK;
	}	

	inline snapT_t<T>* alloc_snapdegree() {
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
		if (mem1->dsnap_count == 0) {
            snapdegree_bulk(1L << LOCAL_VUNIT_COUNT);
		}
		mem1->dsnap_count--;
		return mem1->dlog_beg++;
	}
    
    inline status_t snapdegree_bulk(vid_t count) {
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
        mem1->dsnap_count = count;
        mem1->dlog_beg = (snapT_t<T>*)alloc_huge(sizeof(snapT_t<T>)*count);
        if (MAP_FAILED == mem1->dlog_beg) {
            mem1->dlog_beg = (snapT_t<T>*)calloc(sizeof(snapT_t<T>), count);
        }
        __sync_fetch_and_add(&snap_size, sizeof(snapT_t<T>)*count);
        return eOK;
	}
    
	inline index_t alloc_adjlist(degree_t count, bool hub) {
		
        index_t size = count*sizeof(T) + sizeof(delta_adjlist_t<T>);
        if (hub || count >= 256) {
            size = TO_PAGESIZE(size);
        } else {
            size = TO_CACHELINE(size);
        }
        degree_t max_count = (size - sizeof(delta_adjlist_t<T>))/sizeof(T);

        // #if defined(DEL) || defined(MALLOC) 
		// adj_list =  (delta_adjlist_t<T>*)malloc(size);
        // assert(adj_list!=0);
        // #else 
        // mem_t<T>* mem1 = mem + omp_get_thread_num();  
        // index_t tmp = 0;
		// if (size > mem1->delta_size) {
		// 	tmp = max(1UL << LOCAL_DELTA_SIZE, size);
        //     delta_adjlist_bulk(tmp);
		// }
		// adj_list = (delta_adjlist_t<T>*)mem1->adjlog_beg;
		// assert(adj_list != 0);
		// mem1->adjlog_beg += size;
		// mem1->delta_size -= size;
        // #endif

        mem_t<T>* mem1 = mem + omp_get_thread_num();
        index_t tmp = 0;
        if (size > mem1->delta_size) {
			tmp = max(1UL << LOCAL_DELTA_SIZE, size);
            // std::cout << "mem1->adjlog_beg = " << mem1->adjlog_beg << ",offset = " << mem1->offset << std::endl;
            // std::cout << "tmp = " << tmp << ", mem1->delta_size = " << mem1->delta_size << ", size = " << size << std::endl;
            delta_adjlist_bulk(tmp);
		}
        index_t adj_list = (((index_t)mem1->adjlog_beg) << 32) + mem1->offset;
        
        mem1->delta_size -= size;
        // mem1->offset += size;
        __sync_fetch_and_add(&mem1->offset, size);

        // adj_list->set_nebrcount(0);
        // adj_list->add_next(0);
        // adj_list->set_maxcount(max_count);
        
        // std::cout << "iter = " << iter << std::endl;
        // std::cout << "adj_list = " << adj_list << std::endl;
        // std::cout << "mem1->adjlog_beg = " << mem1->adjlog_beg << std::endl;
        // std::cout << "mem1->offset = " << mem1->offset << std::endl;
        file_delta_adjlist_t<T>::set_nebrcount(adj_list, 0);
        file_delta_adjlist_t<T>::add_next(adj_list, 0);
        file_delta_adjlist_t<T>::set_maxcount(adj_list, max_count);
        
        // std::cout << "real max_count = " << max_count << std::endl;
        // std::cout << "max_count = " << file_delta_adjlist_t<T>::get_maxcount(adj_list) << std::endl;
        // iter += 1;
        // if (iter == 15) exit(0);
        
		return adj_list;
	}

    void free_adjlist(delta_adjlist_t<T>* adj_list, bool chain) {
        std::cout << "Unexpected Error Here! void free_adjlist(delta_adjlist_t<T>* adj_list, bool chain) " << std::endl;
        return ;
        // #if defined(DEL) || defined(MALLOC)
        // mem_t<T>* mem1 = mem + omp_get_thread_num();  
        // if(chain) {
        //     delta_adjlist_t<T>* adj_list1 = adj_list;
        //     while (adj_list != 0) {
        //         adj_list1 = adj_list->get_next();
        //         free(adj_list);
        //         adj_list = adj_list1;
        //     }
        // } else {
        //     free(adj_list);
        // }
        // #endif
    }
    
    inline status_t delta_adjlist_bulk(index_t size) {
        mem_t<T>* mem1 = mem + omp_get_thread_num();  
        mem1->delta_size = size;
        mem1->offset = 0;

        // Adjlists 放在 FILE 中
        // if (mem1->adjlog_beg != 0) close(mem1->adjlog_beg);
        // std::string filePath = "/mnt/pmem1/zorax/testGraphOne/delta_adjlist_" + std::to_string(__sync_fetch_and_add(&adjfilecount, 1)) + ".txt";
        std::string filePath = "/mnt/ramdisk1/zorax/testGraphOne/delta_adjlist_" + std::to_string(__sync_fetch_and_add(&adjfilecount, 1)) + ".txt";
        assert( access(filePath.c_str(), 'r') == -1 ); //确保文件不存在
        mem1->adjlog_beg = open(filePath.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
        // std::cout << "mem1->adjlog_beg = " << mem1->adjlog_beg << std::endl;
        
        // memset(buf, 0, size);
        
        // lseek(mem1->adjlog_beg, size, SEEK_SET);

        // char* buf = (char*)calloc(size, sizeof(char));
        // write(mem1->adjlog_beg, &buf, size*sizeof(char));
        // free(buf);

        char end = EOF;
        lseek(mem1->adjlog_beg, size, SEEK_SET);
        write(mem1->adjlog_beg, &end, sizeof(char));
        lseek(mem1->adjlog_beg, 0, SEEK_SET);
        assert(mem1->adjlog_beg);

        // mem1->adjlog_beg = (char*)alloc_huge(size);
        // if (MAP_FAILED == mem1->adjlog_beg) {
        //     // // Adjlists 放在 DRAM 上
        //     // mem1->adjlog_beg = (char*)malloc(size);
        //     // assert(mem1->adjlog_beg);

        //     // Adjlists 放在 PMEM 上
        //     std::string filePath = "/mnt/pmem1/zorax/testGraphOne/delta_adjlist_" + std::to_string(__sync_fetch_and_add(&adjfilecount, 1)) + ".txt";
        //     assert( access(filePath.c_str(), 'r') == -1 ); //确保文件不存在
        //     size_t mapped_len;
        //     int is_pmem;
        //     /* create a pmem file and memory map it */
        //     mem1->adjlog_beg = (char*)pmem_map_file(filePath.c_str(), size, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
        //     if (mem1->adjlog_beg == NULL)  {
        //         std::cout << "Could not map pmem file :" << "for delta_adjlist" << " error: " << strerror(errno) << std::endl;
        //     }
        //     // if (!is_pmem){
        //     //     std::cout << "not pmem!" << std::endl;
        //     // }
        //     memset(mem1->adjlog_beg, 0, size);  //pre touch, 消除page fault影响
        //     assert(mem1->adjlog_beg);
        // }
        __sync_fetch_and_add(&adjlist_size, size);
        //cout << "alloc adj " << delta_size << endl; 
        return eOK;
    }

    inline thd_mem_t() {
        if(posix_memalign((void**)&mem, 64, THD_COUNT*sizeof(mem_t<T>))) {
            cout << "posix_memalign failed()" << endl;
            mem = (mem_t<T>*)calloc(sizeof(mem_t<T>), THD_COUNT);
        } else {
            memset(mem, 0, THD_COUNT*sizeof(mem_t<T>));
        } 
    } 
};

