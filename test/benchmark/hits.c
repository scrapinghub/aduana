#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "lz4_link_stream.h"
#include "hits.h"

int 
main(int argc, char **argv) {
     if (argc != 2) {
	  printf("Usage: hits path_to_lz4_links\n");
	  return -1;
     }

     char test_dir[] = "test-hits-XXXXXX";
     mkdtemp(test_dir);
     char data[] = "test-hits-XXXXXX/data.mdb";
     char lock[] = "test-hits-XXXXXX/lock.mdb";
     for (size_t i=0; test_dir[i] != 0; i++)
	  data[i] = lock[i] = test_dir[i];

     Hits *hits;
     if (hits_new(&hits, test_dir, 1000000) != 0) 
	  return -1;
     printf("Initialized HITS\n");

     LZ4LinkStream *link_stream;
     if (lz4_link_stream_new(&link_stream, argv[1]) != 0)
	  return -1;
	  
     clock_t t = clock();
     if (hits_compute(hits, link_stream, lz4_link_stream_next, lz4_link_stream_reset) != 0)
	  return -1;
     t = clock() - t;
     printf("HITS loop: %.2f\n", ((float)t)/CLOCKS_PER_SEC/30.0);


#ifdef PRINT_RESULTS
     for (size_t i=0; i<hits->n_pages; ++i) {
	  printf("% 12zu PR: %.3e %.3e\n", i, 
		 *(float*)mmap_array_idx(hits->h1, i),
		 *(float*)mmap_array_idx(hits->a1, i));
     }
#endif

     hits_delete(hits);

     remove(data);
     remove(lock);
     remove(test_dir);
     return 0;
}
