#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "edge_stream.h"
#include "mmap_array.h"


typedef struct {
     MMapArray *hub_1;
     MMapArray *hub_2;
     MMapArray *auth_1;
     MMapArray *auth_2;

     size_t n_pages;   
} HITS;

  
HITS*
hits_new(size_t max_vertices) {
     HITS *hits = (HITS*)malloc(sizeof(HITS));
     hits->n_pages = 0;

     // We assume edges are given in more or less FROM order
     // The update will be:
     //   hub_2[FROM] += auth_1[TO]
     //   auth_2[TO] += hub_1[FROM]
     // This means that hub_1 and hub_2 are read/written in sequential order
     // and authorities in random order

     hits->hub_1 = mmap_array_new_from_path("./hub_1.bin", max_vertices, sizeof(float));
     if (hits->hub_1 == 0) {
	  fprintf(stderr, "Error allocating memory for hub 1 data\n");	  
	  exit(EXIT_FAILURE);
     }
     mmap_array_exit_on_error(hits->hub_1);
    

     hits->hub_2 = mmap_array_new_from_path("./hub_2.bin", max_vertices, sizeof(float));
     if (hits->hub_1 == 0) {
	  fprintf(stderr, "Error allocating memory for hub 2 data\n");	  
	  exit(EXIT_FAILURE);
     }
     mmap_array_exit_on_error(hits->hub_2);
     

     hits->auth_1 = mmap_array_new_from_path(0, max_vertices, sizeof(float));
     if (hits->hub_1 == 0) {
	  fprintf(stderr, "Error allocating memory for auth 1 data\n");	  
	  exit(EXIT_FAILURE);
     }
     mmap_array_exit_on_error(hits->auth_1);
     

     hits->auth_2 = mmap_array_new_from_path(0, max_vertices, sizeof(float));
     if (hits->hub_1 == 0) {
	  fprintf(stderr, "Error allocating memory for auth 2 data\n");	  
	  exit(EXIT_FAILURE);
     }
     mmap_array_exit_on_error(hits->auth_2);     

     const float v0 = 1.0/(float)max_vertices;
     for (size_t i=0; i<max_vertices; ++i) {
	  mmap_array_set(hits->hub_1, i, &v0);
     }
     for (size_t i=0; i<max_vertices; ++i) {
	  mmap_array_set(hits->auth_1, i, &v0);
     }
     return hits;
}

void
hits_delete(HITS *hits) {
     mmap_array_delete(hits->hub_1);
     mmap_array_delete(hits->hub_2);
     mmap_array_delete(hits->auth_1);     
     mmap_array_delete(hits->auth_2);     
     free(hits);
}

void
hits_expand(HITS *hits) {
     mmap_array_resize(hits->hub_1, 2*hits->hub_1->n_elements);
     mmap_array_resize(hits->hub_2, 2*hits->hub_2->n_elements);
     mmap_array_resize(hits->auth_1, 2*hits->auth_1->n_elements);
     mmap_array_resize(hits->auth_2, 2*hits->auth_2->n_elements);
}

void
hits_set_n_pages(HITS *hits, size_t n_pages) {
     hits->n_pages = n_pages;
     if (n_pages > hits->hub_1->n_elements) {	  
	  hits_expand(hits);
     } 	       
}

void
hits_stream_edges(HITS *hits, const char *edge_fname) {    
     int fd = open(edge_fname, O_RDONLY);    
     if (fd == -1) {
	  perror(edge_fname);
	  exit(EXIT_FAILURE);
     }
     EdgeStream *es = edge_stream_new(fd);    
     if (es == 0) {
	  fprintf(stderr, "Error creating edge stream for %s\n", edge_fname);
	  exit(EXIT_FAILURE);
     }
     Edge edge;
     bool end_stream = false;
     do {
	  switch(edge_stream_next(es, &edge)) {
	  case EDGE_STREAM_ERROR:
	       fprintf(stderr, "Error processing edge stream for %s\n", edge_fname);
	       fprintf(stderr, "%s\n", es->error_name);
	       exit(EXIT_FAILURE);
	  case EDGE_STREAM_END:
	       end_stream = true;
	       break;
	  case EDGE_STREAM_NEXT:
	       if (edge.from >= (int64_t)hits->n_pages) {
		    hits_set_n_pages(hits, edge.from + 1);
	       }
	       if (edge.to >= (int64_t)hits->n_pages) {
		    hits_set_n_pages(hits, edge.to + 1);
	       }	       	      
	       *(float*)mmap_array_idx(hits->hub_2, edge.from) += 
		    *(float*)mmap_array_idx(hits->auth_1, edge.to);
	       *(float*)mmap_array_idx(hits->auth_2, edge.to) += 
		    *(float*)mmap_array_idx(hits->hub_1, edge.from);
	       break;
	  }
     } while (!end_stream);
     
     edge_stream_delete(es);  
     close(fd);
}

void
hits_end_loop(HITS *hits) {
     // Normalize output values
     float hub_sum = 0.0;
     for (size_t i=0; i<hits->n_pages; ++i) {
	  hub_sum += *(float*)mmap_array_idx(hits->hub_2, i);
     }
     for (size_t i=0; i<hits->n_pages; ++i) {
	  float *h = (float*)mmap_array_idx(hits->hub_2, i);
	  *h /= hub_sum;
     }

     float auth_sum = 0.0;
     for (size_t i=0; i<hits->n_pages; ++i) {
	  auth_sum += *(float*)mmap_array_idx(hits->auth_2, i);
     }
     for (size_t i=0; i<hits->n_pages; ++i) {
	  float *a = (float*)mmap_array_idx(hits->auth_2, i);
	  *a /= auth_sum;
     }

     // swap for next iter
     MMapArray *tmp = hits->hub_1;
     hits->hub_1 = hits->hub_2;
     hits->hub_2 = tmp;

     memcpy(hits->auth_1->mem, 
	    hits->auth_2->mem, 
	    hits->auth_2->n_elements*hits->auth_2->element_size);

}


#define KB 1024LL
#define MB (1024LL * KB)

int 
main(int argc, char **argv) {
     size_t max_vertices = 1*MB;

     HITS *hits = hits_new(max_vertices);
     printf("Initial memory allocation done for %zu vertices \n", max_vertices);

     for (int i=0; i<4; ++i) {	  
	  printf("HITS loop % 2d\n", i);
	  printf("--------------------------------\n");
	  clock_t t = clock();
	  for (int j=1; j<argc; ++j) {
	       if (j % 10 == 0) {
		    printf("Update [% 4d/% 4d] %s\n", j, argc - 1, argv[j]);
	       }
	       hits_stream_edges(hits, argv[j]);
	  }
	  hits_end_loop(hits);
	  t = clock() - t;
	  printf("HITS loop: %.2f\n", ((float)t)/CLOCKS_PER_SEC);
     }

#ifdef PRINT_RESULTS
     for (size_t i=0; i<hits->n_pages; ++i) {
	  printf("% 12zu PR: %.3e %.3e\n", i, 
		 *(float*)mmap_array_idx(hits->hub_1, i),
		 *(float*)mmap_array_idx(hits->auth_1, i));
     }
#endif

     hits_delete(hits);
     return 0;
}
