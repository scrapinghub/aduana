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
     MMapArray *out_degree;
     MMapArray *value1;
     MMapArray *value2;

     size_t n_pages;   
     float damping;
} PageRank;

  
PageRank*
page_rank_new(size_t max_vertices, float damping) {
     PageRank *pr = (PageRank*)malloc(sizeof(PageRank));

     pr->damping = damping;
     pr->n_pages = 0;

     pr->out_degree = mmap_array_new_from_path("./out_degree.bin", max_vertices, sizeof(float));
     if (pr->out_degree == 0) {
	  fprintf(stderr, "Error initialing out degree data\n");	  
	  exit(EXIT_FAILURE);
     }
     mmap_array_exit_on_error(pr->out_degree);

     pr->value1 = mmap_array_new_from_path("./pr.bin", max_vertices, sizeof(float));
     if (pr->value1 == 0) {
	  fprintf(stderr, "Error intializing value (1) data\n");	  
	  exit(EXIT_FAILURE);
     }
     mmap_array_exit_on_error(pr->value1);
     

     pr->value2 = mmap_array_new_from_path(0, max_vertices, sizeof(float));
     if (pr->value2 == 0) {
	  fprintf(stderr, "Error initializing value (2) data\n");	  
	  exit(EXIT_FAILURE);
     }
     mmap_array_exit_on_error(pr->value2);
     

     mmap_array_advise(pr->value1, MADV_SEQUENTIAL);
     const float v0 = 1.0/(float)pr->value1->n_elements;
     for (size_t i=0; i<pr->value1->n_elements; ++i) {
	  mmap_array_set(pr->value1, i, &v0);
     }
     mmap_array_advise(pr->value1, MADV_DONTNEED);

     /* Probably unnecessary. ftruncate fills the file with the '\0' char.
	In turn, IEEE standard defines 0.0 with all bits 0 */     
     mmap_array_advise(pr->out_degree, MADV_SEQUENTIAL);
     const float zero = 0.0;
     for (size_t i=0; i<pr->out_degree->n_elements; ++i) {
	  mmap_array_set(pr->out_degree, i, &zero);
     }
     mmap_array_advise(pr->out_degree, MADV_DONTNEED);

     return pr;
}

void
page_rank_delete(PageRank *pr) {
     mmap_array_delete(pr->out_degree);
     mmap_array_delete(pr->value1);
     mmap_array_delete(pr->value2);     
     free(pr);
}

void
page_rank_expand(PageRank *pr) {
     mmap_array_resize(pr->out_degree, 2*pr->out_degree->n_elements);
     mmap_array_resize(pr->value1, 2*pr->value1->n_elements);
     mmap_array_resize(pr->value2, 2*pr->value2->n_elements);
}

void
page_rank_set_n_pages(PageRank *pr, size_t n_pages) {
     pr->n_pages = n_pages;
     if (n_pages > pr->out_degree->n_elements) {
	  page_rank_expand(pr);
     }    
}

void 
page_rank_init(PageRank *pr, const char *edge_fname) {
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
     mmap_array_advise(pr->out_degree, MADV_SEQUENTIAL);

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
	       if (edge.from >= (int64_t)pr->n_pages) {
		    page_rank_set_n_pages(pr, edge.from + 1);
	       }
	       if (edge.to >= (int64_t)pr->n_pages) {
		    page_rank_set_n_pages(pr, edge.to + 1);
	       }	       	      
	       (*(float*)mmap_array_idx(pr->out_degree, edge.from))++;
	       break;
	  }
     } while (!end_stream);

     edge_stream_delete(es);  
     close(fd);
}

void
page_rank_begin_loop(PageRank *pr) {
     mmap_array_advise(pr->out_degree, MADV_SEQUENTIAL);
     mmap_array_advise(pr->value1, MADV_SEQUENTIAL);
     float out_degree = 0.0;
     for (size_t i=0; i<pr->n_pages; ++i) {
	  out_degree = *(float*)mmap_array_idx(pr->out_degree, i);
	  if (out_degree > 0) {
	       *(float*)mmap_array_idx(pr->value1, i) *= pr->damping/out_degree;
	  }
     }
     mmap_array_advise(pr->out_degree, MADV_DONTNEED);
     mmap_array_advise(pr->value1, MADV_DONTNEED);

     mmap_array_advise(pr->value2, MADV_SEQUENTIAL);
     const float d = (1.0 - pr->damping)/pr->n_pages;
     for (size_t i=0; i<pr->n_pages; ++i) {
	  mmap_array_set(pr->value2, i, &d);
     }
}

void
page_rank_stream_edges(PageRank *pr, const char *edge_fname) {
     mmap_array_advise(pr->value2, MADV_RANDOM);
     mmap_array_advise(pr->value1, MADV_SEQUENTIAL);
     
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
	       *(float*)mmap_array_idx(pr->value2, edge.to) += 
		    *(float*)mmap_array_idx(pr->value1, edge.from);
	       break;
	  }
     } while (!end_stream);
     
     edge_stream_delete(es);  
     close(fd);
}

void
page_rank_end_loop(PageRank *pr) {
     memcpy(pr->value1->mem, 
	    pr->value2->mem, 
	    pr->value2->n_elements*pr->value2->element_size);
}


#define KB 1024LL
#define MB (1024LL * KB)

int 
main(int argc, char **argv) {
     size_t max_vertices = 1*MB;
     PageRank *pr = page_rank_new(max_vertices, 0.85);
     printf("Initial memory allocation done for %zu vertices \n", max_vertices);

     for (int i=1; i<argc; ++i) {
	  if (i % 10 == 0) {
	       printf("Init [% 4d/% 4d] %s\n", i, argc - 1, argv[i]);
	  }
	  page_rank_init(pr, argv[i]);
     }
     printf("\nNumber of pages: %zu\n", pr->n_pages);

     for (int i=0; i<4; ++i) {	  
	  printf("PageRank loop % 2d\n", i);
	  printf("--------------------------------\n");
	  clock_t t = clock();
	  for (int j=1; j<argc; ++j) {
	       if (j % 10 == 0) {
		    printf("Update [% 4d/% 4d] %s\n", j, argc - 1, argv[j]);
	       }
	       page_rank_stream_edges(pr, argv[j]);
	  }
	  page_rank_end_loop(pr);
	  t = clock() - t;
	  printf("PageRank loop: %.2f\n", ((float)t)/CLOCKS_PER_SEC);
     }

     
#ifdef PRINT_RESULTS // Print results 
     for (size_t i=0; i<pr->n_pages; ++i) {
	  printf("% 12zu PR: %.3e\n", i, *(float*)mmap_array_idx(pr->value1, i));
     }
#endif

     page_rank_delete(pr);
     return 0;
}
