#define _POSIX_C_SOURCE 200112L
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

void madvise_or_die(void *dst, size_t size, int flag) {
     if (madvise(dst, size, flag) != 0) {
	  perror("Error in madvise\n");
	  exit(EXIT_FAILURE);
     }
}

void msync_or_die(void *src, size_t size, int flag) {
     if (msync(src, size, flag) != 0) {
	  perror("Error in msync\n");
	  exit(EXIT_FAILURE);
     }
}

#ifndef real_t
#define real_t float
#endif

typedef struct {
     real_t *out_degree;
     real_t *value1;
     real_t *value2;
   
     int fd_out_degree;
     int fd_value1;
     int fd_value2;

     size_t size;
     int64_t max_vertices;
     int64_t n_vertices;
     
     real_t damping;
} VertexDB;


/* Notice that this functions requires to have zram enabled in the Linux 
   kernel. It requires 3 big enough block devices at /dev/zram{0,1,2}.
   See:

       https://www.kernel.org/doc/Documentation/blockdev/zram.txt

   Make sure also that you have file permissions for the devices. In my system
   at least this means the user is in the disk group.
 */
   
void
vertex_db_new(VertexDB *db, int64_t max_vertices, real_t damping) {
     db->damping = damping;
     db->max_vertices = max_vertices;
     db->n_vertices = 0;
     db->size = max_vertices*sizeof(real_t);

     db->fd_out_degree = open("/dev/zram0", O_RDWR);
     if (db->fd_out_degree == -1) {
	  perror("Error opening out degree file");
          exit(EXIT_FAILURE);
     }

     db->fd_value1 = open("/dev/zram1", O_CREAT | O_RDWR, S_IREAD | S_IWRITE);
     if (db->fd_value1 == -1) {
          perror("Error opening value1 file");
          exit(EXIT_FAILURE);
     }

#if 0
     db->fd_value2 = open("/dev/zram2", O_CREAT | O_RDWR, S_IREAD | S_IWRITE);
     if (db->fd_value2 == -1) {
	  perror("Error opening value2 file");
          exit(EXIT_FAILURE);
     }
#else
     db->fd_value2 = 0;
#endif

     ftruncate(db->fd_out_degree, db->size);
     ftruncate(db->fd_value1, db->size);
#if 0
     ftruncate(db->fd_value2, db->size);
#endif

     db->out_degree = mmap(
	  0, db->size, PROT_READ | PROT_WRITE, MAP_SHARED, db->fd_out_degree, 0);   
     if (db->out_degree == MAP_FAILED) {
	  perror("Could not mmap out_degree");
	  exit(EXIT_FAILURE);
     }

     db->value1 = mmap(
	  0, db->size, PROT_READ | PROT_WRITE, MAP_SHARED, db->fd_value1, 0);   
     if (db->value1 == MAP_FAILED) {
	  perror("Could not mmap value1");
	  exit(EXIT_FAILURE);
     }
#if 0
     db->value2 = mmap(
	  0, db->size, PROT_READ | PROT_WRITE, MAP_SHARED, db->fd_value2, 0);   
#else
     db->value2 = mmap(
	  0, db->size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, db->fd_value2, 0);   
#endif

     if (db->value2 == MAP_FAILED) {
	  perror("Could not mmap value2");
	  exit(EXIT_FAILURE);
     }

     madvise_or_die(db->value1, db->size, MADV_SEQUENTIAL);
     const real_t v0 = 1.0/(real_t)db->max_vertices;
     for (int64_t i=0; i<db->max_vertices; ++i) {
	  db->value1[i] = v0;
     }
     msync_or_die(db->value1, db->size, MS_SYNC);
     madvise_or_die(db->value1, db->size, MADV_DONTNEED);     

     /* Probably unnecessary. ftruncate fills the file with the '\0' char.
	In turn, IEEE standard defines 0.0 with all bits 0 */
     madvise_or_die(db->out_degree, db->size, MADV_SEQUENTIAL);
     for (int64_t i=0; i<db->max_vertices; ++i) {
	  db->out_degree[i] = 0.0;
     }
     msync_or_die(db->out_degree, db->size, MS_SYNC);
     madvise_or_die(db->out_degree, db->size, MADV_DONTNEED);     

}

void
vertex_db_delete(VertexDB *db) {
     msync_or_die(db->value1, db->size, MS_SYNC);
     msync_or_die(db->value2, db->size, MS_SYNC);

     if (munmap(db->value1, db->size) != 0) {
	  perror("Could not munmap value1");	  
     }
     if (munmap(db->value2, db->size) != 0) {
	  perror("Could not munmap value2");	  
     }
     if (munmap(db->out_degree, db->size) !=0) {
	  perror("Could not munmap out_degree");	  	 
     }

     if (close(db->fd_value1) != 0) {
	  perror("Error closing value1 file descriptor");
     }
     if (close(db->fd_value2) != 0) {
	  perror("Error closing value2 file descriptor");
     }     
     if (close(db->fd_out_degree) != 0) {
	  perror("Error closing out_degree file descriptor");
     }
}

void
vertex_set_n_vertices(VertexDB *db, int64_t n_vertices) {
     if (n_vertices <= db->max_vertices) {
	  db->n_vertices = n_vertices;
     } else {
	  printf("Error: trying to set more vertices (%"PRId64 ") than available (%"PRId64")\n", 
		 n_vertices, db->max_vertices);
	  exit(EXIT_FAILURE);
     }
}

void 
vertex_db_init(VertexDB *db, const char *edge_fname) {
     int fd = open(edge_fname, O_RDONLY);
     if (fd == -1) {
	  perror(edge_fname);
	  exit(EXIT_FAILURE);
     }
     EdgeStream *es = edge_stream_new(fd);
     if (es == 0) {
	  printf("Error creating edge stream for %s\n", edge_fname);
	  exit(EXIT_FAILURE);
     }
     madvise_or_die(db->out_degree, db->size, MADV_SEQUENTIAL);

     Edge edge;
     bool end_stream = false;
     do {
	  switch(edge_stream_next(es, &edge)) {
	  case EDGE_STREAM_ERROR:
	       printf("Error processing edge stream for %s\n", edge_fname);
	       printf("%s\n", es->error_name);
	       exit(EXIT_FAILURE);
	  case EDGE_STREAM_END:
	       end_stream = true;
	       break;
	  case EDGE_STREAM_NEXT:     	      
	       if (edge.from >= db->n_vertices) {
		    vertex_set_n_vertices(db, edge.from + 1);
	       }
	       if (edge.to >= db->n_vertices) {
		    vertex_set_n_vertices(db, edge.to + 1);
	       }	       	      
	       db->out_degree[edge.from]++;
	       break;
	  }
     } while (!end_stream);

     edge_stream_delete(es);  
     close(fd);
}

void
vertex_db_begin_loop(VertexDB *db) {
     madvise_or_die(db->out_degree, db->size, MADV_SEQUENTIAL);
     madvise_or_die(db->value1, db->size, MADV_SEQUENTIAL);
     real_t out_degree = 0.0;
     for (int64_t i=0; i<db->n_vertices; ++i) {
	  out_degree = db->out_degree[i];
	  if (out_degree > 0) {
	       db->value1[i] *= db->damping/out_degree;
	  }
     }
     madvise_or_die(db->out_degree, db->size, MADV_DONTNEED);
     madvise_or_die(db->value1, db->size, MADV_DONTNEED);

     madvise_or_die(db->value2, db->size, MADV_SEQUENTIAL);
     const real_t d = (1.0 - db->damping)/db->n_vertices;
     for (int64_t i=0; i<db->n_vertices; ++i) {
	  db->value2[i] = d;
     }
}

void
vertex_stream_edges(VertexDB *db, const char *edge_fname) {
     madvise_or_die(db->value2, db->size, MADV_RANDOM);
     madvise_or_die(db->value1, db->size, MADV_SEQUENTIAL);
     
     int fd = open(edge_fname, O_RDONLY);    
     if (fd == -1) {
	  perror(edge_fname);
	  exit(EXIT_FAILURE);
     }
     EdgeStream *es = edge_stream_new(fd);    
     if (es == 0) {
	  printf("Error creating edge stream for %s\n", edge_fname);
	  exit(EXIT_FAILURE);
     }
     Edge edge;
     bool end_stream = false;
     do {
	  switch(edge_stream_next(es, &edge)) {
	  case EDGE_STREAM_ERROR:
	       printf("Error processing edge stream for %s\n", edge_fname);
	       printf("%s\n", es->error_name);
	       exit(EXIT_FAILURE);
	  case EDGE_STREAM_END:
	       end_stream = true;
	       break;
	  case EDGE_STREAM_NEXT:     	      
	       db->value2[edge.to] += db->value1[edge.from];
	       break;
	  }
     } while (!end_stream);
     
     edge_stream_delete(es);  
     close(fd);
}

void
vertex_db_end_loop(VertexDB *db) {
#if 0
     real_t *tmp = db->value1;
     db->value1 = db->value2;
     db->value2 = tmp;
#else
     memcpy(db->value1, db->value2, db->n_vertices*sizeof(real_t));
#endif
}


#define KB 1024LL
#define MB (1024LL * KB)
#define GB (1024LL * MB)
#define TB (1024LL * GB)

int 
main(int argc, char **argv) {
     VertexDB db;
     vertex_db_new(&db, 4*GB, 0.85);
     printf("Memory allocation done for %"PRId64" vertices \n", db.max_vertices);

     for (int i=1; i<argc; ++i) {
	  if (i % 10 == 0) {
	       printf("Init [% 4d/% 4d] %s\n", i, argc - 1, argv[i]);
	  }
	  vertex_db_init(&db, argv[i]);
     }
     printf("\nNumber of pages: %"PRId64"\n", db.n_vertices);

     /* Iterate an even number of times */
     for (int i=0; i<4; ++i) {	  
	  printf("PageRank loop % 2d\n", i);
	  printf("--------------------------------\n");
	  clock_t t = clock();
	  #pragma omp for
	  for (int j=1; j<argc; ++j) {
	       if (j % 10 == 0) {
		    printf("Update [% 4d/% 4d] %s\n", j, argc - 1, argv[j]);
	       }
	       vertex_stream_edges(&db, argv[j]);
	  }
	  vertex_db_end_loop(&db);
	  t = clock() - t;
	  printf("PageRank loop: %.2f\n", ((float)t)/CLOCKS_PER_SEC);
     }

     /* Print results */
#if 0
     for (int64_t i=0; i<db.n_vertices; ++i) {
         printf("% 12"PRId64" PR: %.3e\n", i, db.value1[i]);
     }
#endif

     vertex_db_delete(&db);
     return 0;
}
