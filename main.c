#define _POSIX_C_SOURCE 200112L
#define _BSD_SOURCE 1

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>
#include <malloc.h>
#include <string.h>

#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>

#include "edge_stream.h"

typedef struct {
     float *out_degree;
     float *value1;
     float *value2;
   
     int fd_out_degree;
     int fd_value1;
     int fd_value2;

     size_t size;
     int64_t n_vertices;

     float damping;
} VertexDB;


/* Notice that this functions requires to have zram enabled in the Linux 
   kernel. It requires 3 big enough block devices at /dev/zram{0,1,2}.
   See:

       https://www.kernel.org/doc/Documentation/blockdev/zram.txt

   Make sure also that you have file permissions for the devices. In my system
   at least this means the user is in the disk group.
 */
   
void
vertex_db_new(VertexDB *db, int64_t n_vertices, float damping) {
     db->damping = damping;
     db->n_vertices = n_vertices;
     db->size = n_vertices*sizeof(float);

     db->fd_out_degree = open("/dev/zram0", O_RDWR);
     if (db->fd_out_degree == -1) {
	  perror("Error opening out degree file");
          exit(EXIT_FAILURE);
     }

     db->fd_value1 = open("/dev/zram1", O_RDWR);
     if (db->fd_value1 == -1) {
          perror("Error opening value1 file");
          exit(EXIT_FAILURE);
     }

     db->fd_value2 = open("/dev/zram2", O_RDWR);
     if (db->fd_value2 == -1) {
	  perror("Error opening value2 file");
          exit(EXIT_FAILURE);
     }

     ftruncate(db->fd_out_degree, db->size);
     ftruncate(db->fd_value1, db->size);
     ftruncate(db->fd_value2, db->size);

     db->out_degree = mmap(
	  0, db->size, PROT_READ | PROT_WRITE, MAP_SHARED, db->fd_out_degree, 0);   
     db->value1 = mmap(
	  0, db->size, PROT_READ | PROT_WRITE, MAP_SHARED, db->fd_value1, 0);   
     db->value2 = mmap(
	  0, db->size, PROT_READ | PROT_WRITE, MAP_SHARED, db->fd_value2, 0);   

     madvise(db->value1, db->size, MADV_SEQUENTIAL);     
     const float v0 = 1.0/(float)db->n_vertices;
     for (int64_t i=0; i<db->n_vertices; ++i) {
	  db->value1[i] = v0;
     }
     msync(db->value1, db->size, MS_SYNC);
     madvise(db->value1, db->size, MADV_DONTNEED);     

     /* Probably unnecessary. ftruncate fills the file with the '\0' char.
	In turn, IEEE standard defines 0.0 with all bits 0 */
     madvise(db->out_degree, db->size, MADV_SEQUENTIAL);
     for (int64_t i=0; i<db->n_vertices; ++i) {
	  db->out_degree[i] = 0.0;
     }
     msync(db->out_degree, db->size, MS_SYNC);
     madvise(db->out_degree, db->size, MADV_DONTNEED);     

}

void
vertex_db_delete(VertexDB *db) {
     msync(db->value1, db->size, MS_SYNC);
     msync(db->value2, db->size, MS_SYNC);

     munmap(db->value1, db->size);
     munmap(db->value2, db->size);
     munmap(db->out_degree, db->size);

     close(db->fd_value1);
     close(db->fd_value2);
     close(db->fd_out_degree);
}

void 
vertex_db_init(VertexDB *db, const char *edge_fname) {
     EdgeStream es;
     edge_stream_new(&es, edge_fname);

     madvise(db->out_degree, db->size, MADV_SEQUENTIAL);

     Edge edge;
     while (true) {
	  if (!edge_stream_next(&es, &edge)) {
	       break;
	  }
	  db->out_degree[edge.from]++;
     }
     edge_stream_delete(&es);  
}

void
vertex_db_begin_loop(VertexDB *db) {
     madvise(db->out_degree, db->size, MADV_SEQUENTIAL);
     madvise(db->value1, db->size, MADV_SEQUENTIAL);
     float out = 0.0;
     for (int64_t i=0; i<db->n_vertices; ++i) {
	  out = db->out_degree[i];
	  if (out > 0) {
	       db->value1[i] *= db->damping/out;
	  }
     }
     madvise(db->out_degree, db->size, MADV_DONTNEED);
     madvise(db->value1, db->size, MADV_DONTNEED);

     madvise(db->value2, db->size, MADV_SEQUENTIAL);
     const float d = (1.0 - db->damping)/db->n_vertices;
     for (int64_t i=0; i<db->n_vertices; ++i) {
	  db->value2[i] = d;
     }
}
void
vertex_stream_edges(VertexDB *db, const char *edge_fname) {
     EdgeStream es;
     edge_stream_new(&es, edge_fname);
     
     madvise(db->value2, db->size, MADV_RANDOM);
     madvise(db->value1, db->size, MADV_SEQUENTIAL);

     Edge edge;
     while (true) {
	  if (!edge_stream_next(&es, &edge)) {
	       break;
	  }
	  db->value2[edge.to] += db->value1[edge.from];
     }
     edge_stream_delete(&es);  
}

void
vertex_db_end_loop(VertexDB *db) {
     float *tmp = db->value1;
     db->value1 = db->value2;
     db->value2 = tmp;
}


#define KB 1024LL
#define MB (1024LL * KB)
#define GB (1024LL * MB)
#define TB (1024LL * GB)

int 
main(int argc, char **argv) {
     VertexDB db;
     vertex_db_new(&db, 4*GB, 0.85);
     printf("Memory allocation done\n");

     for (int i=1; i<argc; ++i) {
	  if (i % 10 == 0) {
	       printf("Init [% 4d/% 4d] %s\n", i, argc - 1, argv[i]);
	  }
	  vertex_db_init(&db, argv[i]);
     }

     /* Iterate an even number of times */
     for (int i=0; i<4; ++i) {
	  printf("PageRank loop % 2d\n", i);
	  printf("--------------------------------\n");
	  vertex_db_begin_loop(&db);
	  for (int j=1; j<argc; ++j) {
	       if (j % 10 == 0) {
		    printf("Update [% 4d/% 4d] %s\n", j, argc - 1, argv[j]);
	       }
	       vertex_stream_edges(&db, argv[j]);
	  }
	  vertex_db_end_loop(&db);
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
