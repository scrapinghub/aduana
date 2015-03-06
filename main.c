#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <inttypes.h>

#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>

#include "edge_stream.h"


typedef struct {
     float out_degree;
     float value_1;
     float value_2;
} PageRank;

typedef struct {
     PageRank *mem;
     FILE *disk;
     int64_t n_vertices;
     size_t size;
     float damping;
} VertexDB;

void
vertex_db_new(VertexDB *db, int64_t n_vertices, const char *fname) {
     db->damping = 0.85; /* TODO */
     db->n_vertices = n_vertices;
     db->size = n_vertices*sizeof(PageRank);
     if (fname == 0) {
	  db->disk = tmpfile();
     } else {
	  db->disk = fopen(fname, "wb+");
     }
  
     ftruncate(fileno(db->disk), db->size);
     db->mem = mmap(0, db->size, 
		    PROT_READ | PROT_WRITE, MAP_SHARED, fileno(db->disk), 0);   
}

void
vertex_db_delete(VertexDB *db) {
     msync(db->mem, db->size, MS_SYNC);
     munmap(db->mem, db->size);
     fclose(db->disk);
}

void 
vertex_db_init(VertexDB *db, const char *edge_fname) {
     EdgeStream es;
     edge_stream_new(&es, edge_fname);

     Edge edge;
     while (true) {
	  if (!edge_stream_next(&es, &edge)) {
	       break;
	  }
	  PageRank* pr = db->mem + edge.from;
	  ++(pr->out_degree);
	  pr->value_2 = 1.0/(db->n_vertices);	  
     }
     edge_stream_delete(&es);  
}

void
vertex_db_update(VertexDB *db, const char *edge_fname) {
     EdgeStream es;
     edge_stream_new(&es, edge_fname);

     Edge edge;
     while (true) {
	  if (!edge_stream_next(&es, &edge)) {
	       break;
	  }
	  PageRank* pr_from = db->mem + edge.from;
	  PageRank* pr_to = db->mem + edge.to;
	  
	  pr_to->value_1 += pr_from->value_2/pr_from->out_degree;	  	  
     }
     edge_stream_delete(&es);  
}

void
vertex_db_commit(VertexDB *db) {
     const float a = (1.0 - db->damping)/db->n_vertices;

     for (PageRank* pr=db->mem; pr!=&(db->mem[db->n_vertices]); ++pr) {
	  pr->value_2 = a  + db->damping*pr->value_1;
	  pr->value_1 = 0.0;
     }
}


#define KB 1024LL
#define MB (1024LL * KB)
#define GB (1024LL * MB)
#define TB (1024LL * GB)

int 
main(int argc, char **argv) {
     VertexDB db;
     vertex_db_new(&db, 4*GB, "./bigpr.bin");

     for (int i=1; i<argc; ++i) {
	  if (i % 10 == 0) {
	       printf("Init [% 4d/% 4d] %s\n", i, argc - 1, argv[i]);
	  }
	  vertex_db_init(&db, argv[i]);
     }

     for (int i=1; i<4; ++i) {
	  printf("PageRank loop % 2d\n", i);
	  printf("--------------------------------\n");
	  for (int j=1; j<argc; ++j) {
	       if (j % 10 == 0) {
		    printf("Update [% 4d/% 4d] %s\n", j, argc - 1, argv[j]);
	       }
	       vertex_db_update(&db, argv[j]);
	  }
     }

     /* Print results */
#if 0
     for (int64_t i=0; i<db.n_vertices; ++i) {
         printf("% 12"PRId64" PR: %.3e\n", i, db.mem[i].value_2);
     }
#endif

     vertex_db_delete(&db);
     return 0;
}
