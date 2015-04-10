#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <malloc.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "mmap_array.h"
#include "pagedb.h"
#include "hits.h"

static void
hits_set_error(Hits *hits, int error, const char *msg) {
     hits->error = error;
     strncpy(hits->error_msg, msg, HITS_MAX_ERROR_LENGTH);
}

static void
hits_add_error_aux(Hits *hits, const char *msg) {
     (void)strncat(
	  hits->error_msg,
	  msg,
	  HITS_MAX_ERROR_LENGTH -
	       strnlen(hits->error_msg, HITS_MAX_ERROR_LENGTH));
}

static void
hits_add_error(Hits *hits, const char *msg) {
    hits_add_error_aux(hits, ": ");
    hits_add_error_aux(hits, msg);
}

HitsError
hits_new(Hits **hits, const char *path, size_t max_vertices) {
     Hits *p = *hits = malloc(sizeof(*p));
     if (!p)
	  return hits_error_memory;
     p->n_pages = 0;
     p->max_loops = 0;
     hits_set_error(p, hits_error_ok, "NO ERROR");

     char *error1 = 0;
     char *error2 = 0;
     char *ph1 = build_path(path, "hits_h1.bin");
     char *ph2 = build_path(path, "hits_h2.bin");
     if (!ph1 || !ph2) {
	  error1 = "building file paths";
	  goto on_error;
     }

     // We assume edges are given in more or less FROM order
     // The update will be:
     //   h2[FROM] += a1[TO]
     //   a2[TO] += h1[FROM]
     // This means that h1 and h2 are read/written in sequential order
     // and authorities in random order
     if (mmap_array_new(&p->h1, ph1, max_vertices, sizeof(float)) != 0) {
	  error1 = "building h1 mmap array";
	  error2 = p->h1? p->h1->error_msg: "NULL";
	  goto on_error;
     }
     if (mmap_array_new(&p->h2, ph2, max_vertices, sizeof(float)) != 0) {
	  error1 = "building h2 mmap array";
	  error2 = p->h2? p->h2->error_msg: "NULL";
	  goto on_error;
     }
     if (mmap_array_new(&p->a1, 0, max_vertices, sizeof(float)) != 0) {
	  error1 = "building a1 mmap array";
	  error2 = p->a1? p->a1->error_msg: "NULL";
	  goto on_error;
     }
     if (mmap_array_new(&p->a2, 0, max_vertices, sizeof(float)) != 0) {
	  error1 = "building a1 mmap array";
	  error2 = p->a2? p->a2->error_msg: "NULL";
	  goto on_error;
     }

     const float v0 = 1.0/(float)max_vertices;
     for (size_t i=0; i<max_vertices; ++i)
	  if (mmap_array_set(p->h1, i, &v0) != 0) {
	       error1 = "initializing h1";
	       error2 = p->h1->error_msg;
	       goto on_error;
	  }

     for (size_t i=0; i<max_vertices; ++i)
	  if (mmap_array_set(p->a1, i, &v0) != 0) {
	       error1 = "initializing a1";
	       error2 = p->a1->error_msg;
	       goto on_error;
	  }

     return 0;

on_error:
     hits_set_error(p, hits_error_internal, __func__);
     if (error1 != 0)
	  hits_add_error(p, error1);
     if (error2 != 0)
	  hits_add_error(p, error2);
     return p->error;
}

HitsError
hits_delete(Hits *hits) {
     char *error1 = 0;
     char *error2 = 0;

     if (mmap_array_delete(hits->h1) != 0) {
	  error1 = "deleting h1";
	  error2 = hits->h1->error_msg;
     } else if (mmap_array_delete(hits->h2) != 0) {
	  error1 = "deleting h2";
	  error2 = hits->h2->error_msg;
     } else if (mmap_array_delete(hits->a1) != 0) {
	  error1 = "deleting a1";
	  error2 = hits->a1->error_msg;
     } else if (mmap_array_delete(hits->a2) != 0) {
	  error1 = "deleting a2";
	  error2 = hits->a2->error_msg;
     } else {
	  free(hits);
	  return 0;
     }
     hits_set_error(hits, hits_error_internal, __func__);
     if (error1 != 0)
	  hits_add_error(hits, error1);
     if (error2 != 0)
	  hits_add_error(hits, error2);
     return hits->error;
}

static HitsError
hits_expand(Hits *hits) {
     char *error1 = 0;
     char *error2 = 0;

     if (mmap_array_resize(hits->h1, 2*hits->h1->n_elements) != 0) {
	  error1 = "resizing h1";
	  error2 = hits->h1->error_msg;
     } else if (mmap_array_resize(hits->h2, 2*hits->h2->n_elements) != 0) {
	  error1 = "resizing h1";
	  error2 = hits->h1->error_msg;
     } else if (mmap_array_resize(hits->a1, 2*hits->a1->n_elements) != 0) {
	  error1 = "resizing h1";
	  error2 = hits->h1->error_msg;
     } else if (mmap_array_resize(hits->a2, 2*hits->a2->n_elements) != 0) {
	  error1 = "resizing h1";
	  error2 = hits->h1->error_msg;
     } else {
	  return 0;
     }
     hits_set_error(hits, hits_error_internal, __func__);
     if (error1 != 0)
	  hits_add_error(hits, error1);
     if (error2 != 0)
	  hits_add_error(hits, error2);
     return hits->error;
}

HitsError
hits_set_n_pages(Hits *hits, size_t n_pages) {
     hits->n_pages = n_pages;
     if (n_pages > hits->h1->n_elements)
	  return hits_expand(hits);
     return 0;
}

static HitsError
hits_loop(Hits *hits,
	  void *link_stream_state,
	  LinkStreamNextFunc *link_stream_next
     ) {
     Link link;
     int end_stream = 0;

     if (mmap_array_advise(hits->h2, MADV_SEQUENTIAL) != 0)
	  return hits_error_internal;
     mmap_array_zero(hits->h2);
     if (mmap_array_advise(hits->a2, MADV_SEQUENTIAL) != 0)
	  return hits_error_internal;
     mmap_array_zero(hits->a2);

     do {
	  switch(link_stream_next(link_stream_state, &link)) {
	  case link_stream_state_init:
	       break;
	  case link_stream_state_error:
	       return hits_error_internal;
	  case link_stream_state_end:
	       end_stream = 1;
	       break;
	  case link_stream_state_next:
	       // check if mmap array should be expanded
	       if (link.from >= (int64_t)hits->n_pages)
		    if (hits_set_n_pages(hits, link.from + 1) != 0)
			 return hits_error_internal;
	       if (link.to >= (int64_t)hits->n_pages)
		    if (hits_set_n_pages(hits, link.to + 1) != 0)
			 return hits_error_internal;

	       // hub[i] = sum(auth[j]) for all j such that i->j
	       float *s2 = mmap_array_idx(hits->h2, link.from);
	       float *s1 = mmap_array_idx(hits->a1, link.to);
	       if (s1 && s2)
		    *s2 += *s1;
	       else
		    return hits_error_internal;

	       // auth[i] = sum(hub[j]) for all j such that j->i
	       s2 = mmap_array_idx(hits->a2, link.to);
	       s1 = mmap_array_idx(hits->h1, link.from);
	       if (s1 && s2)
		    *s2 += *s1;
	       else
		    return hits_error_internal;
	       break;
	  }
     } while (!end_stream);

     return 0;
}

static HitsError
hits_end_loop(Hits *hits, float *delta) {
     // Normalize output values and compute how much scores have changed (delta)
     char *error1 = 0;
     char *error2 = 0;

     float *score1;
     float *score2;

     *delta = 0.0;

     // get total hub score
     float hub_sum = 0.0;
     for (size_t i=0; i<hits->n_pages; ++i)
	  if (!(score2 = mmap_array_idx(hits->h2, i))) {
	       error1 = "accesing h2";
	       error2 = hits->h2->error_msg;
	       goto on_error;
	  } else {
	       hub_sum += *score2;
	  }
     // normalize score and get change
     for (size_t i=0; i<hits->n_pages; ++i)
	  if (!(score2 = mmap_array_idx(hits->h2, i))) {
	       error1 = "accesing h2";
	       error2 = hits->h2->error_msg;
	       goto on_error;
	  } else {
	       *score2 /= hub_sum;
	       if (!(score1 = mmap_array_idx(hits->h1, i))) {
		    error1 = "accesing h1";
		    error2 = hits->h1->error_msg;
		    goto on_error;
	       }
	       float diff = fabs(*score2 - *score1);
	       if (diff > *delta)
		    *delta = diff;
	       *score1 = *score2;
	  }

     // get total authority score
     float auth_sum = 0.0;
     for (size_t i=0; i<hits->n_pages; ++i)
	  if (!(score2 = mmap_array_idx(hits->a2, i))) {
	       error1 = "accesing a2";
	       error2 = hits->a2->error_msg;
	       goto on_error;
	  } else {
	       auth_sum += *score2;
	  }

     for (size_t i=0; i<hits->n_pages; ++i)
	  if (!(score2 = mmap_array_idx(hits->a2, i))) {
	       error1 = "accesing a2";
	       error2 = hits->a2->error_msg;
	       goto on_error;
	  } else {
	       *score2 /= auth_sum;
	       if (!(score1 = mmap_array_idx(hits->a1, i))) {
		    error1 = "accesing a1";
		    error2 = hits->a1->error_msg;
		    goto on_error;
	       }
	       float diff = fabs(*score2 - *score1);
	       if (diff > *delta)
		    *delta = diff;
	       *score1 = *score2;

	  }
     return 0;
on_error:
     hits_set_error(hits, hits_error_internal, __func__);
     if (error1 != 0)
	  hits_add_error(hits, error1);
     if (error2 != 0)
	  hits_add_error(hits, error2);
     return hits->error;
}

HitsError
hits_compute(Hits *hits,
	     void *link_stream_state,
	     LinkStreamNextFunc *link_stream_next,
	     LinkStreamResetFunc *link_stream_reset,
	     float precision) {
     HitsError rc = 0;

     float delta = precision + 1.0;
     size_t n_loops = 0;
     while (delta > precision) {
	  if ((rc = hits_loop(hits, link_stream_state, link_stream_next)) != 0)
	       return rc;
	  if (link_stream_reset(link_stream_state) == link_stream_state_error)
	       return hits_error_internal;
	  if ((rc = hits_end_loop(hits, &delta)) != 0)
	       return rc;

	  ++n_loops;
	  if (n_loops == hits->max_loops)
	       return hits_error_precision;
     }
     return 0;
}
