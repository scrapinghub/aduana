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

#include "mmap_array.h"
#include "pagedb.h"
#include "page_rank.h"

static void
page_rank_set_error(PageRank *pr, int error, const char *msg) {
     pr->error = error;
     strncpy(pr->error_msg, msg, PAGE_RANK_MAX_ERROR_LENGTH);
}

static void
page_rank_add_error_aux(PageRank *pr, const char *msg) {
     (void)strncat(
	  pr->error_msg,
	  msg,
	  PAGE_RANK_MAX_ERROR_LENGTH -
	       strnlen(pr->error_msg, PAGE_RANK_MAX_ERROR_LENGTH));
}

static void
page_rank_add_error(PageRank *pr, const char *msg) {
    page_rank_add_error_aux(pr, ": ");
    page_rank_add_error_aux(pr, msg);
}

PageRankError
page_rank_new(PageRank **pr, const char *path, size_t max_vertices, float damping) {
     PageRank *p = *pr = malloc(sizeof(*p));
     if (!p)
	  return page_rank_error_memory;

     p->damping = damping;
     p->n_pages = 0;
     p->initialized = 0;
     page_rank_set_error(p, page_rank_error_ok, "NO ERROR");

     char *error1 = 0;
     char *error2 = 0;
     char *p1 = build_path(path, "pr_out_degree.bin");
     char *p2 = build_path(path, "pr.bin");
     if (!p1 || !p2) {
	  error1 = "building file paths";
	  goto on_error;
     }
     if (mmap_array_new(&p->out_degree, p1, max_vertices, sizeof(float)) != 0) {
	  error1 = "building out_degree mmap array";
	  error2 = p->out_degree? p->out_degree->error_msg: "NULL";
	  goto on_error;
     }
     if (mmap_array_new(&p->value1, p2, max_vertices, sizeof(float)) != 0) {
	  error1 = "building value1 mmap array";
	  error2 = p->value1? p->value1->error_msg: "NULL";
	  goto on_error;
     }
     if (mmap_array_new(&p->value2, 0, max_vertices, sizeof(float)) != 0) {
	  error1 = "building value2 mmap array";
	  error2 = p->value2? p->value2->error_msg: "NULL";
	  goto on_error;
     }

     if (mmap_array_advise(p->value1, MADV_SEQUENTIAL) != 0) {
	  error1 = "value1";
	  error2 = p->value1->error_msg;
	  goto on_error;
     }
     const float v0 = 1.0/(float)p->value1->n_elements;
     for (size_t i=0; i<p->value1->n_elements; ++i)
	  if (mmap_array_set(p->value1, i, &v0) != 0) {
	       error1 = "value1";
	       error2 = p->value1->error_msg;
	       goto on_error;
	  }

     if (mmap_array_advise(p->value1, MADV_DONTNEED) != 0) {
	  error1 = "value1";
	  error2 = p->value1->error_msg;
	  goto on_error;
     }

     /* Probably unnecessary. ftruncate fills the file with the '\0' char.
	In turn, IEEE standard defines 0.0 with all bits 0 */
     if (mmap_array_advise(p->out_degree, MADV_SEQUENTIAL) != 0) {
	  error1 = "out_degree";
	  error2 = p->out_degree->error_msg;
	  goto on_error;
     }
     const float zero = 0.0;
     for (size_t i=0; i<p->out_degree->n_elements; ++i)
	  if (mmap_array_set(p->out_degree, i, &zero) != 0) {
	       error1 = "setting out_degree value";
	       error2 = p->out_degree->error_msg;
	  }

     if (mmap_array_advise(p->out_degree, MADV_DONTNEED) != 0) {
	  error1 = "out_degree";
	  error2 = p->out_degree->error_msg;
	  goto on_error;
     }

     return 0;

on_error:
     page_rank_set_error(p, page_rank_error_internal, __func__);
     if (error1 != 0)
	  page_rank_add_error(p, error1);
     if (error2 != 0)
	  page_rank_add_error(p, error2);
     return p->error;
}

PageRankError
page_rank_delete(PageRank *pr) {
     char *error1 = 0;
     char *error2 = 0;

     if (mmap_array_delete(pr->out_degree) != 0) {
	  error1 = "deleting out_degree";
	  error2 = pr->out_degree->error_msg;
     } else if (mmap_array_delete(pr->value1) != 0) {
	  error1 = "deleting value1";
	  error2 = pr->value1->error_msg;
     } else if (mmap_array_delete(pr->value2) != 0) {
	  error1 = "deleting value2";
	  error2 = pr->value2->error_msg;
     } else {
	  free(pr);
	  return 0;
     }
     page_rank_set_error(pr, page_rank_error_internal, __func__);
     if (error1 != 0)
	  page_rank_add_error(pr, error1);
     if (error2 != 0)
	  page_rank_add_error(pr, error2);
     return pr->error;
}

static PageRankError
page_rank_expand(PageRank *pr) {
     char *error1 = 0;
     char *error2 = 0;

     if (mmap_array_resize(pr->out_degree, 2*pr->out_degree->n_elements) != 0) {
	  error1 = "resizing out_degree";
	  error2 = pr->out_degree->error_msg;
     } else if (mmap_array_resize(pr->value1, 2*pr->value1->n_elements) != 0) {
	  error1 = "resizing value1";
	  error2 = pr->value1->error_msg;
     } else if (mmap_array_resize(pr->value2, 2*pr->value2->n_elements) != 0) {
	  error1 = "resizing value2";
	  error2 = pr->value2->error_msg;
     } else {
	  return 0;
     }
     page_rank_set_error(pr, page_rank_error_internal, __func__);
     if (error1 != 0)
	  page_rank_add_error(pr, error1);
     if (error2 != 0)
	  page_rank_add_error(pr, error2);
     return pr->error;
}

PageRankError
page_rank_set_n_pages(PageRank *pr, size_t n_pages) {
     pr->n_pages = n_pages;
     if (n_pages > pr->out_degree->n_elements)
	  return page_rank_expand(pr);
     return 0;
}

static PageRankError
page_rank_init(PageRank *pr,
	       void *link_stream_state,
	       LinkStreamNextFunc *link_stream_next) {

     char *error1 = 0;
     char *error2 = 0;

     if (mmap_array_advise(pr->out_degree, MADV_SEQUENTIAL) != 0) {
	  error1 = "resizing out_degree";
	  error2 = pr->out_degree->error_msg;
	  goto on_error;
     }

     Link link;
     int end_stream = 0;
     do {
	  switch(link_stream_next(link_stream_state, &link)) {
	  case link_stream_state_init:
	       break;
	  case link_stream_state_error:
	       return page_rank_error_internal;
	  case link_stream_state_end:
	       end_stream = 1;
	       break;
	  case link_stream_state_next:
	       if (link.from >= (int64_t)pr->n_pages)
		    if (page_rank_set_n_pages(pr, link.from + 1) != 0)
			 return page_rank_error_internal;
	       if (link.to >= (int64_t)pr->n_pages)
		    if (page_rank_set_n_pages(pr, link.to + 1) != 0)
			 return page_rank_error_internal;
	       float *deg = mmap_array_idx(pr->out_degree, link.from);
	       if (!deg)
		    return page_rank_error_internal;
	       ++(*deg);
	       break;
	  }
     } while (!end_stream);

     return 0;

on_error:
     page_rank_set_error(pr, page_rank_error_internal, __func__);
     if (error1 != 0)
	  page_rank_add_error(pr, error1);
     if (error2 != 0)
	  page_rank_add_error(pr, error2);
     return pr->error;
}

static PageRankError
page_rank_begin_loop(PageRank *pr) {
     char *error1 = 0;
     char *error2 = 0;
     if (mmap_array_advise(pr->out_degree, MADV_SEQUENTIAL) != 0) {
	  error1 = "out_degree";
	  error2 = pr->out_degree->error_msg;
	  goto on_error;
     }
     if (mmap_array_advise(pr->value1, MADV_SEQUENTIAL) != 0) {
	  error1 = "value1";
	  error2 = pr->value1->error_msg;
	  goto on_error;
     }

     float d = 0.0;
     for (size_t i=0; i<pr->n_pages; ++i) {
	  float *out_degree = mmap_array_idx(pr->out_degree, i);
	  if (!out_degree)
	       return page_rank_error_internal;

	  float *value1 = mmap_array_idx(pr->value1, i);
	  if (!value1)
	       return page_rank_error_internal;

	  if (*out_degree > 0)
	       d += (1.0 - pr->damping)*(*value1);
	  else
	       d += *value1;
     }
     d /= pr->n_pages;

     for (size_t i=0; i<pr->n_pages; ++i) {
	  float *out_degree = mmap_array_idx(pr->out_degree, i);
	  if (!out_degree)
	       return page_rank_error_internal;

	  if (*out_degree > 0) {
	       float *value1 = mmap_array_idx(pr->value1, i);
	       if (!value1)
		    return page_rank_error_internal;
	       *value1 *= pr->damping/(*out_degree);
	  }
     }

     if (mmap_array_advise(pr->out_degree, MADV_DONTNEED) != 0) {
	  error1 = "out_degree";
	  error2 = pr->out_degree->error_msg;
	  goto on_error;
     }
     if (mmap_array_advise(pr->value1, MADV_DONTNEED) != 0) {
	  error1 = "value1";
	  error2 = pr->value1->error_msg;
	  goto on_error;
     }
     if (mmap_array_advise(pr->value2, MADV_SEQUENTIAL) != 0) {
	  error1 = "value2";
	  error2 = pr->value2->error_msg;
	  goto on_error;
     }

     for (size_t i=0; i<pr->n_pages; ++i)
	  if (mmap_array_set(pr->value2, i, &d) != 0)
	       return page_rank_error_internal;

     return 0;

on_error:
     page_rank_set_error(pr, page_rank_error_internal, __func__);
     if (error1 != 0)
	  page_rank_add_error(pr, error1);
     if (error2 != 0)
	  page_rank_add_error(pr, error2);
     return pr->error;
}

static PageRankError
page_rank_loop(PageRank *pr,
	       void *link_stream_state,
	       LinkStreamNextFunc *link_stream_next) {
     char *error1 = 0;
     char *error2 = 0;

     if (mmap_array_advise(pr->value2, MADV_RANDOM) != 0) {
	  error1 = "value2";
	  error2 = pr->value2->error_msg;
	  goto on_error;
     }
     if (mmap_array_advise(pr->value1, MADV_SEQUENTIAL) != 0) {
	  error1 = "value1";
	  error2 = pr->value1->error_msg;
	  goto on_error;
     }

     Link link;
     int end_stream = 0;
     float *value1;
     float *value2;
     do {
	  switch(link_stream_next(link_stream_state, &link)) {
	  case link_stream_state_init:
	       break;
	  case link_stream_state_error:
	       return page_rank_error_internal;
	  case link_stream_state_end:
	       end_stream = 1;
	       break;
	  case link_stream_state_next:
	       value1 = mmap_array_idx(pr->value1, link.from);
	       value2 = mmap_array_idx(pr->value2, link.to);
	       if (!value1 || !value2)
		    return page_rank_error_internal;
	       *value2 += *value1;
	       break;
	  }
     } while (!end_stream);

     return 0;

on_error:
     page_rank_set_error(pr, page_rank_error_internal, __func__);
     if (error1 != 0)
	  page_rank_add_error(pr, error1);
     if (error2 != 0)
	  page_rank_add_error(pr, error2);
     return pr->error;
}

static void
page_rank_end_loop(PageRank *pr) {
     memcpy(pr->value1->mem,
	    pr->value2->mem,
	    pr->value2->n_elements*pr->value2->element_size);
}

PageRankError
page_rank_compute(PageRank *pr,
		  void *link_stream_state,
		  LinkStreamNextFunc *link_stream_next,
		  LinkStreamResetFunc *link_stream_reset) {

     PageRankError rc = 0;
     if (!pr->initialized) {
	  if ((rc = page_rank_init(pr, link_stream_state, link_stream_next)) != 0)
	       return rc;
	  if (link_stream_reset(link_stream_state) != 0)
	       return page_rank_error_internal;
	  pr->initialized = 1;
     }
     // TODO stop iteration when desired precision achieved
     for (int i=0; i<30; ++i) {
	  rc = page_rank_begin_loop(pr) ||
	       page_rank_loop(pr, link_stream_state, link_stream_next);
	  if (rc != 0)
	       return rc;
	  if (link_stream_reset(link_stream_state) == link_stream_state_error)
	       return page_rank_error_internal;
	  page_rank_end_loop(pr);
     }

     return 0;
}
