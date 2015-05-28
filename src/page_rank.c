#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1

#ifdef __APPLE__
#define MADV_RANDOM 1
#define MADV_SEQUENTIAL 2
#define MADV_DONTNEED 4
#endif

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(_WIN32) 
#include "mman.h"
#else
#include <sys/mman.h>
#endif
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "mmap_array.h"

#include "page_rank.h"
#include "util.h"

static void
page_rank_set_error(PageRank *pr, int code, const char *message) {
     error_set(pr->error, code, message);

}

static void
page_rank_add_error(PageRank *pr, const char *message) {
     error_add(pr->error, message);
}

PageRankError
page_rank_new(PageRank **pr, const char *path, size_t max_vertices) {
     PageRank *p = *pr = malloc(sizeof(*p));
     if (!p)
          return page_rank_error_memory;
     if (!(p->error = error_new())) {
          free(p);
          return page_rank_error_memory;
     }

     p->n_pages = 0;
     p->damping = PAGE_RANK_DEFAULT_DAMPING;
     p->max_loops = PAGE_RANK_DEFAULT_MAX_LOOPS;
     p->persist = PAGE_RANK_DEFAULT_PERSIST;
     p->precision = PAGE_RANK_DEFAULT_PRECISION;
     p->scores = 0;

     char *error1 = 0;
     char *error2 = 0;
     p->path_out_degree = build_path(path, "pr_out_degree.bin");
     p->path_pr = build_path(path, "pr.bin");
     if (!p->path_out_degree || !p->path_pr) {
          error1 = "building file paths";
          goto on_error;
     }
     if (mmap_array_new(&p->out_degree, p->path_out_degree, max_vertices, sizeof(float)) != 0) {
          error1 = "building out_degree mmap array";
          error2 = p->out_degree? p->out_degree->error->message: "NULL";
          goto on_error;
     }
     if (mmap_array_new(&p->value1, p->path_pr, max_vertices, sizeof(float)) != 0) {
          error1 = "building value1 mmap array";
          error2 = p->value1? p->value1->error->message: "NULL";
          goto on_error;
     }
     if (mmap_array_new(&p->value2, 0, max_vertices, sizeof(float)) != 0) {
          error1 = "building value2 mmap array";
          error2 = p->value2? p->value2->error->message: "NULL";
          goto on_error;
     }

     if (mmap_array_advise(p->value1, MADV_SEQUENTIAL) != 0) {
          error1 = "value1";
          error2 = p->value1->error->message;
          goto on_error;
     }

     // Initialize value1 with equal PageRank score
     const float v0 = 1.0/(float)p->value1->n_elements;
     for (size_t i=0; i<p->value1->n_elements; ++i)
          if (mmap_array_set(p->value1, i, &v0) != 0) {
               error1 = "value1";
               error2 = p->value1->error->message;
               goto on_error;
          }

     if (mmap_array_advise(p->value1, MADV_DONTNEED) != 0) {
          error1 = "value1";
          error2 = p->value1->error->message;
          goto on_error;
     }

     return 0;

on_error:
     page_rank_set_error(p, page_rank_error_internal, __func__);
     page_rank_add_error(p, error1);
     page_rank_add_error(p, error2);
     return p->error->code;
}

PageRankError
page_rank_delete(PageRank *pr) {
     if (!pr)
          return 0;

     char *error1 = 0;
     char *error2 = 0;

     if (mmap_array_delete(pr->out_degree) != 0) {
          error1 = "deleting out_degree";
          error2 = pr->out_degree->error->message;
     } else if (mmap_array_delete(pr->value1) != 0) {
          error1 = "deleting value1";
          error2 = pr->value1->error->message;
     } else if (mmap_array_delete(pr->value2) != 0) {
          error1 = "deleting value2";
          error2 = pr->value2->error->message;
     } else {
          free(pr->path_out_degree);
          free(pr->path_pr);
          error_delete(pr->error);
          free(pr);
          return 0;
     }
     page_rank_set_error(pr, page_rank_error_internal, __func__);
     page_rank_add_error(pr, error1);
     page_rank_add_error(pr, error2);
     return pr->error->code;
}

static PageRankError
page_rank_expand(PageRank *pr) {
     char *error1 = 0;
     char *error2 = 0;

     if (mmap_array_resize(pr->out_degree, 2*pr->out_degree->n_elements) != 0) {
          error1 = "resizing out_degree";
          error2 = pr->out_degree->error->message;
     } else if (mmap_array_resize(pr->value1, 2*pr->value1->n_elements) != 0) {
          error1 = "resizing value1";
          error2 = pr->value1->error->message;
     } else if (mmap_array_resize(pr->value2, 2*pr->value2->n_elements) != 0) {
          error1 = "resizing value2";
          error2 = pr->value2->error->message;
     } else {
          return 0;
     }
     page_rank_set_error(pr, page_rank_error_internal, __func__);
     page_rank_add_error(pr, error1);
     page_rank_add_error(pr, error2);
     return pr->error->code;
}

PageRankError
page_rank_set_n_pages(PageRank *pr, size_t n_pages) {
     pr->n_pages = n_pages;
     if (n_pages > pr->out_degree->n_elements)
          return page_rank_expand(pr);
     return 0;
}

// 1. Expand the mmap array until is big enough
// 2. Compute the out degree
static PageRankError
page_rank_init(PageRank *pr,
               void *stream_state,
               LinkStreamNextFunc *link_stream_next) {

     char *error1 = 0;
     char *error2 = 0;

     if (mmap_array_advise(pr->out_degree, MADV_SEQUENTIAL) != 0) {
          error1 = "advising out_degree on sequential access";
          error2 = pr->out_degree->error->message;
          goto on_error;
     }
     mmap_array_zero(pr->out_degree);

     pr->total_score = 0.0;
     if (pr->scores)
          for (size_t i=0; i<pr->scores->n_elements; ++i) {
               float *score = mmap_array_idx(pr->scores, i);
               if (score)
                    pr->total_score += *score;
          }
     if (pr->total_score == 0)
          pr->total_score = 1.0;

     Link link;
     int end_stream = 0;
     do {
          switch(link_stream_next(stream_state, &link)) {
          case stream_state_init:
               break;
          case stream_state_error:
               return page_rank_error_internal;
          case stream_state_end:
               end_stream = 1;
               break;
          case stream_state_next:
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

     // Since its possible that the number of pages has changed, renormalize
     if (mmap_array_advise(pr->value1, MADV_SEQUENTIAL) != 0) {
          error1 = "advising out_degree on sequential access";
          error2 = pr->value1->error->message;
          goto on_error;
     }
     float sum = 0.0;
     for (size_t i=0; i<pr->n_pages; ++i) {
          float *score = mmap_array_idx(pr->value1, i);
          sum += *score;
     }
     for (size_t i=0; i<pr->n_pages; ++i) {
          float *score = mmap_array_idx(pr->value1, i);
          *score /= sum;
     }
     return 0;

on_error:
     page_rank_set_error(pr, page_rank_error_internal, __func__);
     page_rank_add_error(pr, error1);
     page_rank_add_error(pr, error2);
     return pr->error->code;
}

static PageRankError
page_rank_loop(PageRank *pr,
               void *stream_state,
               LinkStreamNextFunc *link_stream_next) {
     char *error1 = 0;
     char *error2 = 0;

     // Clear value2 scores
     if (mmap_array_advise(pr->value2, MADV_SEQUENTIAL) != 0) {
          error1 = "advising value2 on sequential access";
          error2 = pr->value2->error->message;
          goto on_error;
     }
     mmap_array_zero(pr->value2);

     if (mmap_array_advise(pr->value1, MADV_SEQUENTIAL) != 0) {
          error1 = "value1";
          error2 = pr->value1->error->message;
          goto on_error;
     }
     if (mmap_array_advise(pr->out_degree, MADV_SEQUENTIAL) != 0) {
          error1 = "out_degree";
          error2 = pr->out_degree->error->message;
          goto on_error;
     }
     if (mmap_array_advise(pr->value2, MADV_RANDOM) != 0) {
          error1 = "value2";
          error2 = pr->value2->error->message;
          goto on_error;
     }
     Link link;
     int end_stream = 0;
     float *value1;
     float *value2;
     float *degree;
     do {
          switch(link_stream_next(stream_state, &link)) {
          case stream_state_init:
               break;
          case stream_state_error:
               error1 = "getting next link";
               error2 = "stream error";
               goto on_error;
          case stream_state_end:
               end_stream = 1;
               break;
          case stream_state_next:
               degree = mmap_array_idx(pr->out_degree, link.from);
               value1 = mmap_array_idx(pr->value1, link.from);
               value2 = mmap_array_idx(pr->value2, link.to);
#if 0 // ignore links out of the known graph
               if (!value1 || !value2 || !degree) {
                    error1 = "indexing next link";
                    error2 =
                         value1 == 0? "value1":
                         value2 == 0? "value2": "degree";
                    goto on_error;
               }
#endif
               if (value1 && value2 && degree)
                    *value2 += pr->damping*(*value1)/(*degree);
               break;
          }
     } while (!end_stream);

     return 0;

on_error:
     page_rank_set_error(pr, page_rank_error_internal, __func__);
     page_rank_add_error(pr, error1);
     page_rank_add_error(pr, error2);
     return pr->error->code;
}

static PageRankError
page_rank_end_loop(PageRank *pr, float *delta) {
     char *error1 = 0;
     char *error2 = 0;

     float *score1;
     float *score2;

     if (mmap_array_advise(pr->value2, MADV_SEQUENTIAL) != 0) {
          error1 = "value2";
          error2 = pr->value2->error->message;
          goto on_error;
     }

     float rem = 0.0;
     for (size_t i=0; i<pr->n_pages; ++i) {
          float *score = mmap_array_idx(pr->value2, i);
          rem += *score;
     }
     rem = 1.0 - rem;

     if (!pr->scores) {
          float r = rem/pr->n_pages;
          for (size_t i=0; i<pr->n_pages; ++i) {
               float *score = mmap_array_idx(pr->value2, i);
               *score += r;
          }
     } else {
          for (size_t i=0; i<pr->n_pages; ++i) {
               float *r = mmap_array_idx(pr->scores, i);
               if (r) {
                    float *score = mmap_array_idx(pr->value2, i);
                    *score += rem*(*r)/pr->total_score;
               }
          }
     }

     *delta = 0.0;
     for (size_t i=0; i<pr->n_pages; ++i) {
          score1 = mmap_array_idx(pr->value1, i);
          score2 = mmap_array_idx(pr->value2, i);
          if (!score1 || !score2) {
               error1 = "accessing value1 and value";
               goto on_error;
          }
          float diff = fabs(*score2 - *score1);
          if (diff > *delta)
               *delta = diff;
          // swap scores, we want to retain the old score because it's needed
          // to stream over scores updates
          float tmp = *score1;
          *score1 = *score2;
          *score2 = tmp;
     }
     return 0;
on_error:
     page_rank_set_error(pr, page_rank_error_internal, __func__);
     page_rank_add_error(pr, error1);
     page_rank_add_error(pr, error2);
     return pr->error->code;
}

PageRankError
page_rank_compute(PageRank *pr,
                  void *stream_state,
                  LinkStreamNextFunc *link_stream_next,
                  LinkStreamResetFunc *link_stream_reset) {

     PageRankError rc = 0;

     if ((rc = page_rank_init(pr, stream_state, link_stream_next)) != 0)
          return rc;

     switch (link_stream_reset(stream_state)) {
     case stream_state_init:
          break;
     case stream_state_end:
          return 0;
     default:
          page_rank_set_error(pr, page_rank_error_internal, __func__);
          page_rank_add_error(pr, "resetting link stream");
          return pr->error->code;
     }

     float delta = pr->precision + 1.0;
     size_t n_loops = 0;
     while (delta > pr->precision) {
          rc = page_rank_loop(pr, stream_state, link_stream_next);
          if (rc != 0)
               return rc;
          if (link_stream_reset(stream_state) == stream_state_error) {
               page_rank_set_error(pr, page_rank_error_internal, __func__);
               page_rank_add_error(pr, "resetting link stream");
               return pr->error->code;
          }
          rc = page_rank_end_loop(pr, &delta);
          if (rc != 0)
               return rc;

          ++n_loops;
          if (n_loops == pr->max_loops) {
               page_rank_set_error(pr, page_rank_error_precision, __func__);
               page_rank_add_error(pr, "could not achieve precision");
               return pr->error->code;
          }
     }

     return 0;
}

PageRankError
page_rank_get(const PageRank *pr, size_t idx, float *score_old, float *score_new) {
     float *pr_score_new = mmap_array_idx(pr->value1, idx);
     float *pr_score_old = mmap_array_idx(pr->value2, idx);
     if (!pr_score_new || !pr_score_old)
          return page_rank_error_internal;
     *score_new = *pr_score_new;
     *score_old = *pr_score_old;
     return 0;
}

void
page_rank_set_persist(PageRank *pr, int value) {
     pr->persist = pr->out_degree->persist =
          pr->value1->persist = pr->value2->persist = value;
}

#if (defined TEST) && TEST
#include "test_page_rank.c"
#endif // TEST
