#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1

#ifdef __APPLE__
#define MADV_SEQUENTIAL 2
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
#include "hits.h"
#include "util.h"

static void
hits_set_error(Hits *hits, int code, const char *message) {
     error_set(hits->error, code, message);
}

static void
hits_add_error(Hits *hits, const char *message) {
     error_add(hits->error, message);
}

HitsError
hits_new(Hits **hits, const char *path, size_t max_vertices) {
     Hits *p = *hits = malloc(sizeof(*p));
     if (!p)
          return hits_error_memory;
     if (!(p->error = error_new())) {
          free(p);
          return hits_error_memory;
     }
     p->n_pages = 0;

     p->max_loops = HITS_DEFAULT_MAX_LOOPS;
     p->precision = HITS_DEFAULT_PRECISION;
     p->persist = HITS_DEFAULT_PERSIST;
     p->scores = 0;

     char *error1 = 0;
     char *error2 = 0;
     p->path_h1 = build_path(path, "hits_h1.bin");
     p->path_h2 = build_path(path, "hits_h2.bin");
     if (!p->path_h1 || !p->path_h2) {
          error1 = "building file paths";
          goto on_error;
     }

     // We assume edges are given in more or less FROM order
     // The update will be:
     //   h2[FROM] += a1[TO]
     //   a2[TO] += h1[FROM]
     // This means that h1 and h2 are read/written in sequential order
     // and authorities in random order
     if (mmap_array_new(&p->h1, p->path_h1, max_vertices, sizeof(float)) != 0) {
          error1 = "building h1 mmap array";
          error2 = p->h1? p->h1->error->message: "NULL";
          goto on_error;
     }
     if (mmap_array_new(&p->h2, p->path_h2, max_vertices, sizeof(float)) != 0) {
          error1 = "building h2 mmap array";
          error2 = p->h2? p->h2->error->message: "NULL";
          goto on_error;
     }
     if (mmap_array_new(&p->a1, 0, max_vertices, sizeof(float)) != 0) {
          error1 = "building a1 mmap array";
          error2 = p->a1? p->a1->error->message: "NULL";
          goto on_error;
     }
     if (mmap_array_new(&p->a2, 0, max_vertices, sizeof(float)) != 0) {
          error1 = "building a1 mmap array";
          error2 = p->a2? p->a2->error->message: "NULL";
          goto on_error;
     }

     const float v0 = 1.0/(float)max_vertices;
     for (size_t i=0; i<max_vertices; ++i)
          if (mmap_array_set(p->h1, i, &v0) != 0) {
               error1 = "initializing h1";
               error2 = p->h1->error->message;
               goto on_error;
          }

     for (size_t i=0; i<max_vertices; ++i)
          if (mmap_array_set(p->a1, i, &v0) != 0) {
               error1 = "initializing a1";
               error2 = p->a1->error->message;
               goto on_error;
          }

     return 0;

on_error:
     hits_set_error(p, hits_error_internal, __func__);
     hits_add_error(p, error1);
     hits_add_error(p, error2);
     return p->error->code;
}

HitsError
hits_delete(Hits *hits) {
     if (!hits)
          return 0;

     char *error1 = 0;
     char *error2 = 0;

     if (mmap_array_delete(hits->h1) != 0) {
          error1 = "deleting h1";
          error2 = hits->h1->error->message;
     } else if (mmap_array_delete(hits->h2) != 0) {
          error1 = "deleting h2";
          error2 = hits->h2->error->message;
     } else if (mmap_array_delete(hits->a1) != 0) {
          error1 = "deleting a1";
          error2 = hits->a1->error->message;
     } else if (mmap_array_delete(hits->a2) != 0) {
          error1 = "deleting a2";
          error2 = hits->a2->error->message;
     } else {
          free(hits->path_h1);
          free(hits->path_h2);
          error_delete(hits->error);
          free(hits);
          return 0;
     }
     hits_set_error(hits, hits_error_internal, __func__);
     hits_add_error(hits, error1);
     hits_add_error(hits, error2);
     return hits->error->code;
}

static HitsError
hits_expand(Hits *hits) {
     char *error1 = 0;
     char *error2 = 0;

     if (mmap_array_resize(hits->h1, 2*hits->h1->n_elements) != 0) {
          error1 = "resizing h1";
          error2 = hits->h1->error->message;
     } else if (mmap_array_resize(hits->h2, 2*hits->h2->n_elements) != 0) {
          error1 = "resizing h1";
          error2 = hits->h1->error->message;
     } else if (mmap_array_resize(hits->a1, 2*hits->a1->n_elements) != 0) {
          error1 = "resizing h1";
          error2 = hits->h1->error->message;
     } else if (mmap_array_resize(hits->a2, 2*hits->a2->n_elements) != 0) {
          error1 = "resizing h1";
          error2 = hits->h1->error->message;
     } else {
          return 0;
     }

     hits_set_error(hits, hits_error_internal, __func__);
     hits_add_error(hits, error1);
     hits_add_error(hits, error2);
     return hits->error->code;
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
          void *stream_state,
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
          switch(link_stream_next(stream_state, &link)) {
          case stream_state_init:
               break;
          case stream_state_error:
               return hits_error_internal;
          case stream_state_end:
               end_stream = 1;
               break;
          case stream_state_next:
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
               if (s1 && s2) {
                    if (hits->scores) {
                         float *score = mmap_array_idx(hits->scores, link.to);
                         if (score)
                              *s2 += (*score)*(*s1);
                    } else {
                         *s2 += *s1;
                    }
               }
#if 0 // ignore links out of the known graph
               else {
                    return hits_error_internal;
               }
#endif

               // auth[i] = sum(hub[j]) for all j such that j->i
               s2 = mmap_array_idx(hits->a2, link.to);
               s1 = mmap_array_idx(hits->h1, link.from);
               if (s1 && s2)
                    *s2 += *s1;
#if 0 // ignore links out of the known graph
               else
                    return hits_error_internal;
               break;
#endif
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
               error2 = hits->h2->error->message;
               goto on_error;
          } else {
               hub_sum += *score2;
          }
     // normalize score and get change
     for (size_t i=0; i<hits->n_pages; ++i)
          if (!(score2 = mmap_array_idx(hits->h2, i))) {
               error1 = "accesing h2";
               error2 = hits->h2->error->message;
               goto on_error;
          } else {
               *score2 /= hub_sum;
               if (!(score1 = mmap_array_idx(hits->h1, i))) {
                    error1 = "accesing h1";
                    error2 = hits->h1->error->message;
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

     // get total authority score
     float auth_sum = 0.0;
     for (size_t i=0; i<hits->n_pages; ++i)
          if (!(score2 = mmap_array_idx(hits->a2, i))) {
               error1 = "accesing a2";
               error2 = hits->a2->error->message;
               goto on_error;
          } else {
               auth_sum += *score2;
          }

     for (size_t i=0; i<hits->n_pages; ++i)
          if (!(score2 = mmap_array_idx(hits->a2, i))) {
               error1 = "accesing a2";
               error2 = hits->a2->error->message;
               goto on_error;
          } else {
               *score2 /= auth_sum;
               if (!(score1 = mmap_array_idx(hits->a1, i))) {
                    error1 = "accesing a1";
                    error2 = hits->a1->error->message;
                    goto on_error;
               }
               float diff = fabs(*score2 - *score1);
               if (diff > *delta)
                    *delta = diff;
               float tmp = *score1;
               *score1 = *score2;
               *score2 = tmp;
          }
     return 0;
on_error:
     hits_set_error(hits, hits_error_internal, __func__);
     hits_add_error(hits, error1);
     hits_add_error(hits, error2);
     return hits->error->code;
}

HitsError
hits_compute(Hits *hits,
             void *stream_state,
             LinkStreamNextFunc *link_stream_next,
             LinkStreamResetFunc *link_stream_reset) {
     HitsError rc = 0;

     float delta = hits->precision + 1.0;
     size_t n_loops = 0;
     while (delta > hits->precision) {
          if ((rc = hits_loop(hits, stream_state, link_stream_next)) != 0)
               return rc;
          if (link_stream_reset(stream_state) == stream_state_error)
               return hits_error_internal;
          if ((rc = hits_end_loop(hits, &delta)) != 0)
               return rc;

          ++n_loops;
          if (n_loops == hits->max_loops)
               return hits_error_precision;
     }
     return 0;
}

HitsError
hits_get_hub(const Hits *pr,
             size_t idx,
             float *score_old,
             float *score_new) {
     float *h_score_new = mmap_array_idx(pr->h1, idx);
     float *h_score_old = mmap_array_idx(pr->h2, idx);
     if (!h_score_new || !h_score_old)
          return hits_error_internal;
     *score_new = *h_score_new;
     *score_old = *h_score_old;
     return 0;
}

HitsError
hits_get_authority(const Hits *hits,
                   size_t idx,
                   float *score_old,
                   float *score_new) {
     float *a_score_new = mmap_array_idx(hits->a1, idx);
     float *a_score_old = mmap_array_idx(hits->a2, idx);
     if (!a_score_new || !a_score_old)
          return hits_error_internal;
     *score_new = *a_score_new;
     *score_old = *a_score_old;
     return 0;
}

void
hits_set_persist(Hits *hits, int value) {
     hits->persist = hits->h1->persist = hits->h2->persist =
          hits->a1->persist = hits->a2->persist = value;
}

#if (defined TEST) && TEST
#include "test_hits.c"
#endif // TEST
