#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#if defined(__APPLE__)
#define S_IREAD S_IRUSR
#define S_IWRITE S_IWUSR
#define S_IEXEC S_IXUSR

#define MREMAP_MAYMOVE 1
#define MAP_ANONYMOUS 0x1000
#endif

#include <errno.h>
#include <fcntl.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32) 
#include "mman.h"
#else
#include <sys/mman.h>
#endif
#include <sys/stat.h>
#include <unistd.h>

#include "mmap_array.h"
#include "util.h"


static void
mmap_array_set_error(MMapArray *marr, int code, const char *message) {
     error_set(marr->error, code, message);
}

static void
mmap_array_add_error(MMapArray *marr, const char *message) {
     error_add(marr->error, message);
}

static void
mmap_array_set_error_out_of_bounds(MMapArray *marr, size_t n) {
     marr->error->code = mmap_array_error_out_of_bounds;
     if (snprintf(
              marr->error->message,
              MAX_ERROR_LENGTH,
              "Out of bounds error: %zu (max: %zu)",
              n,
              marr->n_elements) < 0) {
          mmap_array_set_error(marr, mmap_array_error_internal, __func__);
     }
}

MMapArrayError
mmap_array_new(MMapArray **marr,
               const char *path,
               size_t n_elements,
               size_t element_size) {

     MMapArray *p = *marr = malloc(sizeof(*p));
     if (!p)
          return mmap_array_error_memory;
     if (!(p->error = error_new())) {
          free(p);
          return mmap_array_error_memory;
     }
     if (path) {
          if (!(p->path = strdup(path))) {
               free(p->error);
               free(p);
               return mmap_array_error_memory;
          }
     } else {
          p->path = 0;
     }
     // resizing works by doubling current size
     if (n_elements == 0)
          n_elements = 1;

     p->n_elements = n_elements;
     p->element_size = element_size;
     p->persist = MMAP_ARRAY_DEFAULT_PERSIST;

     char *error = 0;
     int errno_cp = 0;
     int error_code = 0;

     int mmap_flags;
     if (!path) {
          p->fd = -1;
          mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS;
     } else {
          if ((p->fd = open(path, O_CREAT | O_RDWR, S_IREAD | S_IWRITE)) == -1) {
               error_code = mmap_array_error_file;
               errno_cp = errno;
               error = "could not open path";
               goto on_error;
          }
          mmap_flags = MAP_SHARED;
          if (ftruncate(p->fd, n_elements*element_size) != 0) {
               error_code = mmap_array_error_file;
               errno_cp = errno;
               error = "file truncation failed";
               goto on_error;
          }
     }
     p->mem = mmap(
          0, n_elements*element_size, PROT_READ | PROT_WRITE, mmap_flags, p->fd, 0);
     if (p->mem == MAP_FAILED) {
          error_code = mmap_array_error_mmap;
          errno_cp = errno;
          error = "initializing mmap";
          goto on_error;
     }

     return 0;
on_error:
     mmap_array_set_error(p, error_code, __func__);
     if (error != 0)
          mmap_array_add_error(p, error);
     if (errno_cp != 0)
          mmap_array_add_error(p, strerror(errno_cp));
     return p->error->code;
}

MMapArrayError
mmap_array_advise(MMapArray *marr, int flag) {
     if (madvise(marr->mem, marr->n_elements*marr->element_size, flag) != 0) {
          int errno_cp = errno;
          mmap_array_set_error(marr, mmap_array_error_mmap, __func__);
          mmap_array_add_error(marr, strerror(errno_cp));
          return marr->error->code;
     }
     return 0;
}

MMapArrayError
mmap_array_sync(MMapArray *marr, int flag) {
     if (msync(marr->mem, marr->n_elements*marr->element_size, flag) != 0) {
          int errno_cp = errno;
          mmap_array_set_error(marr, mmap_array_error_mmap, __func__);
          mmap_array_add_error(marr, strerror(errno_cp));
     }
     return marr->error->code;
}

void *
mmap_array_idx(MMapArray *marr, size_t n) {
#ifndef DONT_CHECK_BOUNDS
     if (n >= marr->n_elements) {
          mmap_array_set_error_out_of_bounds(marr, n);
          return 0;
     }
#endif
     return (void*)(marr->mem + n*marr->element_size);
}

MMapArrayError
mmap_array_set(MMapArray *marr, size_t n, const void *x) {
     char *a = (char*)x;
     char *b = (char*)mmap_array_idx(marr, n);
     if (b)
          for (size_t i=0; i<marr->element_size; ++i)
               b[i] = a[i];

     return marr->error->code;
}

void
mmap_array_zero(MMapArray *marr) {
     memset(marr->mem, 0, marr->n_elements*marr->element_size);
}

MMapArrayError
mmap_array_delete(MMapArray *marr) {
     if (!marr)
          return 0;

     int errno_cp = 0;
     char *error = 0;
     if (munmap(marr->mem, marr->n_elements*marr->element_size) != 0) {
          errno_cp = errno;
          error = "munmap";
          goto on_error;
     }
     if (marr->fd != -1) {
          if (close(marr->fd) != 0) {
               errno_cp = errno;
               error = "closing file descriptor";
               goto on_error;
          }
          if (!marr->persist && (remove(marr->path) != 0)) {
               errno_cp = errno;
               error = "deleting associated file";
               goto on_error;
          }
     }

     free(marr->path);
     error_delete(marr->error);
     free(marr);
     return 0;

on_error:
     mmap_array_set_error(marr, mmap_array_error_mmap, __func__);
     if (error != 0)
          mmap_array_add_error(marr, error);
     if (errno_cp != 0)
          mmap_array_add_error(marr, strerror(errno_cp));
     return marr->error->code;
}

MMapArrayError
mmap_array_resize(MMapArray *marr, size_t n_elements) {
     const size_t new_size = n_elements*marr->element_size;
     const size_t old_size = marr->n_elements*marr->element_size;

     int error_code = 0;
     int errno_cp = 0;
     char *error = 0;

     if (marr->fd != -1 && ftruncate(marr->fd, new_size) !=0) {
          error_code = mmap_array_error_file;
          errno_cp = errno;
          error = "resizing file";
          goto on_error;
     }
#if defined(_WIN32) || defined(__APPLE__)
     if (munmap(marr->mem, old_size) != 0)
          marr->mem = MAP_FAILED;
     else
          marr->mem = mmap(
               0,
               new_size,
               PROT_READ | PROT_WRITE,
               marr->fd == -1? MAP_PRIVATE | MAP_ANONYMOUS: MAP_SHARED,
               marr->fd,
               0);
#else
     marr->mem = mremap(marr->mem, old_size, new_size, MREMAP_MAYMOVE);
#endif

     if (marr->mem == MAP_FAILED) {
          error_code = mmap_array_error_mmap;
          errno_cp = errno;
          error = "resizing mmap";
          goto on_error;
     }
     marr->n_elements = n_elements;
     return 0;

on_error:
     mmap_array_set_error(marr, error_code, __func__);
     mmap_array_add_error(marr, error);
     if (errno_cp != 0)
          mmap_array_add_error(marr, strerror(errno_cp));
     return marr->error->code;
}
