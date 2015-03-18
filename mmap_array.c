#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1

#include <errno.h>
#include <fcntl.h>
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "mmap_array.h"

static void
mmap_array_set_error(MMapArray *marr, int error, const char *msg) {
     marr->error = error;
     strncpy(marr->error_msg, msg, MMAP_ARRAY_MAX_ERROR_LENGTH);     
}

static void
mmap_array_add_error(MMapArray *marr, const char *msg) {    
     (void)strncat(
	  marr->error_msg, 
	  msg, 
	  MMAP_ARRAY_MAX_ERROR_LENGTH - 
	       strnlen(marr->error_msg, MMAP_ARRAY_MAX_ERROR_LENGTH));
}

static void
mmap_array_set_error_out_of_bounds(MMapArray *marr, size_t n) {
     marr->error = mmap_array_error_out_of_bounds;
     if (snprintf(
	      marr->error_msg, 
	      MMAP_ARRAY_MAX_ERROR_LENGTH,
	      "Out of bounds error: %zu (max: %zu)",
	      n,
	      marr->n_elements) < 0) {
	  mmap_array_set_error(marr, mmap_array_error_internal, __func__);
     }
}

void
mmap_array_exit_on_error(MMapArray *marr) {
     marr->exit_on_error = 1;
     if (marr->error != mmap_array_error_ok) {
	  fputs(marr->error_msg, stderr);
	  exit(marr->error);
     }
}

static int
mmap_array_init(MMapArray *marr, int fd, size_t n_elements, size_t element_size) {
     marr->fd = fd;
     marr->n_elements = n_elements;
     marr->element_size = element_size;

     marr->error = mmap_array_error_ok;
     marr->error_msg[0] = '\0';

     marr->exit_on_error = 0;

     int flags = MAP_SHARED;
     if (fd == -1) {
	  flags |= MAP_ANONYMOUS;
     } else {
	  if (ftruncate(fd, n_elements*element_size) != 0) {
	       int error_cp = errno;
	       mmap_array_set_error(marr, mmap_array_error_file, __func__);
	       mmap_array_add_error(marr, ": ");	       
	       mmap_array_add_error(marr, strerror(error_cp));	       
	       return -1;
	  }
     }
     marr->mem = mmap(
	  0, n_elements*element_size, PROT_READ | PROT_WRITE, flags, fd, 0);

     if (marr->mem == MAP_FAILED) {
	  int errno_cp = errno;
	  mmap_array_set_error(marr, mmap_array_error_mmap, "Initializing mmap: ");
	  mmap_array_add_error(marr, strerror(errno_cp));
	  return -1;
     }

     return 0;
}

static int
mmap_array_close(MMapArray *marr) {
     if (munmap(marr->mem, marr->n_elements*marr->element_size) != 0) {
	  int errno_cp = errno;
	  mmap_array_set_error(marr, mmap_array_error_mmap, "Closing mmap: ");
	  mmap_array_add_error(marr, strerror(errno_cp));
	  if (marr->exit_on_error) {
	       mmap_array_exit_on_error(marr);
	  }
	  return -1;	  
     }     
     if (close(marr->fd) != 0) {
	  int errno_cp = errno;
	  mmap_array_set_error(marr, mmap_array_error_file, "Closing file descriptor: ");
	  mmap_array_add_error(marr, strerror(errno_cp));
	  if (marr->exit_on_error) {
	       mmap_array_exit_on_error(marr);
	  }
	  return -1;	  	  
     }
     return 0;
}


MMapArray*
mmap_array_new_from_fd(int fd, size_t n_elements, size_t element_size) {
     MMapArray *marr = (MMapArray*)malloc(sizeof(MMapArray));
     if (marr != 0) {
	  mmap_array_init(marr, fd, n_elements, element_size);
     } else if (marr->exit_on_error) {
	  mmap_array_exit_on_error(marr);     
     }
     return marr;
}

MMapArray*
mmap_array_new_from_path(const char *path, size_t n_element, size_t element_size) {
     int fd = -1;
     if (path != 0) {
	  fd = open(path, O_CREAT | O_RDWR, S_IREAD | S_IWRITE);
	  if (fd == -1) {
	       return 0;
	  }
     } 
     return mmap_array_new_from_fd(fd, n_element, element_size);
}

int
mmap_array_advise(MMapArray *marr, int flag) {
     if (madvise(marr->mem, marr->n_elements*marr->element_size, flag) != 0) {
	  int errno_cp = errno;
	  mmap_array_set_error(marr, mmap_array_error_advise, "madvise failed: ");
	  mmap_array_add_error(marr, strerror(errno_cp));
	  if (marr->exit_on_error) {
	       mmap_array_exit_on_error(marr);
	  }
	  return -1;
     }
     return 0;
}

int
mmap_array_sync(MMapArray *marr, int flag) {
     if (msync(marr->mem, marr->n_elements*marr->element_size, flag) != 0) {
	  int errno_cp = errno;
	  mmap_array_set_error(marr, mmap_array_error_advise, "msync failed: ");
	  mmap_array_add_error(marr, strerror(errno_cp));
	  if (marr->exit_on_error) {
	       mmap_array_exit_on_error(marr);
	  }
	  return -1;
     }
     return 0;
}

void *
mmap_array_idx(MMapArray *marr, size_t n) {
#ifndef DONT_CHECK_BOUNDS
     if (n >= marr->n_elements) {
	  mmap_array_set_error_out_of_bounds(marr, n);
	  if (marr->exit_on_error) {
	       mmap_array_exit_on_error(marr);
	  }
          return 0;
     }
#endif
     return (void*)(marr->mem + n*marr->element_size);
}

int
mmap_array_set(MMapArray *marr, size_t n, const void *x) {
     char *a = (char*)x;
     char *b = (char*)mmap_array_idx(marr, n);
     if (b == 0) {
	  if (marr->exit_on_error) {
	       mmap_array_exit_on_error(marr);
	  }
	  return -1;
     }
     for (size_t i=0; i<marr->element_size; ++i) {
	  b[i] = a[i];
     }
     return 0;
}

int
mmap_array_delete(MMapArray *marr) {
     if (mmap_array_close(marr) != 0) {
	  if (marr->exit_on_error) {
	       mmap_array_exit_on_error(marr);
	  }
	  return -1;
     }
     free(marr);
     return 0;
}
