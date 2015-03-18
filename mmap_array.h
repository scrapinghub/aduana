#ifndef _MMAP_ARRAY_H
#define _MMAP_ARRAY_H

#define MMAP_ARRAY_MAX_ERROR_LENGTH 10000

typedef enum {
     mmap_array_error_ok = 0,
     mmap_array_error_internal,
     mmap_array_error_mmap,
     mmap_array_error_file,
     mmap_array_error_out_of_bounds,
     mmap_array_error_advise
       
} MMapError;

typedef struct {
     char *mem;
     int fd;
     size_t n_elements;
     size_t element_size;

     int error;
     char error_msg[MAX_ERROR_LENGTH+1];

     int exit_on_error;
} MMapArray;

MMapArray*
mmap_array_new_from_fd(int fd, size_t n_elements, size_t element_size);

MMapArray*
mmap_array_new_from_path(const char *path, size_t n_element, size_t element_size);

int
mmap_array_delete(MMapArray *marr);

void
mmap_array_exit_on_error(MMapArray *marr);

int
mmap_array_advise(MMapArray *marr, int flag);

int
mmap_array_sync(MMapArray *marr, int flag);

void *
mmap_array_idx(MMapArray *marr, size_t n);

int
mmap_array_set(MMapArray *marr, size_t n, const void *x);



#endif // _MMAP_ARRAY
