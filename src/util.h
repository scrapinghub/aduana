#ifndef __UTIL_H__
#define __UTIL_H__

#include <pthread.h>
#include "lmdb.h"

#define MAX_ERROR_LENGTH 10000

typedef enum {
     stream_state_init, /**< Stream ready */
     stream_state_next, /**< A new element has been obtained */
     stream_state_end,  /**< No more elements */
     stream_state_error /**< Unexpected error */
} StreamState;

typedef struct {
     /** Make operations on errors atomic. Errors in threading ops
         will be silently ignored */
     pthread_mutex_t mtx;

     int code;
     char message[MAX_ERROR_LENGTH + 1];
} Error;

void
error_init(Error *error);

void
error_destroy(Error *error);

void
error_set(Error* error, int code, const char *msg);

void
error_clean(Error *error);

void
error_add(Error* error, const char *msg);

char *
concat(const char *s1, const char *s2, char separator);

char *
build_path(const char *path, const char *fname);

char *
make_dir(const char *path);

#endif // __UTIL_H__
