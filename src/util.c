#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <errno.h>
#include <malloc.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "util.h"

#ifdef _WIN32
#include <direct.h>
#endif

static void
error_set_1(Error* error, int code, const char *message) {
     error->code = code;
     strncpy(error->message, message? message: "no description", MAX_ERROR_LENGTH);
}

static void
error_set_2(Error* error, int code, const char *message) {
     if (pthread_mutex_lock(&error->mtx) != 0)
          return;
     error_set_1(error, code, message);
     (void)pthread_mutex_unlock(&error->mtx);
}

void
error_init(Error *error) {
     (void)pthread_mutex_init(&error->mtx, 0);
     error_set_1(error, 0, "NO ERROR");
}

void
error_destroy(Error *error) {
     (void)pthread_mutex_destroy(&error->mtx);
}

Error*
error_new(void) {
     Error *ret = malloc(sizeof(*ret));
     if (ret)
          error_init(ret);
     return ret;
}

void
error_delete(Error *error) {
     if (error) {
          error_destroy(error);
          free(error);
     }
}

void
error_clean(Error *error) {
     if (error)
          error_set_2(error, 0, "NO ERROR");
}

void
error_set(Error* error, int code, const char *message) {
     if (error && error->code == 0)
          error_set_2(error, code, message);
}

static void
error_add_aux(Error *error, const char *message) {
     (void)strncat(
          error->message,
          message,
          MAX_ERROR_LENGTH -
          strnlen(error->message, MAX_ERROR_LENGTH));
}

void
error_add(Error* error, const char *message) {
     if (error && message) {
          if (pthread_mutex_lock(&error->mtx) != 0)
               return;
          error_add_aux(error, ": ");
          error_add_aux(error, message);
          (void)pthread_mutex_unlock(&error->mtx);
     }
}

int
error_code(const Error* error) {
     return error->code;
}

char *
error_message(const Error *error) {
     return error_code(error)? error->message: 0;
}

char *
concat(const char *s1, const char *s2, char separator) {
     size_t len1 = strlen(s1);
     size_t len2 = strlen(s2);
     char *s3 = malloc(len1 + len2 + 2);
     if (s3) {
          memcpy(s3, s1, len1);
          s3[len1] = separator;
          memcpy(s3 + len1 + 1, s2, len2);
          s3[len1 + len2 + 1] = '\0';
     }
     return s3;
}

char *
build_path(const char *path, const char *fname) {
     return concat(path, fname, '/');
}

char *
make_dir(const char *path) {
#ifdef _WIN32
     if (_mkdir(path)) {
#else
     if (mkdir(path, 0775) != 0) {
#endif
          if (errno != EEXIST)
               return strerror(errno);
          else {
               // path exists, check that is a valid directory
               struct stat buf;
               if (stat(path, &buf) != 0)
                    return strerror(errno);
               else if (!(buf.st_mode & S_IFDIR))
                    return "existing path not a directory";
          }
     }
     return 0;
}

uint8_t*
varint_encode_uint64(uint64_t n, uint8_t *out) {
     do {
          *(out++) = (n & 0x7F) | 0x80;
     } while (n >>= 7);
     *(out - 1) &= 0x7F;
     return out;
}

uint64_t
varint_decode_uint64(uint8_t *in, uint8_t* read) {
     uint64_t res = 0;
     uint8_t b = 0;
     do {
          res |= (*in & 0x7F) << b;
          b += 7;
     } while (*(in++) & 0x80);

     if (read)
          *read = (b/7);
     return res;
}

uint8_t*
varint_encode_int64(int64_t n, uint8_t *out) {
     return varint_encode_uint64(n >= 0? 2*n: 2*abs(n) + 1, out);
}
int64_t
varint_decode_int64(uint8_t *in, uint8_t* read) {
     uint64_t res = varint_decode_uint64(in, read);
     if (res % 2 == 0)
          return res/2;
     else
          return -((res - 1)/2);
}

#ifdef _WIN32
#include <io.h>
void
mkdtemp(char *template) {
     _mktemp_s(template, strlen(template) + 1);
}
#endif

#if (defined TEST) && TEST
#include "test_util.c"
#endif // TEST
