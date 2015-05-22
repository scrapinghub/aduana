#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <errno.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif
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

const char *
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

int
url_domain(const char *url, int *start, int *end) {
     //     +-- colon 1
     //     |      +-- colon 2                +-- slash 3
     //     v      v                          v
     // http://user:passwd@www.blabla.com:8080/foo#3
     //      ^^           |              ^
     //      |+-- slash 2 +-- at         +-- colon 3
     //      +-- slash 1
     int colon2 = -1;
     int colon3 = -1;
     int at = -1;
     int slash2 = -1;
     int slash3 = -1;

     int i = 0;

     // read "http[s]://" or fail
     if ((url[i++] != 'h') ||
         (url[i++] != 't') ||
         (url[i++] != 't') ||
         (url[i++] != 'p'))
          return -1;
     if (url[i] == 's')
          i++;
     if (url[i++] != ':' ||
         url[i++] != '/' ||
         url[i++] != '/')
          return -1;

     slash2 = i - 1;

     int n_colon = 1;
     for (; slash3 == -1; i++) {
          switch (url[i]) {
          case '\0':
               slash3 = i;
               break;
          case ':':
               switch (n_colon++) {
               case 1:
                    colon2 = i;
                    break;
               case 2:
                    if (at == -1)
                         return -1;
                    colon3 = i;
                    break;
               default:
                    return -1;
               }
               break;
          case '@':
               if (at == -1)
                    at = i;
               else
                    return -1;
               break;
          case '/':
               if (slash3 == -1)
                    slash3 = i;
               else
                    return -1;
          }
     }
     if (at == -1) {
          *start = slash2 + 1;
          *end = colon2 == -1? slash3 - 1: colon2 - 1;
     } else {
          *start = at + 1;
          *end = colon3 == -1? slash3 - 1: colon3 - 1;
     }
     return 0;
}

int
same_domain(const char *url1, const char *url2) {
     int s1, e1;
     int parse1 = url_domain(url1, &s1, &e1);
     int s2, e2;
     int parse2 = url_domain(url2, &s2, &e2);

     if (parse1 == 0) {
          if (parse2 == 0) {
               if ((e2 - s2) != (e1 - s1))
                    return 0;
               while ((s2 <= e2) && (s1 <= e1))
                    if (url1[s1++] != url2[s2++])
                         return 0;
               return 1;
          } else {
               return 0;
          }
     } else if (parse2 == 0) {
          return 0;
     } else {
          return strcmp(url1, url2) == 0? 1: 0;
     }
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
