#ifndef __UTIL_H__
#define __UTIL_H__

#define MAX_ERROR_LENGTH 10000

typedef struct {
     int code;
     char message[MAX_ERROR_LENGTH + 1];
} Error;

void
error_set(Error* error, int code, const char *msg);

void
error_add(Error* error, const char *msg);

char *
concat(const char *s1, const char *s2, char separator);

char *
build_path(const char *path, const char *fname);

char *
make_dir(const char *path);

#endif // __UTIL_H__
