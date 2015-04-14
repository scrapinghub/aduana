#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <errno.h>
#include <malloc.h>
#include <string.h>
#include <sys/stat.h>

#include "util.h"

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

// TODO delete?
/** Print hexadecimal representation of hash.

We assume that out has twice the length of the original hash to
print the hexadecimal representation.
*/
void
hash_print(char *hash, size_t len, char *out) {
     for (size_t i=0; i<len; ++i)
          sprintf(out + 2*i, "%02X", hash[i]);
}

char *
make_dir(const char *path) {
     if (mkdir(path, 0775) != 0) {
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
