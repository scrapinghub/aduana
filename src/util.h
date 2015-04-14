#ifndef __UTIL_H__
#define __UTIL_H__

char *
concat(const char *s1, const char *s2, char separator);

char *
build_path(const char *path, const char *fname);

char *
make_dir(const char *path);

#endif // __UTIL_H__
