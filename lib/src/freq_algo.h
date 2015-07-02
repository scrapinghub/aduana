#ifndef _FREQ_ALGO_H
#define _FREQ_ALGO_H

#include "page_db.h"
#include "mmap_array.h"

int
freq_algo_simple(PageDB *db, MMapArray **freqs, const char *path, char **error_msg);

#endif // _FREQ_ALGO_H
