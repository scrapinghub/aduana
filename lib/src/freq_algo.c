#include "freq_algo.h"
#include "page_db.h"
#include "freq_scheduler.h"
#include "mmap_array.h"

int
freq_algo_simple(PageDB *db, MMapArray **freqs, const char *path, char **error_msg) {
#define ERROR(msg) do {*error_msg = strdup(msg); return -1;} while (0)
     *error_msg = 0;

     if (mmap_array_new(freqs, path, 1, sizeof(PageFreq)) != 0)
	  ERROR(*freqs? strdup((*freqs)->error->message): "memory");
     MMapArray *pfreqs = *freqs;

     HashInfoStream *st;
     if (hashinfo_stream_new(&st, db) != 0)
	  ERROR(st? strdup(db->error->message): "memory");

     StreamState ss;
     uint64_t hash;
     PageInfo *pi;
     size_t n_pages = 0;
     while ((ss = hashinfo_stream_next(st, &hash, &pi)) == stream_state_next) {
	  if (pi->n_crawls >= 2) {
	       PageFreq pf = {
		    .hash = hash,
		    .freq = page_info_rate(pi)
	       };
	       if ((++n_pages >= pfreqs->n_elements) &&
		   (mmap_array_resize(pfreqs, 2*pfreqs->n_elements) != 0))
		    ERROR(pfreqs->error->message);

	       if (mmap_array_set(pfreqs, n_pages - 1, &pf) != 0)
		    ERROR(pfreqs->error->message);
	  }
	  page_info_delete(pi);
     }
     if (ss != stream_state_end)
	  ERROR("stream error");

     hashinfo_stream_delete(st);

     return 0;
}
