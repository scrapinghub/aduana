#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <errno.h>
#include <malloc.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include "lmdb.h"
#include "xxhash.h"

#include "pagedb.h"
#include "hits.h"
#include "page_rank.h"

char *
build_path(const char *path, const char *fname) {
     size_t plen = strlen(path);
     size_t flen = strlen(fname);
     char *fp = malloc(plen + flen + 2);
     if (fp) {
	  memcpy(fp, path, plen);
	  fp[plen] = '/';
	  memcpy(fp + plen + 1, fname, flen);
	  fp[plen + flen + 1] = '\0';
     }
     return fp;
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

/// @addtogroup PageInfo
/// @{
int
page_info_print(const PageInfo *pi, char *out) {
     size_t i = 0;
     strncpy(out, ctime(&pi->first_crawl), 24);
     i += 24;
     out[i++] = '|';                                      // i = 25
     strncpy(out + i, ctime(&pi->last_crawl), 24);
     i += 24;
     out[i++] = '|';                                      // i = 50
     if (snprintf(out + i, 9, "%.2e", (double)pi->n_crawls) < 0)
	  return -1;
     i += 8;
     out[i++] = '|';                                      // i = 59
     if (snprintf(out + i, 9, "%.2e", (double)pi->n_changes) < 0)
	  return -1;
     i += 8;
     out[i++] = '|';                                      // i = 68
     for (int j=0;
	  j<511 && pi->url[j] != '\0';
	  out[i++] = pi->url[j++]);                       // i <= 580
     out[i] = '\0';
     return i;
}

/** Create a new PageInfo from just an (uncrawled) link URL.
 *
 * @return A pointer to the new structure or NULL if failure
 */
static PageInfo *
page_info_new_link(const char *url, float score) {
     PageInfo *pi = calloc(1, sizeof(*pi));
     if (!pi)
	  return 0;

     if (!(pi->url = malloc(strlen(url) + 1))) {
	  free(pi);
	  return 0;
     }
     strcpy(pi->url, url);
     pi->score = score;
     return pi;
}

/** Create a new PageInfo from a crawled page.
 *
 * @return A pointer to the new structure or NULL if failure
 */
static PageInfo *
page_info_new_crawled(const CrawledPage *cp) {
     PageInfo *pi = page_info_new_link(cp->url, cp->score);
     if (!pi)
	  return 0;

     pi->first_crawl = pi->last_crawl = cp->time;
     pi->n_crawls = 1;
     pi->content_hash_length = cp->content_hash_length;
     if (!(pi->content_hash = malloc(pi->content_hash_length))) {
	  free(pi->url);
	  free(pi->content_hash);
	  free(pi);
	  return 0;
     }
     memcpy(pi->content_hash, cp->content_hash, cp->content_hash_length);
     return pi;
}

/** Update the PageInfo with a CrawledPage for the same page
 *
 * @return 0 if success, -1 if failure
 */
static int
page_info_update(PageInfo *pi, const CrawledPage *cp) {
     // Unmodified fields:
     //     url
     //     if hash length did not change
     //     (most probable use case is using fixed size hashes):
     //         content_hash_length
     //     if page did not change also:
     //         content_hash
     //         n_changes
     if (pi->content_hash_length != cp->content_hash_length) {
	  pi->content_hash = realloc(pi->content_hash,
				     cp->content_hash_length);
	  if (!pi->content_hash)
	       return -1;
	  pi->content_hash_length = cp->content_hash_length;
	  memcpy(pi->content_hash, cp->content_hash, cp->content_hash_length);
	  pi->n_changes++;
     } else {
	  for (size_t i=0; i<cp->content_hash_length; ++i)
	       if (pi->content_hash[i] != cp->content_hash[i]) {
		    for (size_t j=i; j<cp->content_hash_length; ++j)
			 pi->content_hash[j] = cp->content_hash[j];
		    pi->n_changes++;
		    break;
	       }
     }
     pi->n_crawls++;
     pi->last_crawl = cp->time;
     pi->score = cp->score;

     return 0;
}

/** Serialize the PageInfo into a contiguos block of memory.
 *
 * @param pi The PageInfo to be serialized
 * @param val The destination of the serialization
 *
 * @return 0 if success, -1 if failure.
 */
static int
page_info_dump(const PageInfo *pi, MDB_val *val) {
     size_t url_size = strlen(pi->url) + 1;
     val->mv_size =
	  sizeof(pi->first_crawl) + sizeof(pi->last_crawl) + sizeof(pi->n_changes) +
	  sizeof(pi->n_crawls) + sizeof(pi->score) + sizeof(pi->content_hash_length) + url_size +
	  pi->content_hash_length;
     char *data = val->mv_data = malloc(val->mv_size);
     if (!data)
	  return -1;

     size_t i = 0;
     size_t j;
     char * s;
#define PAGE_INFO_WRITE(x) for (j=0, s=(char*)&(x); j<sizeof(x); data[i++] = s[j++])
     PAGE_INFO_WRITE(pi->first_crawl);
     PAGE_INFO_WRITE(pi->last_crawl);
     PAGE_INFO_WRITE(pi->n_changes);
     PAGE_INFO_WRITE(pi->n_crawls);
     PAGE_INFO_WRITE(pi->score);
     PAGE_INFO_WRITE(pi->content_hash_length);
     for (j=0; j<url_size; data[i++] = pi->url[j++]);
     for (j=0; j<pi->content_hash_length; data[i++] = pi->content_hash[j++]);

     return 0;
}

/** Create a new PageInfo loading the information from a previously
 * dumped PageInfo inside val.
 *
 * @return pointer to the new PageInfo or NULL if failure
 */
static PageInfo *
page_info_load(const MDB_val *val) {
     PageInfo *pi = malloc(sizeof(*pi));
     char *data = val->mv_data;
     size_t i = 0;
     size_t j;
     char * d;
#define PAGE_INFO_READ(x) for (j=0, d=(char*)&(x); j<sizeof(x); d[j++] = data[i++])
     PAGE_INFO_READ(pi->first_crawl);
     PAGE_INFO_READ(pi->last_crawl);
     PAGE_INFO_READ(pi->n_changes);
     PAGE_INFO_READ(pi->n_crawls);
     PAGE_INFO_READ(pi->score);
     PAGE_INFO_READ(pi->content_hash_length);

     size_t url_size = strlen(data + i) + 1;
     if (!(pi->url = malloc(url_size))) {
	  free(pi);
	  return 0;
     }
     for (j=0; j<url_size; pi->url[j++] = data[i++]);

     if (!(pi->content_hash = malloc(pi->content_hash_length))) {
	  free(pi->url);
	  free(pi);
	  return 0;
     }
     for (j=0; j<pi->content_hash_length; pi->content_hash[j++] = data[i++]);

     return pi;
}

void
page_info_delete(PageInfo *pi) {
     if (pi != 0) {
	  free(pi->url);
	  free(pi->content_hash);
	  free(pi);
     }
}
/// @}

/// @addtogroup PageDB
/// @{

/** The "info" database stores a fixed amount of keys
 * this key points to the number of pages inside the database,
 * crawled or not. It is used to assign new IDs
 */
static char info_n_pages[] = "n_pages";

/** Order scores from greater to lower */
static int
mdb_cmp_score(const MDB_val *a, const MDB_val *b) {
     float x = *(float*)(a->mv_data);
     float y = *(float*)(b->mv_data);
     return x < y? +1: x > y? -1: 0;
}

static int
page_db_open_cursor(MDB_txn *txn,
		    const char *db_name,
		    int flags,
		    MDB_cursor **cursor,
		    MDB_cmp_func *func) {
     MDB_dbi dbi;
     int mdb_rc =
	  mdb_dbi_open(txn, db_name, flags, &dbi) ||
	  (func && mdb_set_compare(txn, dbi, func)) ||
	  mdb_cursor_open(txn, dbi, cursor);
     if (mdb_rc != 0)
	  *cursor = 0;
     return mdb_rc;
}

static int
page_db_open_hash2info(MDB_txn *txn, MDB_cursor **cursor) {
     return page_db_open_cursor(
	  txn, "hash2info", MDB_CREATE | MDB_INTEGERKEY, cursor, 0);
}

static int
page_db_open_hash2idx(MDB_txn *txn, MDB_cursor **cursor) {
     return page_db_open_cursor(
	  txn, "hash2idx", MDB_CREATE | MDB_INTEGERKEY, cursor, 0);
}

static int
page_db_open_links(MDB_txn *txn, MDB_cursor **cursor) {
     return page_db_open_cursor(
	  txn, "links", MDB_CREATE | MDB_INTEGERKEY, cursor, 0);
}

static int
page_db_open_info(MDB_txn *txn, MDB_cursor **cursor) {
     return page_db_open_cursor(txn, "info", 0, cursor, 0);
}

static int
page_db_open_schedule(MDB_txn *txn, MDB_cursor **cursor) {
     return page_db_open_cursor(
	  txn, "schedule", MDB_CREATE | MDB_DUPSORT, cursor, mdb_cmp_score);
}

/** Best First Search scheduler: add new page */
int
bfs_add(MDB_txn *txn, MDB_val *hash, PageInfo *pi) {
     int mdb_rc = 0;
     if (pi->n_crawls == 0) {
	  MDB_val key = {
	       .mv_size = sizeof(pi->score),
	       .mv_data = &pi->score
	  };
	  MDB_cursor *cur;
	  mdb_rc =
	       page_db_open_schedule(txn, &cur) ||
	       mdb_cursor_put(cur, &key, hash, 0);
     }
     return mdb_rc;
}

/** Best First Search scheduler: retrieve next page to crawl */
int
bfs_get(MDB_txn *txn, MDB_val *hash) {
     MDB_val key;  // stores the score
     MDB_val val1; // stores the hash
     MDB_val val2; // stores the PageInfo
     MDB_cursor *cur_schedule;
     MDB_cursor *cur_hash2info;

     hash->mv_size = 0;
     hash->mv_data = 0;

     int mdb_rc =
	  page_db_open_schedule(txn, &cur_schedule) ||
	  page_db_open_hash2info(txn, &cur_hash2info);

     int done = 0;
     while (!done) {
	  if ((mdb_rc = mdb_cursor_get(cur_schedule, &key, &val1, MDB_FIRST)) != 0)
	       goto exit;
	  if (val1.mv_size > hash->mv_size) {
	       if (!(hash->mv_data = realloc(hash->mv_data, val1.mv_size))) {
		    mdb_rc = -1; // TODO
		    goto exit;
	       }
	       hash->mv_size = val1.mv_size;
	  }
	  memcpy(hash->mv_data, val1.mv_data, val1.mv_size);
	  if ((mdb_rc = mdb_cursor_del(cur_schedule, 0)) != 0)
	       goto exit;

	  if ((mdb_rc = mdb_cursor_get(cur_hash2info, hash, &val2, MDB_SET)) != 0)
	       goto exit;
	  PageInfo *pi = page_info_load(&val2);
	  if (!pi)
	       goto exit;
	  done = pi->n_crawls == 0;
	  free(pi);
     }
exit:
     mdb_cursor_close(cur_schedule);
     mdb_cursor_close(cur_hash2info);
     return mdb_rc;
}

static void
page_db_set_error(PageDB *db, int error, const char *msg) {
     db->error = error;
     strncpy(db->error_msg, msg, PAGE_DB_MAX_ERROR_LENGTH);
}

static void
page_db_add_error_aux(PageDB *db, const char *msg) {
     (void)strncat(
	  db->error_msg,
	  msg,
	  PAGE_DB_MAX_ERROR_LENGTH -
	       strnlen(db->error_msg, PAGE_DB_MAX_ERROR_LENGTH));
}

static void
page_db_add_error(PageDB *db, const char *msg) {
    page_db_add_error_aux(db, ": ");
    page_db_add_error_aux(db, msg);
}

/** Doubles database size.
 *
 * This function is automatically called when an operation cannot proceed because
 * of insufficient allocated mmap memory.
 */
static PageDBError
page_db_grow(PageDB *db) {
     MDB_envinfo info;
     int mdb_rc =
	  mdb_env_info(db->env, &info) ||
	  mdb_env_set_mapsize(db->env, info.me_mapsize*2);
     if (mdb_rc != 0) {
	  page_db_set_error(db, page_db_error_internal, __func__);
	  page_db_add_error(db, mdb_strerror(mdb_rc));
     }
     return db->error;
}

PageDBError
page_db_new(PageDB **db, const char *path) {
     PageDB *p = *db = malloc(sizeof(*p));
     if (p == 0)
	  return page_db_error_memory;
     page_db_set_error(p, page_db_error_ok, "NO ERROR");
     p->error_msg[0] = '\0';
     p->sched_add = bfs_add;
     p->sched_get = bfs_get;

     const char *error = 0;
     // create directory if not present yet
     if (mkdir(path, 0775) != 0) {
	  if (errno != EEXIST)
	       error = strerror(errno);
	  else {
	       // path exists, check that is a valid directory
	       struct stat buf;
	       if (stat(path, &buf) != 0)
		    error = strerror(errno);
	       else if (!(buf.st_mode & S_IFDIR))
		    error = "existing path not a directory";
	  }
     }

     if (error != 0) {
	  page_db_set_error(p, page_db_error_invalid_path, __func__);
	  page_db_add_error(p, error);
	  return p->error;
     }

     // initialize LMDB on the directory
     MDB_txn *txn;
     MDB_dbi dbi;
     int mdb_rc = 0;
     if ((mdb_rc = mdb_env_create(&p->env) != 0))
	  error = "creating environment";
     else if ((mdb_rc = mdb_env_set_mapsize(p->env, PAGE_DB_DEFAULT_SIZE)) != 0)
	  error = "setting map size";
     else if ((mdb_rc = mdb_env_set_maxdbs(p->env, 5)) != 0)
	  error = "setting number of databases";
     else if ((mdb_rc = mdb_env_open(
		    p->env,
		    path,
		    MDB_NOTLS | MDB_WRITEMAP | MDB_MAPASYNC, 0664) != 0))
	  error = "opening environment";
     else if ((mdb_rc = mdb_txn_begin(p->env, 0, 0, &txn)) != 0)
	  error = "starting transaction";
     else if ((mdb_rc = mdb_dbi_open(txn, "info", MDB_CREATE, &dbi)) != 0)
	  error = "opening info database";
     else {
	  size_t n_pages = 0;
	  MDB_val key = {
	       .mv_size = sizeof(info_n_pages),
	       .mv_data = info_n_pages
	  };
	  MDB_val val = {
	       .mv_size = sizeof(size_t),
	       .mv_data = &n_pages
	  };
	  switch (mdb_rc = mdb_put(txn, dbi, &key, &val, MDB_NOOVERWRITE)) {
	  case MDB_KEYEXIST:
	  case 0:
	       if ((mdb_rc = mdb_txn_commit(txn)) != 0)
		    error = "could not commit n_pages";
	       break; // we good
	  default:
	       error = "could not initialize info.n_pages";
	  }
     }

     if (error != 0) {
	  page_db_set_error(p, page_db_error_internal, __func__);
	  page_db_add_error(p, error);
	  page_db_add_error(p, mdb_strerror(mdb_rc));

	  mdb_env_close(p->env);
     }

     return p->error;
}

/* TODO Move this comment to the benchmark.
   How to check if a page has been already been added to the database?
   We cannot add URL's directly as keys since we need to setup a maximum
   key size. We could make key sizes long enough, say, 3000 characters but
   I have a bad feeling about that.

   Insted, we hash all URLs using 64 bit hashes, even for huge crawls we get
   something only in the order of 2^32 different URLs, which is small enough
   to assume there are no collisions. Neverthless I did some tests using
   the URLs directly as keys. Note that timings also include the time to read the
   file, detect URL limits and compute whatever hash if necessary.

   Results with LMDB and XXHASH:

      1. String keys must be terminated with '\0', otherwise some keys are not retrieved!
      2. Some string keys fail to be retrieved, usually with weird characters inside
      2. Directly inserting the strings instead of hashes is MUCH faster (54M urls),
	 but query performance is better with hashes:
	 a. Average insert time:
	      string: 0.7e-6
	      hash  : 5.1e-6
	 b. Average query time:
	      string: 3.0e-6
	      hash  : 1.3e-6

    Results with hat-trie (https://github.com/dcjones/hat-trie):

      1. Timings:
	 a. Average insert time: 1.2e-6
	 b. Average query time : 1.3e-6

    Although hat-trie gives better results LMDB performance is very good and has much
    more features.
*/

/** Store a new or updated @ref PageInfo for a crawled page.
 *
 * @param cur An open cursor to the hash2info database
 * @param key The key (hash) to the page
 * @param page
 * @param mdb_error In case of failure, if the error occurs inside LMDB this output parameter
 *                  will be set with the error (otherwise is set to zero).
 * @return 0 if success, -1 if failure.
 */
static int
page_db_add_crawled_page_info(MDB_cursor *cur,
			      MDB_val *key,
			      const CrawledPage *page,
			      PageInfo **page_info,
			      int *mdb_error) {
     MDB_val val;
     PageInfo *pi = *page_info = 0;

     int mdb_rc = mdb_cursor_get(cur, key, &val, MDB_SET);
     int put_flags = 0;
     switch (mdb_rc) {
     case 0:
	  if (!(pi = page_info_load(&val)))
	       goto on_error;
	  if ((page_info_update(pi, page) != 0))
	       goto on_error;
	  put_flags = MDB_CURRENT;
	  break;
     case MDB_NOTFOUND:
	  if (!(pi = page_info_new_crawled(page)))
	      goto on_error;
	  break;
     default:
	  goto on_error;
     }

     if ((page_info_dump(pi, &val)   != 0))
	  goto on_error;

     if ((mdb_rc = mdb_cursor_put(cur, key, &val, put_flags)) != 0)
	  goto on_error;

     *mdb_error = 0;
     return 0;

on_error:
     *mdb_error = mdb_rc;
     page_info_delete(pi);
     return -1;
}

/** Store a new or updated @ref PageInfo for an uncrawled link.
 *
 * @param cur An open cursor to the hash2info database
 * @param key The key (hash) to the page
 * @param url
 * @param mdb_error In case of failure, if the error occurs inside LMDB this output parameter
 *                  will be set with the error (otherwise is set to zero).
 * @return 0 if success, -1 if failure.
 */
static int
page_db_add_link_page_info(MDB_cursor *cur,
			   MDB_val *key,
			   LinkInfo *link,
			   PageInfo **page_info,
			   int *mdb_error) {
     MDB_val val;
     int mdb_rc = 0;

     PageInfo *pi = *page_info = page_info_new_link(link->url, link->score);
     if (!pi)
	  goto on_error;

     if ((page_info_dump(pi, &val)   != 0))
	  goto on_error;

     if ((mdb_rc = mdb_cursor_put(cur, key, &val, MDB_NOOVERWRITE)) != 0)
	  goto on_error;

     *mdb_error = 0;
     return 0;
on_error:
     *mdb_error = mdb_rc;
     page_info_delete(pi);
     return -1;
}

PageDBError
page_db_add(PageDB *db, const CrawledPage *page) {
     MDB_txn *txn;

     MDB_cursor *cur_hash2info;
     MDB_cursor *cur_hash2idx;
     MDB_cursor *cur_links;
     MDB_cursor *cur_info;

     MDB_val key;
     MDB_val val;

     char *error;
     int mdb_rc;

     // we allow for the transaction to fail once. The reason is that if the
     // database grows past the initial allocated space attempting to add
     // more data will fail. Initially I tried to detect the MDB_MAP_FULL return
     // code but LMDB uses the allocated space to maintain also a freeDB for
     // its own purpose and the mdb_freelist_save gave an EPERMIT error when the
     // database was filled.
     int failed = 0;
txn_start: // return here if the transaction is discarded and must be repeated
     error = 0;
     // start a new write transaction
     if ((mdb_rc = mdb_txn_begin(db->env, 0, 0, &txn)) != 0)
	  error = "opening transaction";
     else if ((mdb_rc = page_db_open_hash2info(txn, &cur_hash2info)) != 0)
	  error = "opening hash2info cursor";
     else if ((mdb_rc = page_db_open_hash2idx(txn, &cur_hash2idx)) != 0)
	  error = "opening hash2idx cursor";
     else if ((mdb_rc = page_db_open_links(txn, &cur_links)) != 0)
	  error = "opening links cursor";
     else if ((mdb_rc = page_db_open_info(txn, &cur_info)) != 0)
	  error = "opening info cursor";

     if (error != 0)
	  goto on_error;

     // get n_pages
     key.mv_size = sizeof(info_n_pages);
     key.mv_data = info_n_pages;
     if ((mdb_rc = mdb_cursor_get(cur_info, &key, &val, MDB_SET)) != 0) {
	  error = "retrieving info.n_pages";
	  goto on_error;
     }
     size_t n_pages = *(size_t*)val.mv_data;


     uint64_t hash = XXH64(page->url, strlen(page->url), 0);
     key.mv_size = sizeof(uint64_t);
     key.mv_data = &hash;

     PageInfo *pi;
     if (page_db_add_crawled_page_info(cur_hash2info, &key, page, &pi, &mdb_rc) != 0) {
	  error = "adding/updating page info";
	  goto on_error;
     }
     if (pi != 0 && (mdb_rc = db->sched_add(txn, &key, pi) != 0)) {
	  error = "scheduling page";
	  goto on_error;
     }
     page_info_delete(pi);

     uint64_t *id = malloc((page->n_links + 1)*sizeof(*id));
     if (!id) {
	  error = "could not malloc";
	  goto on_error;
     }
     for (size_t i=0; i <= page->n_links; ++i) {
	  LinkInfo *link = i > 0? (page->links + i - 1): 0;
	  if (link) {
	       hash = XXH64(link->url, strlen(link->url), 0);
	       key.mv_size = sizeof(uint64_t);
	       key.mv_data = &hash;
	  }
	  val.mv_size = sizeof(uint64_t);
	  val.mv_data = &n_pages;

	  switch (mdb_rc = mdb_cursor_put(cur_hash2idx, &key, &val, MDB_NOOVERWRITE)) {
	  case MDB_KEYEXIST: // not really an error
	       id[i] = *(uint64_t*)val.mv_data;
	       break;
	  case 0:
	       id[i] = n_pages++;
	       if (link) {
		    if (page_db_add_link_page_info(cur_hash2info, &key, link, &pi, &mdb_rc) != 0) {
			 error = "adding/updating link info";
			 goto on_error;
		    }
		    if (pi != 0 && (mdb_rc = db->sched_add(txn, &key, pi) != 0)) {
			 error = "scheduling page";
			 goto on_error;
		    }
		    page_info_delete(pi);
	       }
	       break;
	  default:
	       goto on_error;
	  }
     }

     // store n_pages
     key.mv_size = sizeof(info_n_pages);
     key.mv_data = info_n_pages;
     val.mv_size = sizeof(size_t);
     val.mv_data = &n_pages;
     if ((mdb_rc = mdb_cursor_put(cur_info, &key, &val, 0)) != 0) {
	  error = "storing n_pages";
	  goto on_error;
     }

     // store links and commit transaction
     key.mv_size = sizeof(uint64_t);
     key.mv_data = id;
     val.mv_size = sizeof(uint64_t)*page->n_links;
     val.mv_data = id + 1;
     if ((mdb_rc = mdb_cursor_put(cur_links, &key, &val, 0)) != 0) {
	  error = "storing links";
	  goto on_error;
     }
     if ((mdb_rc = mdb_txn_commit(txn) != 0)) {
	  error = "commiting transaction";
	  goto on_error;
     }

     return db->error;
on_error:
     if (!failed) { // allow one failure
	  mdb_txn_abort(txn);
	  page_db_grow(db);
	  failed = 1;
	  goto txn_start;
     } else {
	  page_db_set_error(db, page_db_error_internal, __func__);
	  if (error != 0)
	       page_db_add_error(db, error);
	  if (mdb_rc != 0)
	       page_db_add_error(db, mdb_strerror(mdb_rc));

	  return db->error;
     }
}

PageDBError
page_db_get_info(PageDB *db, const char *url, PageInfo **pi) {
     MDB_txn *txn;
     MDB_cursor *cur;

     int mdb_rc;
     char *error = 0;
     if ((mdb_rc = mdb_txn_begin(db->env, 0, MDB_RDONLY, &txn)) != 0)
	  error = "opening transaction";
     // open hash2info database
     else if ((mdb_rc = page_db_open_hash2info(txn, &cur)) != 0) {
	  error = "opening hash2info database";
	  goto on_error;
     }
     else {
	  uint64_t hash = XXH64(url, strlen(url), 0);
	  MDB_val key = {
	       .mv_size = sizeof(uint64_t),
	       .mv_data = &hash
	  };
	  MDB_val val;
	  switch (mdb_rc = mdb_cursor_get(cur, &key, &val, MDB_SET)) {
	  case 0:
	       *pi = page_info_load(&val);
	       if (!pi) {
		    error = "deserializing data from database";
		    goto on_error;
	       }
	       break;
	  case MDB_NOTFOUND:
	       *pi = 0;
	       return 0;
	       break;
	  default:
	       error = "retrieving val from hash2info";
	       goto on_error;
	       break;
	  }
     }
     mdb_txn_abort(txn);
     return 0;

on_error:
     mdb_txn_abort(txn);
     page_db_set_error(db, page_db_error_internal, __func__);
     if (error != 0)
	  page_db_add_error(db, error);
     if (mdb_rc != 0)
	  page_db_add_error(db, mdb_strerror(mdb_rc));

     return db->error;
}

PageDBError
page_db_get_idx(PageDB *db, const char *url, uint64_t *idx) {
     MDB_txn *txn = 0;
     MDB_cursor *cur = 0;

     int mdb_rc;
     char *error = 0;
     if ((mdb_rc = mdb_txn_begin(db->env, 0, MDB_RDONLY, &txn)) != 0) {
	  error = "opening transaction";
	  goto on_error;
     // open hash2info database
     } else if ((mdb_rc = page_db_open_hash2idx(txn, &cur)) != 0) {
	  error = "opening hash2idx database";
	  goto on_error;
     }
     else {
	  uint64_t hash = XXH64(url, strlen(url), 0);
	  MDB_val key = {
	       .mv_size = sizeof(uint64_t),
	       .mv_data = &hash
	  };
	  MDB_val val;
	  switch (mdb_rc = mdb_cursor_get(cur, &key, &val, MDB_SET)) {
	  case 0:
	       *idx = *(uint64_t*)val.mv_data;
	       return 0;
	  case MDB_NOTFOUND:
	       *idx = 0;
	       return page_db_error_no_page;
	  default:
	       error = "retrieving val from hash2info";
	       goto on_error;
	       break;
	  }
     }

on_error:
     if (txn != 0)
	  mdb_txn_abort(txn);
     page_db_set_error(db, page_db_error_internal, __func__);
     if (error != 0)
	  page_db_add_error(db, error);
     if (mdb_rc != 0)
	  page_db_add_error(db, mdb_strerror(mdb_rc));

     return db->error;
}

PageDBError
page_db_request(PageDB *db, size_t n_pages, PageInfo **pi) {
     MDB_txn *txn;
     MDB_cursor *cur_hash2info;
     MDB_val hash = {0}; // page hash
     MDB_val val;        // dumped PageInfo

     int mdb_rc;
     char *error = 0;
     // getting pages from the schedule database may involve writing to the
     // database, we open the transaction in write mode
     if ((mdb_rc = mdb_txn_begin(db->env, 0, 0, &txn)) != 0)
	  error = "opening transaction";
     // open hash2info database
     else if ((mdb_rc = page_db_open_hash2info(txn, &cur_hash2info)) != 0) {
	  error = "opening hash2info database";
	  goto on_error;
     }
     else {
	  for (size_t i=0; i<n_pages; ++i)
	       switch (mdb_rc = db->sched_get(txn, &hash)) {
	       case 0:
		    switch (mdb_rc = mdb_cursor_get(cur_hash2info, &hash, &val, MDB_SET)) {
		    case 0:
			 pi[i] = page_info_load(&val);
			 if (!pi) {
			      error = "deserializing data from database";
			      goto on_error;
			 }
			 break;
		    default:
			 error = "retrieving val from hash2info";
			 goto on_error;
			 break;
		    }

		    break;
	       case MDB_NOTFOUND: // no more pages left
		    pi[i] = 0;
		    goto all_pages_retrieved;
	       default:
		    error = "retrieving page to schedule";
		    goto on_error;
	       }
     }

all_pages_retrieved:
     free(hash.mv_data);
     if ((mdb_rc = mdb_txn_commit(txn)) != 0) {
	  error = "commiting scheduler transaction";
	  goto on_error;
     }
     return 0;

on_error:
     free(hash.mv_data);
     page_db_set_error(db, page_db_error_internal, __func__);
     if (error != 0)
	  page_db_add_error(db, error);
     if (mdb_rc != 0)
	  page_db_add_error(db, mdb_strerror(mdb_rc));

     return db->error;
}

/** Close database */
void
page_db_delete(PageDB *db) {
     mdb_env_close(db->env);
     free(db);
}

/// @}


/// @addtogroup PageDBLinkStream
/// @{

static int
page_db_link_stream_copy_links(PageDBLinkStream *es, MDB_val *key, MDB_val *val) {
     es->n_to = val->mv_size/sizeof(uint64_t);
     es->i_to = 0;
     if (es->n_to > es->m_to) {
	  if ((es->to = (uint64_t*)realloc(es->to, es->n_to*sizeof(uint64_t))) == 0) {
	       es->state = link_stream_state_error;
	       return -1;
	  }
	  es->m_to = es->n_to;
     }
     es->from = *(uint64_t*)key->mv_data;

     uint64_t *links = val->mv_data;
     for (size_t i=0; i<es->n_to; ++i)
	  es->to[i] = links[i];

     return 0;
}

PageDBError
page_db_link_stream_new(PageDBLinkStream **es, PageDB *db) {
     PageDBLinkStream *p = *es = calloc(1, sizeof(*p));
     if (p == 0)
	  return page_db_error_memory;

     MDB_txn *txn;
     MDB_dbi dbi_links;

     int mdb_rc = 0;
     char *error = 0;

     // start a new read transaction
     if ((mdb_rc = mdb_txn_begin(db->env, 0, MDB_RDONLY, &txn)) != 0)
	  error = "opening transaction";
     // open cursor to links database
     else if ((mdb_rc = mdb_dbi_open(
		    txn,
		    "links",
		    MDB_CREATE | MDB_INTEGERKEY,
		    &dbi_links)) != 0)
	  error = "opening links database";
     else if ((mdb_rc = mdb_cursor_open(
		    txn,
		    dbi_links,
		    &p->cur)) != 0)
	  error = "opening links cursor";

     if (error != 0)
	  goto mdb_error;

     if (page_db_link_stream_reset(p) == link_stream_state_error) {
	  error = "reseting stream";
	  goto mdb_error;
     }

     return db->error;

mdb_error:
     page_db_set_error(db, page_db_error_internal, __func__);
     if (error != 0)
	  page_db_add_error(db, error);
     if (mdb_rc != 0)
	  page_db_add_error(db, mdb_strerror(mdb_rc));

     return db->error;
}

LinkStreamState
page_db_link_stream_reset(void *st) {
     PageDBLinkStream *es = st;
     int mdb_rc;
     MDB_val key;
     MDB_val val;

     switch (mdb_rc = mdb_cursor_get(es->cur, &key, &val, MDB_FIRST)) {
     case 0:
	  es->state = link_stream_state_init;
	  if (page_db_link_stream_copy_links(es, &key, &val) != 0)
	       return link_stream_state_error;
	  break;
     case MDB_NOTFOUND:
	  es->state = link_stream_state_end;
	  break;
     default:
	  es->state = link_stream_state_error;
	  break;
     }
     return es->state;
}

LinkStreamState
page_db_link_stream_next(void *st, Link *link) {
     PageDBLinkStream *es = st;
     while (es->i_to >= es->n_to) { // skip pages without links
	  int mdb_rc;
	  MDB_val key;
	  MDB_val val;

	  switch (mdb_rc = mdb_cursor_get(es->cur, &key, &val, MDB_NEXT)) {
	  case 0:
	       if (page_db_link_stream_copy_links(es, &key, &val) != 0)
		    return page_db_error_internal;
	       break;
	  case MDB_NOTFOUND:
	       return es->state = link_stream_state_end;
	  default:
	       return es->state = link_stream_state_error;
	  }
     }
     es->state = link_stream_state_next;
     link->from = es->from;
     link->to = es->to[es->i_to++];

     return es->state;
}

void
page_db_link_stream_delete(PageDBLinkStream *es) {
     mdb_txn_abort(mdb_cursor_txn(es->cur));
     free(es->to);
     free(es);
}
/// @}

#if TEST
#include "CuTest.h"

/* Tests the loading/dumping of PageInfo from and into LMDB values */
void
test_page_info_serialization(CuTest *tc) {
     MDB_val val;
     PageInfo pi1 = {
	  .url                 = "test_url_123",
	  .first_crawl         = 123,
	  .last_crawl          = 456,
	  .n_changes           = 100,
	  .n_crawls            = 20,
	  .score               = 0.7,
	  .content_hash_length = 8,
	  .content_hash        = "1234567"
     };

     CuAssertTrue(tc, page_info_dump(&pi1, &val) == 0);

     PageInfo *pi2 = page_info_load(&val);
     CuAssertPtrNotNull(tc, pi2);

     CuAssertStrEquals(tc, pi1.url, pi2->url);
     CuAssertTrue(tc, pi1.first_crawl == pi2->first_crawl);
     CuAssertTrue(tc, pi1.last_crawl == pi2->last_crawl);
     CuAssertTrue(tc, pi1.n_changes == pi2->n_changes);
     CuAssertTrue(tc, pi1.n_crawls == pi2->n_crawls);
     CuAssertTrue(tc, pi1.score == pi2->score);
     CuAssertTrue(tc, pi1.content_hash_length == pi2->content_hash_length);
     CuAssertStrEquals(tc, pi1.content_hash, pi2->content_hash);

     page_info_delete(pi2);
}

/* Tests all the database operations on a very simple crawl of just two pages */
void
test_page_db_simple(CuTest *tc) {
     char test_dir[] = "test-pagedb-XXXXXX";
     mkdtemp(test_dir);
     char data[] = "test-pagedb-XXXXXX/data.mdb";
     char lock[] = "test-pagedb-XXXXXX/lock.mdb";
     for (size_t i=0; test_dir[i] != 0; i++)
	  data[i] = lock[i] = test_dir[i];

     PageDB *db;
     CuAssert(tc,
	      db!=0? db->error_msg: "NULL",
	      page_db_new(&db, test_dir) == 0);

     uint64_t hash1 = 1000;
     uint64_t hash2 = 2000;
     LinkInfo cp1_links[] = {
	  {"a", 0.1}, {"b", 0.2}, {"www.google.com", 0.3}
     };
     CrawledPage cp1 = {
	  .url                 = "www.yahoo.com",
	  .links               = cp1_links,
	  .n_links             = 3,
	  .time                = time(0),
	  .score               = 0.5,
	  .content_hash        = (char*)&hash1,
	  .content_hash_length = sizeof(hash1)
     };
     LinkInfo cp2_links[] = {
	  {"x", 1.1}, {"y", 1.2}
     };
     CrawledPage cp2 = {
	  .url                 = "www.bing.com",
	  .links               = cp2_links,
	  .n_links             = 2,
	  .time                = time(0),
	  .score               = 0.2,
	  .content_hash        = (char*)&hash2,
	  .content_hash_length = sizeof(hash2)
     };

     CuAssert(tc,
	      db->error_msg,
	      page_db_add(db, &cp1) == 0);
     CuAssert(tc,
	      db->error_msg,
	      page_db_add(db, &cp2) == 0);

     cp2.time = time(0);
     hash2 = 3000;
     CuAssert(tc,
	      db->error_msg,
	      page_db_add(db, &cp2) == 0);

     char pi_out[1000];
     char *print_pages[] = {"www.yahoo.com", "www.google.com", "www.bing.com"};
     for (size_t i=0; i<3; ++i) {
	  PageInfo *pi;
	  CuAssert(tc,
		   db->error_msg,
		   page_db_get_info(db, print_pages[i], &pi) == 0);

	  CuAssertPtrNotNull(tc, pi);

	  switch(i) {
	  case 0:
	       CuAssertIntEquals(tc, 1, pi->n_crawls);
	       CuAssertIntEquals(tc, 0, pi->n_changes);
	       break;
	  case 1:
	       CuAssertIntEquals(tc, 0, pi->n_crawls);
	       break;
	  case 2:
	       CuAssertIntEquals(tc, 2, pi->n_crawls);
	       CuAssertIntEquals(tc, 1, pi->n_changes);
	       break;
	  }
	  page_info_print(pi, pi_out);
	  page_info_delete(pi);
/* show on screen the page info:
 *
 * Mon Apr  6 15:34:50 2015|Mon Apr  6 15:34:50 2015|1.00e+00|0.00e+00|www.yahoo.com
 * Thu Jan  1 01:00:00 1970|Thu Jan  1 01:00:00 1970|0.00e+00|0.00e+00|www.google.com
 * Mon Apr  6 15:34:50 2015|Mon Apr  6 15:34:50 2015|2.00e+00|1.00e+00|www.bing.com
 */
#if 0
	  printf("%s\n", pi_out);
#endif
     }

     PageDBLinkStream *es;
     CuAssert(tc,
	      db->error_msg,
	      page_db_link_stream_new(&es, db) == 0);

     if (es->state == link_stream_state_init) {
	  Link link;
	  int i=0;
	  while (page_db_link_stream_next(es, &link) == link_stream_state_next) {
	       switch(i++) {
	       case 0:
		    CuAssertIntEquals(tc, 0, link.from);
		    CuAssertIntEquals(tc, 1, link.to);
		    break;
	       case 1:
		    CuAssertIntEquals(tc, 0, link.from);
		    CuAssertIntEquals(tc, 2, link.to);
		    break;
	       case 2:
		    CuAssertIntEquals(tc, 0, link.from);
		    CuAssertIntEquals(tc, 3, link.to);
		    break;
	       case 3:
		    CuAssertIntEquals(tc, 4, link.from);
		    CuAssertIntEquals(tc, 5, link.to);
		    break;
	       case 4:
		    CuAssertIntEquals(tc, 4, link.from);
		    CuAssertIntEquals(tc, 6, link.to);
		    break;
	       default:
		    CuFail(tc, "too many links");
		    break;
	       }
	  }
	  CuAssertTrue(tc, es->state != link_stream_state_error);
     }
     page_db_link_stream_delete(es);

     page_db_delete(db);

     remove(data);
     remove(lock);
     remove(test_dir);
}

/* Tests the typical database operations on a moderate crawl of 10M pages */
void
test_page_db_large(CuTest *tc) {
     char test_dir[] = "test-pagedb-XXXXXX";
     mkdtemp(test_dir);
     char data[] = "test-pagedb-XXXXXX/data.mdb";
     char lock[] = "test-pagedb-XXXXXX/lock.mdb";
     char h1[] = "test-pagedb-XXXXXX/hits_h1.bin";
     char h2[] = "test-pagedb-XXXXXX/hits_h2.bin";
     for (size_t i=0; test_dir[i] != 0;  i++)
	  data[i] = lock[i] = h1[i] = h2[i] = test_dir[i];

     PageDB *db;
     CuAssert(tc,
	      db!=0? db->error_msg: "NULL",
	      page_db_new(&db, test_dir) == 0);

     const int n_links = 10;
     const int n_pages = 10000000;

     LinkInfo links[n_links + 1];
     for (int j=0; j<=n_links; ++j) {
	  sprintf(links[j].url = malloc(50), "test_url_%d", j);
	  links[j].score = j;
     }

     for (int i=0; i<n_pages; ++i) {
	  for (int j=0; j<n_links; ++j)
	       links[j] = links[j+1];
	  sprintf(links[n_links].url, "test_url_%d", i + n_links);
	  links[n_links].score = i;
#if 0
	  if (i % 100000 == 0)
	       printf("% 12d\n", i);
#endif
	  CrawledPage cp = {
	       .url                 = links[0].url,
	       .links               = links + 1,
	       .n_links             = n_links,
	       .time                = i,
	       .score               = 0.5,
	       .content_hash        = 0,
	       .content_hash_length = 0
	  };

	  CuAssert(tc,
		   db->error_msg,
		   page_db_add(db, &cp) == 0);
     }

     PageDBLinkStream *st;
     CuAssert(tc,
	      db->error_msg,
	      page_db_link_stream_new(&st, db) == 0);

     Hits *hits;
     CuAssert(tc,
	      hits!=0? hits->error_msg: "NULL",
	      hits_new(&hits, test_dir, n_pages) == 0);

     CuAssert(tc,
	      hits->error_msg,
	      hits_compute(hits,
			   st,
			   page_db_link_stream_next,
			   page_db_link_stream_reset) == 0);

     CuAssert(tc,
	      hits->error_msg,
	      hits_delete(hits) == 0);

     page_db_link_stream_delete(st);
     remove(h1);
     remove(h2);

     page_db_delete(db);
     remove(data);
     remove(lock);
     remove(test_dir);
}

/* Checks the accuracy of the HITS computation */
void
test_page_db_hits(CuTest *tc) {
     /* Compute the HITS score of the following graph
      *        +-->2---+
      *        |   |   |
      *        |   v   v
      *        1-->5<--3
      *        ^   ^   |
      *        |   |   |
      *        +---4<--+
      *
      * The link matrix L[i,j], where L[i,j] = 1 means 'i' links to 'j' is:
      *
      *        +-         -+
      *        | 0 1 0 0 1 |
      *        | 0 0 1 0 1 |
      *    L = | 0 0 0 1 1 |
      *        | 1 0 0 0 1 |
      *        | 0 0 0 0 0 |
      *        +-         -+
      */
     char test_dir[] = "test-pagedb-XXXXXX";
     mkdtemp(test_dir);
     char data[] = "test-pagedb-XXXXXX/data.mdb";
     char lock[] = "test-pagedb-XXXXXX/lock.mdb";
     char h1[] = "test-pagedb-XXXXXX/hits_h1.bin";
     char h2[] = "test-pagedb-XXXXXX/hits_h2.bin";
     for (size_t i=0; test_dir[i] != 0;  i++)
	  data[i] = lock[i] = h1[i] = h2[i] = test_dir[i];

     PageDB *db;
     CuAssert(tc,
	      db!=0? db->error_msg: "NULL",
	      page_db_new(&db, test_dir) == 0);

     char *urls[5] = {"1", "2", "3", "4", "5" };
     LinkInfo links_1[] = {{"2", 0.1}, {"5", 0.1}};
     LinkInfo links_2[] = {{"3", 0.1}, {"5", 0.1}};
     LinkInfo links_3[] = {{"4", 0.1}, {"5", 0.1}};
     LinkInfo links_4[] = {{"1", 0.1}, {"5", 0.1}};
     LinkInfo *links[5] = {
	  links_1, links_2, links_3, links_4, 0
     };
     int n_links[5] = {2, 2, 2, 2, 0};

     CrawledPage cp;
     for (int i=0; i<5; ++i) {
	  cp.url = urls[i];
	  cp.links = links[i];
	  cp.n_links = n_links[i];
	  cp.time = i;
	  cp.score = ((float)i)/5.0;
	  cp.content_hash = (char*)&i;
	  cp.content_hash_length = sizeof(i);

	  CuAssert(tc,
		   db->error_msg,
		   page_db_add(db, &cp) == 0);
     }

     PageDBLinkStream *st;
     CuAssert(tc,
	      db->error_msg,
	      page_db_link_stream_new(&st, db) == 0);

     Hits *hits;
     CuAssert(tc,
	      hits!=0? hits->error_msg: "NULL",
	      hits_new(&hits, test_dir, 5) == 0);

     CuAssert(tc,
	      hits->error_msg,
	      hits_compute(hits,
			   st,
			   page_db_link_stream_next,
			   page_db_link_stream_reset) == 0);
     page_db_link_stream_delete(st);

     uint64_t idx;
     float *h_score;
     float *a_score;
     float h_scores[5] = {0.250, 0.250, 0.250, 0.250, 0.000};
     float a_scores[5] = {0.125, 0.125, 0.125, 0.125, 0.500};

     for (int i=0; i<5; ++i) {
	  CuAssert(tc,
		   db->error_msg,
		   page_db_get_idx(db, urls[i], &idx) == 0);

	  CuAssertPtrNotNull(tc,
			     h_score = mmap_array_idx(hits->h1, idx));
	  CuAssertPtrNotNull(tc,
			     a_score = mmap_array_idx(hits->a1, idx));

	  CuAssertDblEquals(tc, h_scores[i], *h_score, 1e-6);
	  CuAssertDblEquals(tc, a_scores[i], *a_score, 1e-6);
     }
     CuAssert(tc,
	      hits->error_msg,
	      hits_delete(hits) == 0);
     remove(h1);
     remove(h2);

     page_db_delete(db);
     remove(data);
     remove(lock);
     remove(test_dir);
}

/* Checks the accuracy of the PageRank computation */
void
test_page_db_page_rank(CuTest *tc) {
     /* Compute the PageRank score of the following graph
      *        +-->2---+
      *        |   |   |
      *        |   v   v
      *        1-->5<--3
      *        ^   ^   |
      *        |   |   |
      *        +---4<--+
      *
      * The link matrix L[i,j], where L[i,j] = 1 means 'i' links to 'j' is:
      *
      *        +-         -+
      *        | 0 1 0 0 1 |
      *        | 0 0 1 0 1 |
      *    L = | 0 0 0 1 1 |
      *        | 1 0 0 0 1 |
      *        | 0 0 0 0 0 |
      *        +-         -+
      *
      * Since page "5" has no outbound links, it is assumed it links to every other page:
      *
      *        +-         -+
      *        | 0 1 0 0 1 |
      *        | 0 0 1 0 1 |
      *    L = | 0 0 0 1 1 |
      *        | 1 0 0 0 1 |
      *        | 1 1 1 1 1 |
      *        +-         -+
      *
      * The out degree is:
      *
      *    deg = {2, 2, 2, 2, 5}
      *
      * Dividing each row with the out degree and transposing we get the matrix:
      *
      *    M[i, j] = L[j, i]/deg[j]
      *
      * we get:
      *
      *        +-                   -+
      *        | 0   0   0   0.5 0.2 |
      *        | 0.5 0   0   0   0.2 |
      *    M = | 0   0.5 0   0   0.2 |
      *        | 0   0   0.5 0   0.2 |
      *        | 0.5 0.5 0.5 0.5 0.2 |
      *        +-                   -+
      *
      * If 'd' is the damping then the PageRank 'PR' is:
      *
      *        1 - d
      *   PR = ----- + (d * M)*PR
      *          N
      *
      * For d=0.85 the numerical solution is:
      *
      *   PR(1) = PR(2) = PR(3) = PR(4) = 0.15936255
      *   PR(5) = 0.3625498
      */
     char test_dir[] = "test-pagedb-XXXXXX";
     mkdtemp(test_dir);
     char data[] = "test-pagedb-XXXXXX/data.mdb";
     char lock[] = "test-pagedb-XXXXXX/lock.mdb";
     char out_degree[] = "test-pagedb-XXXXXX/pr_out_degree.bin";
     char pr_val[] = "test-pagedb-XXXXXX/pr.bin";
     for (size_t i=0; test_dir[i] != 0;  i++)
	  data[i] = lock[i] = out_degree[i] = pr_val[i] = test_dir[i];

     PageDB *db;
     CuAssert(tc,
	      db!=0? db->error_msg: "NULL",
	      page_db_new(&db, test_dir) == 0);

     char *urls[5] = {"1", "2", "3", "4", "5" };
     LinkInfo links_1[] = {{"2", 0.1}, {"5", 0.1}};
     LinkInfo links_2[] = {{"3", 0.1}, {"5", 0.1}};
     LinkInfo links_3[] = {{"4", 0.1}, {"5", 0.1}};
     LinkInfo links_4[] = {{"1", 0.1}, {"5", 0.1}};
     LinkInfo *links[5] = {
	  links_1, links_2, links_3, links_4, 0
     };

     int n_links[5] = {2, 2, 2, 2, 0};

     CrawledPage cp;
     for (int i=0; i<5; ++i) {
	  cp.url = urls[i];
	  cp.links = links[i];
	  cp.n_links = n_links[i];
	  cp.time = i;
	  cp.score = ((float)i)/5.0;
	  cp.content_hash = (char*)&i;
	  cp.content_hash_length = sizeof(i);

	  CuAssert(tc,
		   db->error_msg,
		   page_db_add(db, &cp) == 0);
     }

     PageDBLinkStream *st;
     CuAssert(tc,
	      db->error_msg,
	      page_db_link_stream_new(&st, db) == 0);

     PageRank *pr;
     CuAssert(tc,
	      pr!=0? pr->error_msg: "NULL",
	      page_rank_new(&pr, test_dir, 5, 0.85) == 0);

     CuAssert(tc,
	      pr->error_msg,
	      page_rank_compute(pr,
				st,
				page_db_link_stream_next,
				page_db_link_stream_reset) == 0);
     page_db_link_stream_delete(st);

     uint64_t idx;
     float *score;

     float scores[5] =  {0.15936255,  0.15936255,  0.15936255,  0.15936255,  0.3625498};
     for (int i=0; i<5; ++i) {
	  CuAssert(tc,
		   db->error_msg,
		   page_db_get_idx(db, urls[i], &idx) == 0);

	  CuAssertPtrNotNull(tc,
			     score = mmap_array_idx(pr->value1, idx));

	  CuAssertDblEquals(tc, scores[i], *score, 1e-6);
     }
     CuAssert(tc,
	      pr->error_msg,
	      page_rank_delete(pr) == 0);
     remove(out_degree);
     remove(pr_val);

     page_db_delete(db);
     remove(data);
     remove(lock);
     remove(test_dir);
}

void
test_page_db_requests(CuTest *tc) {
     char test_dir[] = "test-pagedb-XXXXXX";
     mkdtemp(test_dir);
     char data[] = "test-pagedb-XXXXXX/data.mdb";
     char lock[] = "test-pagedb-XXXXXX/lock.mdb";

     for (size_t i=0; test_dir[i] != 0;  i++)
	  data[i] = lock[i] = test_dir[i];

     PageDB *db;
     CuAssert(tc,
	      db!=0? db->error_msg: "NULL",
	      page_db_new(&db, test_dir) == 0);

     /* Make the link structure
      *
      *      0.0    1.0    0.1    0.5    0.4
      *   1 ---> 2 ---->4----->5------>8----->9
      *   |             |      |       |
      *   |      +------+   +--+--+    |0.2
      *   |      | 0.2   0.0|  0.5|    |
      *   | 0.1  v          v     v    |
      *   +----> 3          6     7<---+
      *
      */
     LinkInfo links_1[] = {
	  {.url = "2", .score = 0.0},
	  {.url = "3", .score = 0.1}
     };
     LinkInfo links_2[] = {
	  {.url = "4", .score = 1.0}
     };
     LinkInfo links_4[] = {
	  {.url = "3", .score = 0.2},
	  {.url = "5", .score = 0.1}
     };
     LinkInfo links_5[] = {
	  {.url = "6", .score = 0.0},
	  {.url = "7", .score = 0.5},
	  {.url = "8", .score = 0.5}
     };
     LinkInfo links_8[] = {
	  {.url = "7", .score = 0.2},
	  {.url = "9", .score = 0.4}
     };

     LinkInfo *links_7 = 0;

     LinkInfo *links[] = {
	  links_1, links_2, links_4, links_5, links_8, links_7
     };
     char *urls[6] = {"1", "2", "4", "5", "8", "7" };
     size_t n_links[6] = {2, 1, 2, 3, 2, 0};

     /* We have the following schedule after crawling pages 1, 2, 4, 5, 8 and 7
      * Note:
      *    - x: the page has been crawled
      *    - n: the link has not been added because it was already present
      *
      *     1           2           4           5           8           7
      * page score  page score  page score  page score  page score  page score
      * ---- -----  ---- -----  ---- -----  ---- -----  ---- -----  ---- -----
      * 3    0.1    4    1.0    4    1.0 x  4    1.0 x  4    1.0 x  4    1.0 x
      * 2    0.0    3    0.1    3    0.2 n  7    0.5    7    0.5    7    0.5 x
      *             2    0.0 x  3    0.1    8    0.5    8    0.5 x  8    0.5 x
      *                         5    0.1    3    0.2 n  9    0.4    9    0.4
      *                         2    0.0 x  3    0.1    3    0.2 n  3    0.2 n
      *                                     5    0.1 x  7    0.2 n  7    0.2 n
      *                                     6    0.0    3    0.1    3    0.1
					    2    0.0 x  5    0.1 x  5    0.1 x
      *                                                 6    0.0    6    0.0
      *                                                 2    0.0 x  2    0.0 x
      *
      */
     CrawledPage cp;
     for (int i=0; i<6; ++i) {
	  cp.url = urls[i];
	  cp.links = links[i];
	  cp.n_links = n_links[i];
	  cp.time = i;
	  cp.score = ((float)i)/5.0;
	  cp.content_hash = (char*)&i;
	  cp.content_hash_length = sizeof(i);

	  CuAssert(tc,
		   db->error_msg,
		   page_db_add(db, &cp) == 0);
     }

     /* Requests should return:
      *
      * page score
      * ---- -----
      * 9    0.4
      * 3    0.1
      * 6    0.0
      */
     PageInfo *pi[10] = {0};
     CuAssert(tc,
	      db->error_msg,
	      page_db_request(db, 2, pi) == 0);

     CuAssertStrEquals(tc, "9", pi[0]->url);
     CuAssertStrEquals(tc, "3", pi[1]->url);

     for (size_t i=0; i<10; ++i)
	  free(pi[i]);

     CuAssert(tc,
	      db->error_msg,
	      page_db_request(db, 4, pi) == 0);

     CuAssertStrEquals(tc, "6", pi[0]->url);
     CuAssert(tc, "too many requests returned", pi[1] == 0);

     for (size_t i=0; i<10; ++i)
	  free(pi[i]);

     page_db_delete(db);
     remove(data);
     remove(lock);
     remove(test_dir);
}

CuSuite *
test_page_db_suite(void) {
     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_page_info_serialization);
     SUITE_ADD_TEST(suite, test_page_db_simple);
     SUITE_ADD_TEST(suite, test_page_db_hits);
     SUITE_ADD_TEST(suite, test_page_db_page_rank);
     SUITE_ADD_TEST(suite, test_page_db_requests);
     //SUITE_ADD_TEST(suite, test_page_db_large);

     return suite;
}
#endif // TEST
