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

#define KB 1024LL
#define MB (1024*KB)
#define GB (1024*MB)

// The information that comes with a crawled page
typedef struct {
     char *url;
     char **links;
     size_t n_links;
     time_t time;
     char *content_hash;
     size_t content_hash_length;
} CrawledPage;

// The information we keep about crawled and uncrawled pages
typedef struct {
     char *url;
     time_t first_crawl;
     time_t last_crawl;
     size_t n_changes;
     size_t n_crawls;
     size_t content_hash_length;
     char *content_hash;
} PageInfo;


// Create a new PageInfo from just an (uncrawled) link
PageInfo *
page_info_new_link(const char *link) {
     PageInfo *pi = calloc(1, sizeof(*pi));
     if (!pi)
	  return 0;

     if (!(pi->url = malloc(strlen(link)))) {
	  free(pi);
	  return 0;
     }
     strcpy(pi->url, link);
     return pi;
}

// Create a new PageInfo from a crawled page
PageInfo *
page_info_new_crawled(CrawledPage *cp) {
     PageInfo *pi = page_info_new_link(cp->url);
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

// Update the PageInfo with a CrawledPage for the same page
int
page_info_update(PageInfo *pi, CrawledPage *cp) {
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

     return 0;
}

// Serialize the PageInfo into a contiguos block of memory. Store
// the block and its size inside val.
// Return 0 if success, -1 if failure.
int
page_info_dump(PageInfo *pi, MDB_val *val) {
     size_t url_size = strlen(pi->url) + 1;
     val->mv_size =
	  sizeof(pi->first_crawl) + sizeof(pi->last_crawl) + sizeof(pi->n_changes) +
	  sizeof(pi->n_crawls) + sizeof(pi->content_hash_length) + url_size +
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
     PAGE_INFO_WRITE(pi->content_hash_length);
     for (j=0; j<url_size; data[i++] = pi->url[j++]);
     for (j=0; j<pi->content_hash_length; data[i++] = pi->content_hash[j++]);

     return 0;
}

// Create a new PageInfo loading the information from a previously
// dumpled PageInfo inside val. Return pointer to the new PageInfo.
PageInfo *
page_info_load(MDB_val *val) {
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

// Destroy PageInfo
void
page_info_delete(PageInfo *pi) {
     if (pi != 0) {
	  free(pi->url);
	  free(pi->content_hash);
	  free(pi);
     }
}


// the "info" database stores a fixed amount of keys
// this key points to the number of pages inside the database,
// crawled or not. It is used to assign new IDs
char info_n_pages[] = "n_pages";

#define PAGE_DB_MAX_ERROR_LENGTH 10000
#define PAGE_DB_DEFAULT_SIZE 100*MB

typedef enum {
     page_db_error_ok = 0,
     page_db_error_memory,
     page_db_error_invalid_path,
     page_db_error_internal
} PageDBError;

typedef struct {
     MDB_env *env;

     PageDBError error;
     // a descriptive message associated with error
     char error_msg[PAGE_DB_MAX_ERROR_LENGTH+1];
} PageDB;

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

// Doubles database size
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

// Creates a new database and stores data inside path
PageDBError
page_db_new(PageDB **db, const char *path) {
     PageDB *p = *db = malloc(sizeof(*p));
     if (p == 0)
	  return page_db_error_memory;
     p->error = page_db_error_ok;
     p->error_msg[0] = '\0';

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
     else if ((mdb_rc = mdb_env_set_maxdbs(p->env, 4)) != 0)
	  error = "setting number of databases";
     else if ((mdb_rc = mdb_env_open(p->env, path, 0, 0664) != 0))
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

/* How to check if a page has been already been added to the database?
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
	      string: 7.1e-7
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

static int
page_db_add_crawled_page_info(MDB_cursor *cur, MDB_val *key, CrawledPage *page, int *mdb_error) {
     MDB_val val;
     PageInfo *pi = 0;

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
     page_info_delete(pi);
     return 0;

on_error:
     *mdb_error = mdb_rc;
     page_info_delete(pi);
     return -1;
}

static int
page_db_add_link_page_info(MDB_cursor *cur, MDB_val *key, char *url, int *mdb_error) {
     MDB_val val;
     int mdb_rc = 0;

     PageInfo *pi = page_info_new_link(url);
     if (!pi)
	  goto on_error;

     if ((page_info_dump(pi, &val)   != 0))
	  goto on_error;

     if ((mdb_rc = mdb_cursor_put(cur, key, &val, MDB_NOOVERWRITE)) != 0)
	  goto on_error;

     *mdb_error = 0;
     page_info_delete(pi);
     return 0;
on_error:
     *mdb_error = mdb_rc;
     page_info_delete(pi);
     return -1;
}

PageDBError
page_db_add(PageDB *db, CrawledPage *page) {
     MDB_txn *txn;
     MDB_dbi dbi_hash2info;
     MDB_dbi dbi_hash2idx;
     MDB_dbi dbi_links;
     MDB_dbi dbi_info;

     MDB_cursor *cur_hash2info;
     MDB_cursor *cur_hash2idx;
     MDB_cursor *cur_links;
     MDB_cursor *cur_info;

     MDB_val key;
     MDB_val val;

     char *error;
     int mdb_rc;

txn_start: // return here if the transaction is discarded and must be repeated
     error = 0;
     // start a new write transaction
     const int flags = MDB_CREATE | MDB_INTEGERKEY;
     if ((mdb_rc = mdb_txn_begin(db->env, 0, 0, &txn)) != 0)
	  error = "opening transaction";
     // open cursor to hash2info database
     else if ((mdb_rc = mdb_dbi_open(
		    txn,
		    "hash2info",
		    flags,
		    &dbi_hash2info)) != 0)
	  error = "opening hash2info database";
     else if ((mdb_rc = mdb_cursor_open(
		    txn,
		    dbi_hash2info,
		    &cur_hash2info)) != 0)
	  error = "opening hash2info cursor";
     // open cursor to hash2idx database
     else if ((mdb_rc = mdb_dbi_open(
		    txn,
		    "hash2idx",
		    flags,
		    &dbi_hash2idx)) != 0)
	  error = "opening hash2idx database";
     else if ((mdb_rc = mdb_cursor_open(
		    txn,
		    dbi_hash2idx,
		    &cur_hash2idx)) != 0)
	  error = "opening hash2idx cursor";
     // open cursor to links database
     else if ((mdb_rc = mdb_dbi_open(
		    txn,
		    "links",
		    flags,
		    &dbi_links)) != 0)
	  error = "opening links database";
     else if ((mdb_rc = mdb_cursor_open(
		    txn,
		    dbi_links,
		    &cur_links)) != 0)
	  error = "opening links cursor";
     // open cursor to info database
     else if ((mdb_rc = mdb_dbi_open(
		    txn,
		    "info",
		    0,
		    &dbi_info)) != 0)
	  error = "opening info database";
     else if ((mdb_rc = mdb_cursor_open(
		    txn,
		    dbi_info,
		    &cur_info)) != 0)
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
     if (page_db_add_crawled_page_info(cur_hash2info, &key, page, &mdb_rc) != 0) {
	  error = "adding/updating page info";
	  goto on_error;
     }

     uint64_t *id = malloc((page->n_links + 1)*sizeof(*id));
     for (size_t i=0; i <= page->n_links; ++i) {
	  char *link = i > 0? page->links[i - 1]: 0;
	  if (link) {
	       link = page->links[i - 1];
	       hash = XXH64(link, strlen(link), 0);
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
		    if (page_db_add_link_page_info(cur_hash2info, &key, link, &mdb_rc) != 0) {
			 error = "adding/updating link info";
			 goto on_error;
		    }
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
     mdb_rc =
	  mdb_cursor_put(cur_links, &key, &val, 0) ||
	  mdb_txn_commit(txn);
     if (mdb_rc != 0)
	  goto on_error;


     return db->error;
on_error:
     switch (mdb_rc) {
     case MDB_MAP_FULL:
	  // close transactions, resize, and try again
	  mdb_txn_abort(txn);
	  page_db_grow(db);
	  goto txn_start;

     case MDB_TXN_FULL: // TODO unlikely. Treat as error.
     default:
	  page_db_set_error(db, page_db_error_internal, __func__);
	  if (error != 0)
	       page_db_add_error(db, error);
	  if (mdb_rc != 0)
	       page_db_add_error(db, mdb_strerror(mdb_rc));

	  return db->error;
     }
}

typedef struct {
     int64_t from;
     int64_t to;
} Edge;

typedef enum {
     edge_stream_state_init,
     edge_stream_state_next,
     edge_stream_state_end,
     edge_stream_state_error
} EdgeStreamState;

typedef struct {
     MDB_cursor *cur;

     uint64_t from;
     uint64_t *to;
     size_t n_to;
     size_t i_to;
     size_t m_to;

     EdgeStreamState state;
} EdgeStream;

static int
edge_stream_copy_links(EdgeStream *es, MDB_val *key, MDB_val *val) {
     es->n_to = val->mv_size/sizeof(uint64_t);
     es->i_to = 0;
     if (es->n_to > es->m_to) {
	  if ((es->to = (uint64_t*)realloc(es->to, es->n_to*sizeof(uint64_t))) == 0) {
	       es->state = edge_stream_state_error;
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
edge_stream_new(EdgeStream **es, PageDB *db) {
     EdgeStream *p = *es = calloc(1, sizeof(*p));
     if (p == 0)
	  return page_db_error_memory;

     MDB_txn *txn;
     MDB_dbi dbi_links;
     MDB_val key;
     MDB_val val;

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

     switch (mdb_rc = mdb_cursor_get(p->cur, &key, &val, MDB_FIRST)) {
     case 0:
	  p->state = edge_stream_state_init;
	  if (edge_stream_copy_links(p, &key, &val) != 0)
	       return page_db_error_internal;
	  break;
     case MDB_NOTFOUND:
	  p->state = edge_stream_state_end;
	  break;
     default:
	  p->state = edge_stream_state_error;
	  goto mdb_error;
     }

     return db->error;

mdb_error:
     page_db_set_error(db, page_db_error_internal, __func__);
     if (error != 0)
	  page_db_add_error(db, error);
     page_db_add_error(db, mdb_strerror(mdb_rc));

     return db->error;
}

EdgeStreamState
edge_stream_next(EdgeStream *es, Edge *edge) {
     if (es->i_to == es->n_to) {
	  int mdb_rc;
	  MDB_val key;
	  MDB_val val;

	  switch (mdb_rc = mdb_cursor_get(es->cur, &key, &val, MDB_NEXT)) {
	  case 0:
	       if (edge_stream_copy_links(es, &key, &val) != 0)
		    return page_db_error_internal;
	       break;
	  case MDB_NOTFOUND:
	       return es->state = edge_stream_state_end;
	  default:
	       return es->state = edge_stream_state_error;
	  }
     }
     es->state = edge_stream_state_next;
     edge->from = es->from;
     edge->to = es->to[es->i_to++];

     return es->state;
}

void
edge_stream_delete(EdgeStream *es) {
     mdb_txn_abort(mdb_cursor_txn(es->cur));
     free(es->to);
     free(es);
}

int
main(void) {
     PageDB *db;
     PageDBError pdb_err = page_db_new(&db, "./test_pagedb");
     if (pdb_err != 0) {
	  printf("%d %s\n", pdb_err, db!=0? db->error_msg: "NULL");
	  exit(EXIT_FAILURE);
     }
     char *cp1_links[] = {"a", "b", "www.google.com"};
     CrawledPage cp1 = {
	  .url                 = "cp1",
	  .links               = cp1_links,
	  .n_links             = 3,
	  .time                = 0,
	  .content_hash        = 0,
	  .content_hash_length = 0
     };
     char *cp2_links[] = {"x", "y"};
     CrawledPage cp2 = {
	  .url                 = "cp2",
	  .links               = cp2_links,
	  .n_links             = 2,
	  .time                = 0,
	  .content_hash        = 0,
	  .content_hash_length = 0
     };

     if ((pdb_err = page_db_add(db, &cp1)) != 0) {
	  printf("%d %s\n", pdb_err, db->error_msg);
	  exit(EXIT_FAILURE);
     }

     if ((pdb_err = page_db_add(db, &cp2)) != 0) {
	  printf("%d %s\n", pdb_err, db->error_msg);
	  exit(EXIT_FAILURE);
     }

     EdgeStream *es;
     if ((pdb_err = edge_stream_new(&es, db)) != 0) {
	  printf("%d %s\n", pdb_err, db->error_msg);
	  exit(EXIT_FAILURE);
     }
     if (es->state == edge_stream_state_init) {
	  Edge edge;
	  while (edge_stream_next(es, &edge) == edge_stream_state_next) {
	       printf("%zu %zu\n", edge.from, edge.to);
	  }
     }
     edge_stream_delete(es);

     return 0;
}
