#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <errno.h>
#include <limits.h>
#include <malloc.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include "lmdb.h"
#include "smaz.h"
#include "xxhash.h"

#include "page_db.h"
#include "hits.h"
#include "page_rank.h"
#include "txn_manager.h"
#include "util.h"

/** Allocate a new structure, with an initial default number of @ref LinkInfo slots*/
static PageLinks *
page_links_new(void) {
     PageLinks *pl = malloc(sizeof(*pl));
     if (!pl)
          return 0;

     pl->link_info = calloc(PAGE_LINKS_MIN_LINKS, sizeof(*pl->link_info));
     if (!pl->link_info) {
          free(pl);
          return 0;
     }
     pl->n_links = 0;
     pl->m_links = PAGE_LINKS_MIN_LINKS;
     return pl;
}

/** Delete structure and all @ref LinkInfo inside */
static void
page_links_delete(PageLinks *pl) {
     if (pl) {
          for (size_t i=0; i<pl->n_links; ++i)
               free(pl->link_info[i].url);
          free(pl->link_info);
          free(pl);
     }
}

/** Add a new @ref LinkInfo inside.
 *
 * Makes a copy of the URL.
 *
 * @return 0 if success, -1 if failure
 * */
static int
page_links_add_link(PageLinks *pl, const char *url, float score) {
     // Each time we hit the limit we double reserved space
     if (pl->n_links == pl->m_links) {
          void *p = realloc(pl->link_info, 2*pl->m_links);
          if (!p)
               return -1;
          pl->link_info = p;
          pl->m_links *= 2;
     }
     pl->link_info[pl->n_links].score = score;
     if (!(pl->link_info[pl->n_links].url = strdup(url)))
          return -1;

     pl->n_links++;
     return 0;
}

CrawledPage *
crawled_page_new(const char *url) {
     CrawledPage *cp = calloc(1, sizeof(*cp));
     if (!cp)
          return 0;

     cp->links = page_links_new();
     if (!cp->links) {
          free(cp);
          return 0;
     }

     if (!(cp->url = strdup(url))) {
          page_links_delete(cp->links);
          free(cp);
          return 0;
     }
     // By default we fill the time of the crawling with the current time,
     // which is the most common use case
     cp->time = difftime(time(0), 0);

     // Change manually if desired
     cp->score = 0.0;
     return cp;
}

void
crawled_page_delete(CrawledPage *cp) {
     if (cp) {
          free(cp->url);
          page_links_delete(cp->links);
          free(cp->content_hash);
          free(cp);
     }
}

int
crawled_page_set_hash(CrawledPage *cp, const char *hash, size_t hash_length) {
     if (cp->content_hash_length != hash_length) {
          if (!(cp->content_hash = realloc(cp->content_hash, hash_length)))
               return -1;
          cp->content_hash_length = hash_length;
     }
     memcpy(cp->content_hash, hash, hash_length);
     return 0;
}

int
crawled_page_set_hash128(CrawledPage *cp, char *hash) {
     return crawled_page_set_hash(cp, hash, 2*sizeof(uint64_t));
}

int
crawled_page_set_hash64(CrawledPage *cp, uint64_t hash) {
     return crawled_page_set_hash(cp, (char*)&hash, sizeof(hash));
}

int
crawled_page_set_hash32(CrawledPage *cp, uint32_t hash) {
     return crawled_page_set_hash(cp, (char*)&hash, sizeof(hash));
}

int
crawled_page_add_link(CrawledPage *cp, const char *url, float score) {
     return page_links_add_link(cp->links, url, score);
}

size_t
crawled_page_n_links(const CrawledPage *cp) {
     return cp->links->n_links;
}

const LinkInfo *
crawled_page_get_link(const CrawledPage *cp, size_t i) {
     return cp->links->link_info + i;
}


/// @addtogroup PageInfo
/// @{
int
page_info_print(const PageInfo *pi, char *out) {
     size_t i = 0;

     time_t first_crawl = (time_t)pi->first_crawl;
     strncpy(out, ctime(&first_crawl), 24);
     i += 24;
     out[i++] = '|';                                      // i = 25
     time_t last_crawl = (time_t)pi->last_crawl;
     strncpy(out + i, ctime(&last_crawl), 24);
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

     if (!(pi->url = strdup(url))) {
          free(pi);
          return 0;
     }

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
 * Note that enough new memory will be allocated inside val.mv_data to contain
 * the results of the dump. This memory should be freed when no longer is
 * necessary (for example after an mdb_cursor_put).
 *
 * @param pi The PageInfo to be serialized
 * @param val The destination of the serialization. Should have no memory
 *            allocated inside mv_data since new memory will be allocated.
 *
 * @return 0 if success, -1 if failure.
 */
static int
page_info_dump(const PageInfo *pi, MDB_val *val) {
     /* To save space we apply the following 'compression' method:
        1. If n_crawls > 1 all data is saved
        2. If n_crawls = 1 we have the following constraints:
               last_crawl = first_crawl
               n_changes  = 0
           And so we don't bother to store last_crawl and n_changes
        3. If n_crawls = 0, which is the very common case of an uncrawled page:
               first_crawl         = 0
               last_crawl          = 0
               n_changes           = 0
               content_hash_length = 0
               content_hash        = NULL
     */

     // necessary size except for the URL, which we don't know yet how much is going
     // to cost storing
     val->mv_size = sizeof(pi->score) + sizeof(pi->n_crawls);
     if (pi->n_crawls > 0) {
          val->mv_size += sizeof(pi->first_crawl) +
               pi->content_hash_length + sizeof(pi->content_hash_length);
          if (pi->n_crawls > 1)
               val->mv_size += sizeof(pi->last_crawl) + sizeof(pi->n_changes);
     }

     size_t url_size = strlen(pi->url);
     char *data = val->mv_data = malloc(val->mv_size + 4*url_size);
     if (!data)
          return -1;

     size_t curl_size = (size_t)smaz_compress(
          pi->url, url_size, data + sizeof(unsigned short), 4*url_size);
     if (curl_size > 4*url_size) // TODO
          return -1;
     ((unsigned short*)data)[0] = curl_size;
     val->mv_size += sizeof(unsigned short) + curl_size;


     size_t i = sizeof(unsigned short) + curl_size;
     size_t j;
     char * s;
#define PAGE_INFO_WRITE(x) for (j=0, s=(char*)&(x); j<sizeof(x); data[i++] = s[j++])
     PAGE_INFO_WRITE(pi->score);
     PAGE_INFO_WRITE(pi->n_crawls);
     if (pi->n_crawls > 0) {
          PAGE_INFO_WRITE(pi->first_crawl);
          if (pi->n_crawls > 1) {
               PAGE_INFO_WRITE(pi->last_crawl);
               PAGE_INFO_WRITE(pi->n_changes);
          }
          PAGE_INFO_WRITE(pi->content_hash_length);
          for (j=0; j<pi->content_hash_length; data[i++] = pi->content_hash[j++]);
     }

     return 0;
}

// fast retrieval of just the score associated with the page
static float
page_info_dump_get_score(MDB_val *val) {
     char *data = val->mv_data;
     unsigned short curl_size = ((unsigned short*)data)[0];
     data += sizeof(unsigned short) + curl_size;
     return *((float*)data);
}

/** Create a new PageInfo loading the information from a previously
 * dumped PageInfo inside val.
 *
 * @return pointer to the new PageInfo or NULL if failure
 */
static PageInfo *
page_info_load(const MDB_val *val) {
     PageInfo *pi = calloc(1, sizeof(*pi));
     char *data = val->mv_data;
     size_t i = 0;
     size_t j;
     char * d;
#define PAGE_INFO_READ(x) for (j=0, d=(char*)&(x); j<sizeof(x); d[j++] = data[i++])
     unsigned short curl_size;
     PAGE_INFO_READ(curl_size);

     unsigned short url_size = 4*curl_size + 1;
     int enough_memory = 0;
     do {
          if (!(pi->url = realloc(pi->url, url_size))) {
               free(pi);
               return 0;
          }
          int dec = smaz_decompress(data + i, curl_size, pi->url, url_size);
          if ((unsigned int)dec < url_size) {
               enough_memory = 1;
               url_size = dec;
          } else {
               url_size *= 2;
          }
     } while (!enough_memory);
     pi->url[url_size] = '\0';
     i += curl_size;

     PAGE_INFO_READ(pi->score);
     PAGE_INFO_READ(pi->n_crawls);
     if (pi->n_crawls > 0) {
          PAGE_INFO_READ(pi->first_crawl);
          if (pi->n_crawls > 1) {
               PAGE_INFO_READ(pi->last_crawl);
               PAGE_INFO_READ(pi->n_changes);
          } else {
               pi->last_crawl = pi->first_crawl;
          }
          PAGE_INFO_READ(pi->content_hash_length);

          if (!(pi->content_hash = malloc(pi->content_hash_length))) {
               free(pi->url);
               free(pi);
               return 0;
          }
          for (j=0; j<pi->content_hash_length; pi->content_hash[j++] = data[i++]);
     }
     return pi;
}

float
page_info_rate(const PageInfo *pi) {
     float rate = -1.0;
     float delta = pi->last_crawl - pi->first_crawl;
     if (delta > 0)
          rate = ((float)pi->n_changes)/delta;
     return rate;
}

void
page_info_delete(PageInfo *pi) {
     if (pi != 0) {
          free(pi->url);
          free(pi->content_hash);
          free(pi);
     }
}

PageInfoList *
page_info_list_new(PageInfo *pi, uint64_t hash) {
     PageInfoList *pil = malloc(sizeof(*pil));
     if (!pil)
          return 0;

     pil->hash = hash;
     pil->page_info = pi;
     pil->next = 0;

     return pil;
}

PageInfoList *
page_info_list_cons(PageInfoList *pil, PageInfo *pi, uint64_t hash) {
     PageInfoList *pil_new = page_info_list_new(pi, hash);
     if (!pil_new)
          return 0;

     pil_new->next = pil;

     return pil_new;
}

void
page_info_list_delete(PageInfoList *pil) {
     PageInfoList *next;
     do {
          next = pil->next;
          page_info_delete(pil->page_info);
          free(pil);
          pil = next;
     } while (next != 0);
}
/// @}

/// @addtogroup PageDB
/// @{

/** The "info" database stores a fixed amount of keys
 * this key points to the number of pages inside the database,
 * crawled or not. It is used to assign new IDs
 */
static char info_n_pages[] = "n_pages";


uint64_t
page_db_hash(const char *url) {
     return XXH64(url, strlen(url), 0);
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
          txn, "hash2info", MDB_INTEGERKEY, cursor, 0);
}

static int
page_db_open_hash2idx(MDB_txn *txn, MDB_cursor **cursor) {
     return page_db_open_cursor(
          txn, "hash2idx", MDB_INTEGERKEY, cursor, 0);
}

static int
page_db_open_links(MDB_txn *txn, MDB_cursor **cursor) {
     return page_db_open_cursor(
          txn, "links", MDB_INTEGERKEY, cursor, 0);
}

static int
page_db_open_info(MDB_txn *txn, MDB_cursor **cursor) {
     return page_db_open_cursor(txn, "info", 0, cursor, 0);
}


static void
page_db_set_error(PageDB *db, int code, const char *message) {
     error_set(db->error, code, message);
}

static void
page_db_add_error(PageDB *db, const char *message) {
     error_add(db->error, message);
}

/** Doubles database size.
 *
 * This function is automatically called when an operation cannot proceed because
 * of insufficient allocated mmap memory.
 */
static PageDBError
page_db_expand(PageDB *db) {
     if (txn_manager_expand(db->txn_manager) != 0) {
          page_db_set_error(db, page_db_error_internal, __func__);
          page_db_add_error(db, db->txn_manager->error->message);
     }
     return db->error->code;
}

PageDBError
page_db_new(PageDB **db, const char *path) {
     PageDB *p = *db = malloc(sizeof(*p));
     if (p == 0)
          return page_db_error_memory;

     p->error = error_new();
     if (p->error == 0) {
          free(p);
          return page_db_error_memory;
     }
     p->persist = PAGE_DB_DEFAULT_PERSIST;

     // create directory if not present yet
     const char *error = make_dir(path);
     if ((p->path = strdup(path)) == 0) {
          error = "could not duplicate path string";
     }
     if (error != 0) {
          page_db_set_error(p, page_db_error_invalid_path, __func__);
          page_db_add_error(p, error);
          return p->error->code;
     }

     if (txn_manager_new(&p->txn_manager, 0) != 0) {
          page_db_set_error(p, page_db_error_internal, __func__);
          page_db_add_error(p, p->txn_manager?
                            p->txn_manager->error->message:
                            "NULL");
          return p->error->code;
     }

     // initialize LMDB on the directory
     MDB_txn *txn;
     MDB_dbi dbi;
     int mdb_rc = 0;
     if ((mdb_rc = mdb_env_create(&p->txn_manager->env) != 0))
          error = "creating environment";
     else if ((mdb_rc = mdb_env_set_mapsize(
                    p->txn_manager->env, PAGE_DB_DEFAULT_SIZE)) != 0)
          error = "setting map size";
     else if ((mdb_rc = mdb_env_set_maxdbs(p->txn_manager->env, 5)) != 0)
          error = "setting number of databases";
     else if ((mdb_rc = mdb_env_open(
                    p->txn_manager->env,
                    path,
                    MDB_NOTLS | MDB_WRITEMAP | MDB_MAPASYNC, 0664) != 0))
          error = "opening environment";
     else if ((mdb_rc = txn_manager_begin(p->txn_manager, 0, &txn)) != 0)
          error = "starting transaction";
     // create all database
     else if ((mdb_rc = mdb_dbi_open(txn,
                                     "hash2info",
                                     MDB_CREATE | MDB_INTEGERKEY,
                                     &dbi)) != 0)
          error = "creating hash2info database";
     else if ((mdb_rc = mdb_dbi_open(txn,
                                     "hash2idx",
                                     MDB_CREATE | MDB_INTEGERKEY,
                                     &dbi)) != 0)
          error = "creating hash2idx database";
     else if ((mdb_rc = mdb_dbi_open(txn,
                                     "links",
                                     MDB_CREATE | MDB_INTEGERKEY,
                                     &dbi)) != 0)
          error = "creating links database";
     else if ((mdb_rc = mdb_dbi_open(txn, "info", MDB_CREATE, &dbi)) != 0)
          error = "creating info database";
     else {
          // initialize n_pages inside info database
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
               if (txn_manager_commit(p->txn_manager, txn) != 0)
                    error = p->txn_manager->error->message;
               break; // we good
          default:
               error = "could not initialize info.n_pages";
          }
     }

     if (error != 0) {
          page_db_set_error(p, page_db_error_internal, __func__);
          page_db_add_error(p, error);
          page_db_add_error(p, mdb_strerror(mdb_rc));

          mdb_env_close(p->txn_manager->env);
     }

     return p->error->code;
}


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
     *page_info = 0;

     int mdb_rc = mdb_cursor_get(cur, key, &val, MDB_SET);
     int put_flags = 0;
     switch (mdb_rc) {
     case 0:
          if (!(*page_info = page_info_load(&val)))
               goto on_error;
          if ((page_info_update(*page_info, page) != 0))
               goto on_error;
          put_flags = MDB_CURRENT;
          break;
     case MDB_NOTFOUND:
          if (!(*page_info = page_info_new_crawled(page)))
              goto on_error;
          break;
     default:
          goto on_error;
     }

     if ((page_info_dump(*page_info, &val)   != 0))
          goto on_error;

     if ((mdb_rc = mdb_cursor_put(cur, key, &val, put_flags)) != 0)
          goto on_error;

     free(val.mv_data);
     val.mv_data = 0;

     *mdb_error = 0;
     return 0;

on_error:
     if (val.mv_data)
          free(val.mv_data);
     *mdb_error = mdb_rc;
     page_info_delete(*page_info);
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
                           const LinkInfo *link,
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

     free(val.mv_data);
     val.mv_data = 0;

     *mdb_error = 0;
     return 0;
on_error:
     if (val.mv_data)
          free(val.mv_data);

     *mdb_error = mdb_rc;
     page_info_delete(pi);
     return -1;
}

PageDBError
page_db_add(PageDB *db, const CrawledPage *page, PageInfoList **page_info_list) {
     // check if page should be expanded
     if (page_db_expand(db) != 0)
          return db->error->code;

     MDB_txn *txn;

     MDB_cursor *cur_hash2info;
     MDB_cursor *cur_hash2idx;
     MDB_cursor *cur_links;
     MDB_cursor *cur_info;

     MDB_val key;
     MDB_val val;

     int mdb_rc;
     char *error = 0;

     // start a new write transaction
     txn = 0;
     if ((txn_manager_begin(db->txn_manager, 0, &txn)) != 0)
          error = db->txn_manager->error->message;
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
     if (page_info_list) {
          *page_info_list = page_info_list_new(pi, hash);
          if (!*page_info_list) {
               error = "allocating new PageInfo list";
               goto on_error;
          }
     } else {
          page_info_delete(pi);
          pi = 0;
     }

     size_t n_links = crawled_page_n_links(page);
     uint64_t *id = malloc((n_links + 1)*sizeof(*id));
     if (!id) {
          error = "could not malloc";
          goto on_error;
     }
     for (size_t i=0; i <= n_links; ++i) {
          const LinkInfo *link = i > 0? crawled_page_get_link(page, i - 1): 0;
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
                    if (page_info_list) {
                         if (!(*page_info_list = page_info_list_cons(*page_info_list, pi, hash))) {
                              error = "adding new PageInfo to list";
                              goto on_error;
                         }
                    }
                    else {
                         page_info_delete(pi);
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
     // the links are stored as deltas starting from the 'from' page, encoded
     // using varint
     uint8_t *buf = val.mv_data = malloc(10*n_links);
     if (!buf) {
          error = "allocating memory to store links";
          goto on_error;
     }
     for (size_t i=1; i<=n_links; ++i)
          buf = varint_encode_int64((int64_t)id[i] - (int64_t)id[i-1], buf);
     val.mv_size = (char*)buf - (char*)val.mv_data;
     if ((mdb_rc = mdb_cursor_put(cur_links, &key, &val, 0)) != 0) {
          error = "storing links";
          goto on_error;
     }
     free(val.mv_data);
     free(id);
     id = 0;

     if (txn_manager_commit(db->txn_manager, txn) != 0) {
          error = db->txn_manager->error->message;
          goto on_error;
     }
     return db->error->code;

on_error:
     if (id)
          free(id);
     if (txn)
          txn_manager_abort(db->txn_manager, txn);

     page_db_set_error(db, page_db_error_internal, __func__);
     page_db_add_error(db, error);
     if (mdb_rc != 0)
          page_db_add_error(db, mdb_strerror(mdb_rc));

     return db->error->code;

}

PageDBError
page_db_get_info(PageDB *db, uint64_t hash, PageInfo **pi) {
     MDB_txn *txn;
     MDB_cursor *cur;

     int mdb_rc;
     char *error = 0;
     if (txn_manager_begin(db->txn_manager, MDB_RDONLY, &txn) != 0)
          error = db->txn_manager->error->message;
     // open hash2info database
     else if ((mdb_rc = page_db_open_hash2info(txn, &cur)) != 0) {
          error = "opening hash2info database";
          goto on_error;
     }
     else {
          MDB_val key = {
               .mv_size = sizeof(hash),
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
     mdb_cursor_close(cur);
     txn_manager_abort(db->txn_manager, txn);
     return 0;

on_error:
     txn_manager_abort(db->txn_manager, txn);
     page_db_set_error(db, page_db_error_internal, __func__);
     page_db_add_error(db, error);
     if (mdb_rc != 0)
          page_db_add_error(db, mdb_strerror(mdb_rc));

     return db->error->code;
}

static PageDBError
page_db_get_idx_cur(PageDB *db, MDB_cursor *cur, uint64_t hash, uint64_t *idx) {
     int mdb_rc = 0;

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
          page_db_set_error(db, page_db_error_internal, __func__);
          page_db_add_error(db, "retrieving val from hash2info");
          page_db_add_error(db, mdb_strerror(mdb_rc));
          return db->error->code;
     }
}

PageDBError
page_db_get_idx(PageDB *db, uint64_t hash, uint64_t *idx) {
     MDB_txn *txn = 0;
     MDB_cursor *cur = 0;

     int mdb_rc;
     int ret = 0;
     char *error = 0;
     if (txn_manager_begin(db->txn_manager, MDB_RDONLY, &txn) != 0)
          error = db->txn_manager->error->message;
     // open hash2info database
     else if ((mdb_rc = page_db_open_hash2idx(txn, &cur)) != 0)
          error = "opening hash2idx database";

     if (error) {
          page_db_set_error(db, page_db_error_internal, __func__);
          page_db_add_error(db, error);
          if (mdb_rc != 0)
               page_db_add_error(db, mdb_strerror(mdb_rc));

          ret = page_db_error_internal;
     }
     else {
          ret = page_db_get_idx_cur(db, cur, hash, idx);
     }
     if (cur)
          mdb_cursor_close(cur);
     if (txn)
          txn_manager_abort(db->txn_manager, txn);
     return ret;
}

PageDBError
page_db_get_scores(PageDB *db, MMapArray **scores) {
     MDB_txn *txn;

     MDB_cursor *cur_hash2info;
     MDB_cursor *cur_hash2idx;
     MDB_cursor *cur_info;

     MDB_val key;
     MDB_val val;

     int mdb_rc = 0;
     char *error1 = 0;
     char *error2 = 0;

     if ((txn_manager_begin(db->txn_manager, MDB_RDONLY, &txn)) != 0)
          error1 = db->txn_manager->error->message;
     else if ((mdb_rc = page_db_open_hash2info(txn, &cur_hash2info)) != 0)
          error1 = "opening hash2info cursor";
     else if ((mdb_rc = page_db_open_hash2idx(txn, &cur_hash2idx)) != 0)
          error1 = "opening hash2idx cursor";
     else if ((mdb_rc = page_db_open_info(txn, &cur_info)) != 0)
          error1 = "opening info cursor";

     if (error1)
          goto on_error;

     // get n_pages
     key.mv_size = sizeof(info_n_pages);
     key.mv_data = info_n_pages;
     if ((mdb_rc = mdb_cursor_get(cur_info, &key, &val, MDB_SET)) != 0) {
          error1 = "retrieving info.n_pages";
          error2 = mdb_strerror(mdb_rc);
          goto on_error;
     }
     size_t n_pages = *(size_t*)val.mv_data;

     char *pscores = build_path(db->path, "scores.bin");
     if (mmap_array_new(scores, pscores, n_pages, sizeof(float)) != 0) {
          error1 = "creating scores array";
          error2 = *scores? (*scores)->error->message: "memory error";
          goto on_error;
     }
     mmap_array_zero(*scores);

     for (mdb_rc = mdb_cursor_get(cur_hash2info, &key, &val, MDB_FIRST);
          mdb_rc == 0;
          mdb_rc = mdb_cursor_get(cur_hash2info, &key, &val, MDB_NEXT)) {

          uint64_t hash = *(uint64_t*)key.mv_data;
          float score = page_info_dump_get_score(&val);

          size_t idx;
          switch (page_db_get_idx_cur(db, cur_hash2idx, hash, &idx)) {
          case 0:
               if (mmap_array_set(*scores, idx, &score) != 0) {
                    error1 = "setting score";
                    error2 = (*scores)->error->message;
                    goto on_error;
               }
               break;
          case page_db_error_no_page:
               // ignore
               break;
          default:
               error1 = db->error->message;
               goto on_error;
          }

     }
     if (mdb_rc != MDB_NOTFOUND) {
          error1 = "iterating on hash2info";
          error2 = mdb_strerror(mdb_rc);
          goto on_error;
     }

     if (txn)
          txn_manager_abort(db->txn_manager, txn);
     free(pscores);
     return 0;
on_error:
     if (txn)
          txn_manager_abort(db->txn_manager, txn);
     free(pscores);
     page_db_set_error(db, page_db_error_internal, __func__);
     page_db_add_error(db, error1);
     page_db_add_error(db, error2);

     return db->error->code;
}


/** Close database */
PageDBError
page_db_delete(PageDB *db) {
     if (!db)
          return 0;

     mdb_env_close(db->txn_manager->env);
     if (txn_manager_delete(db->txn_manager) != 0) {
          page_db_set_error(db, page_db_error_internal, __func__);
          page_db_add_error(db, "deleting transaction manager");
          page_db_add_error(db, db->txn_manager->error->message);
          return db->error->code;
     }

     if (!db->persist) {
          char *data = build_path(db->path, "data.mdb");
          char *lock = build_path(db->path, "lock.mdb");

          // proceeed even the data files cannot be deleted from disk
          (void)remove(data);
          (void)remove(lock);
          (void)remove(db->path);

          free(data);
          free(lock);
     }
     free(db->path);
     error_delete(db->error);
     free(db);
     return 0;
}

/** Set persist option for database */
void
page_db_set_persist(PageDB *db, int value) {
     db->persist = value;
}
/// @}


/// @addtogroup PageDBLinkStream
/// @{

static int
page_db_link_stream_copy_links(PageDBLinkStream *es, MDB_val *key, MDB_val *val) {
     // decompressing varints can expand size at most 8x
     size_t max_size = 8*val->mv_size;
     if (max_size > es->m_to) {
          if ((es->to = (uint64_t*)realloc(es->to, max_size*sizeof(uint64_t))) == 0) {
               es->state = stream_state_error;
               return -1;
          }
          es->m_to = max_size;
     }
     es->i_to = 0;
     es->n_to = 0;
     uint64_t id = es->from = *(uint64_t*)key->mv_data;
     uint8_t *pos = (uint8_t*)val->mv_data;
     uint8_t *end = pos + val->mv_size/sizeof(uint8_t);
     while (pos < end) {
          uint8_t read = 0;
          es->to[es->n_to++] = id += varint_decode_int64(pos, &read);
          pos += read;
     }
     return 0;
}

static PageDBError
page_db_link_stream_open_cursor(PageDBLinkStream *es) {
     MDB_txn *txn = 0;
     MDB_dbi dbi_links;

     int mdb_rc = 0;
     char *error = 0;

     // start a new read transaction
     if (txn_manager_begin(es->db->txn_manager, MDB_RDONLY, &txn) != 0) {
          error = es->db->txn_manager->error->message;
          goto mdb_error;
     }
     // open cursor to links database
     switch (mdb_rc = mdb_dbi_open(
                  txn,
                  "links",
                  MDB_INTEGERKEY,
                  &dbi_links)) {

     case 0:
          if ((mdb_rc = mdb_cursor_open(
                    txn,
                    dbi_links,
                    &es->cur)) != 0) {
               error = "opening links cursor";
               goto mdb_error;
          }
          es->state = stream_state_init;
          break;
     case MDB_NOTFOUND:
          txn_manager_abort(es->db->txn_manager, txn);
          es->cur = 0;
          es->state = stream_state_end;
          break;
     default:
          error = "opening links database";
          goto mdb_error;

     }
     return es->db->error->code;

mdb_error:
     es->state = stream_state_error;
     if (txn)
          txn_manager_abort(es->db->txn_manager, txn);
     es->cur = 0;

     page_db_set_error(es->db, page_db_error_internal, __func__);
     page_db_add_error(es->db, error);
     if (mdb_rc != 0)
          page_db_add_error(es->db, mdb_strerror(mdb_rc));

     return es->db->error->code;
}

PageDBError
page_db_link_stream_new(PageDBLinkStream **es, PageDB *db) {
     PageDBLinkStream *p = *es = calloc(1, sizeof(*p));
     if (p == 0)
          return page_db_error_memory;
     p->db = db;

     StreamState state = page_db_link_stream_reset(p);
     if (state == stream_state_error) {
          page_db_set_error(db, page_db_error_internal, __func__);
          page_db_add_error(db, "resetting link stream");
     }
     return db->error->code;
}

StreamState
page_db_link_stream_reset(void *st) {
     PageDBLinkStream *es = st;

     // if no cursor, try to create one
     if (es->cur) {
          txn_manager_abort(es->db->txn_manager, mdb_cursor_txn(es->cur));
          mdb_cursor_close(es->cur);
          es->cur = 0;
     }

     if (page_db_link_stream_open_cursor(es) != 0) {
          return stream_state_error;
     }
     return es->state;
}

StreamState
page_db_link_stream_next(void *st, Link *link) {
     PageDBLinkStream *es = st;
     if (!es->cur)
          return es->state;
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
               return es->state = stream_state_end;
          default:
               return es->state = stream_state_error;
          }
     }
     es->state = stream_state_next;
     link->from = es->from;
     link->to = es->to[es->i_to++];
     return es->state;
}

void
page_db_link_stream_delete(PageDBLinkStream *es) {
     if (es) {
          if (es->cur) {
               txn_manager_abort(es->db->txn_manager, mdb_cursor_txn(es->cur));
               mdb_cursor_close(es->cur);
          }
          free(es->to);
          free(es);
     }
}

/// @}

PageDBError
hashidx_stream_new(HashIdxStream **st, PageDB *db) {
     HashIdxStream *p = *st = calloc(1, sizeof(*p));
     if (p == 0)
          return page_db_error_memory;

     p->db = db;

     MDB_txn *txn;
     int mdb_rc = 0;
     char *error = 0;

     // start a new read transaction
     if (txn_manager_begin(db->txn_manager, MDB_RDONLY, &txn) != 0) {
          error = db->txn_manager->error->message;
          goto mdb_error;
     }
     // open cursor to links database
     if ((mdb_rc = page_db_open_hash2idx(txn, &p->cur)) != 0) {
          error = "opening hash2idx cursor";
          goto mdb_error;
     }

     p->state = stream_state_init;

     return 0;

mdb_error:
     p->state = stream_state_error;

     page_db_set_error(db, page_db_error_internal, __func__);
     page_db_add_error(db, error);
     if (mdb_rc != 0)
          page_db_add_error(db, mdb_strerror(mdb_rc));

     return db->error->code;
}

StreamState
hashidx_stream_next(HashIdxStream *st, uint64_t *hash, size_t *idx) {
     MDB_val key;
     MDB_val val;
     switch (mdb_cursor_get(st->cur,
                            &key,
                            &val,
                            st->state == stream_state_init? MDB_FIRST: MDB_NEXT)) {
     case 0:
          *hash = *(uint64_t*)key.mv_data;
          *idx = *(size_t*)val.mv_data;
          return st->state = stream_state_next;
     case MDB_NOTFOUND:
          return st->state = stream_state_end;
     default:
          return st->state = stream_state_error;
     }
}

void
hashidx_stream_delete(HashIdxStream *st) {
     MDB_txn *txn = mdb_cursor_txn(st->cur);
     mdb_cursor_close(st->cur);
     txn_manager_abort(st->db->txn_manager, txn);
     free(st);
}

#if (defined TEST) && TEST
#include "CuTest.h"
#include "test.h"

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
     CuAssertDblEquals(tc, 0.7, page_info_dump_get_score(&val), 1e-6);

     PageInfo *pi2 = page_info_load(&val);
     CuAssertPtrNotNull(tc, pi2);

     free(val.mv_data);

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

     PageDB *db;
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              page_db_new(&db, test_dir) == 0);
     db->persist = 0;

     CrawledPage *cp1 = crawled_page_new("www.yahoo.com");
     crawled_page_add_link(cp1, "a", 0.1);
     crawled_page_add_link(cp1, "b", 0.2);
     crawled_page_add_link(cp1, "www.google.com", 0.3);
     crawled_page_set_hash64(cp1, 1000);
     cp1->score = 0.5;

     CrawledPage *cp2 = crawled_page_new("www.bing.com");
     crawled_page_add_link(cp2, "x", 1.1);
     crawled_page_add_link(cp2, "y", 1.2);
     crawled_page_set_hash64(cp2, 2000);
     cp2->score = 0.2;

     PageInfoList *pil;
     CuAssert(tc,
              db->error->message,
              page_db_add(db, cp1, &pil) == 0);
     page_info_list_delete(pil);

     CuAssert(tc,
              db->error->message,
              page_db_add(db, cp2, &pil) == 0);
     page_info_list_delete(pil);

     crawled_page_set_hash64(cp2, 3000);
     CuAssert(tc,
              db->error->message,
              page_db_add(db, cp2, &pil) == 0);
     page_info_list_delete(pil);

     MMapArray *scores = 0;
     CuAssert(tc,
              db->error->message,
              page_db_get_scores(db, &scores) == 0);

     size_t idx;
     CuAssert(tc,
              db->error->message,
              page_db_get_idx(db, page_db_hash("www.yahoo.com"), &idx) == 0);
     CuAssertDblEquals(
          tc,
          0.5,
          *(float*)mmap_array_idx(scores, idx),
          1e-6);
     CuAssert(tc,
              db->error->message,
              page_db_get_idx(db, page_db_hash("x"), &idx) == 0);
     CuAssertDblEquals(
          tc,
          1.1,
          *(float*)mmap_array_idx(scores, idx),
          1e-6);


     CuAssert(tc,
              scores->error->message,
              mmap_array_delete(scores) == 0);

     crawled_page_delete(cp1);
     crawled_page_delete(cp2);

     char pi_out[1000];
     char *print_pages[] = {"www.yahoo.com", "www.google.com", "www.bing.com"};
     for (size_t i=0; i<3; ++i) {
          PageInfo *pi;
          CuAssert(tc,
                   db->error->message,
                   page_db_get_info(db, page_db_hash(print_pages[i]), &pi) == 0);

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
              db->error->message,
              page_db_link_stream_new(&es, db) == 0);

     if (es->state == stream_state_init) {
          Link link;
          int i=0;
          while (page_db_link_stream_next(es, &link) == stream_state_next) {
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
          CuAssertTrue(tc, es->state != stream_state_error);
     }
     page_db_link_stream_delete(es);

     page_db_delete(db);
}

static void
test_page_db_crawl(CuTest *tc, size_t n_pages) {
     char test_dir[] = "test-pagedb-XXXXXX";
     mkdtemp(test_dir);

     PageDB *db;
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              page_db_new(&db, test_dir) == 0);
     db->persist = 0;

     const size_t n_links = 10;

     LinkInfo links[n_links + 1];
     for (size_t j=0; j<=n_links; ++j) {
          sprintf(links[j].url = malloc(50), "test_url_%zu", j);
          links[j].score = j;
     }
     time_t start = time(0);
     printf("%s: \n", __func__);
     for (size_t i=0; i<n_pages; ++i) {
#if 1
          if (i % 10000 == 0) {
               double delta = difftime(time(0), start);
               if (delta > 0) {
                    printf("%10zuK/%zuM: %9zu pages/sec\n",
                           i/1000, n_pages/1000000, i/((size_t)delta));
               }
          }
#endif
          free(links[0].url);
          for (size_t j=0; j<n_links; ++j)
               links[j] = links[j+1];
          sprintf(links[n_links].url = malloc(50), "test_url_%zu", i + n_links);
          links[n_links].score = i;

          CrawledPage *cp = crawled_page_new(links[0].url);
          for (size_t j=1; j<=n_links; ++j)
               crawled_page_add_link(cp, links[j].url, 0.5);

          PageInfoList *pil;
          CuAssert(tc,
                   db->error->message,
                   page_db_add(db, cp, &pil) == 0);
          page_info_list_delete(pil);
          crawled_page_delete(cp);
     }
     for (size_t j=0; j<=n_links; ++j)
          free(links[j].url);

     PageDBLinkStream *st;
     CuAssert(tc,
              db->error->message,
              page_db_link_stream_new(&st, db) == 0);

     Hits *hits;
     CuAssert(tc,
              hits!=0? hits->error->message: "NULL",
              hits_new(&hits, test_dir, n_pages) == 0);

     hits->precision = 1e-3;
     CuAssert(tc,
              hits->error->message,
              hits_compute(hits,
                           st,
                           page_db_link_stream_next,
                           page_db_link_stream_reset) == 0);

     CuAssert(tc,
              hits->error->message,
              hits_delete(hits) == 0);

     page_db_link_stream_delete(st);

     page_db_delete(db);
}

/* Tests the typical database operations on a moderate crawl of 10M pages */
void
test_page_db_large(CuTest *tc) {
     test_page_db_crawl(tc, 10000000);
}

/* Tests the typical database operations on a big crawl of 100M pages */
void
test_page_db_very_large(CuTest *tc) {
     test_page_db_crawl(tc, 100000000);
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

     PageDB *db;
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              page_db_new(&db, test_dir) == 0);
     db->persist = 0;

     char *urls[5] = {"1", "2", "3", "4", "5" };
     LinkInfo links_1[] = {{"2", 0.1}, {"5", 0.1}};
     LinkInfo links_2[] = {{"3", 0.1}, {"5", 0.1}};
     LinkInfo links_3[] = {{"4", 0.1}, {"5", 0.1}};
     LinkInfo links_4[] = {{"1", 0.1}, {"5", 0.1}};
     LinkInfo *links[5] = {
          links_1, links_2, links_3, links_4, 0
     };
     int n_links[5] = {2, 2, 2, 2, 0};

     for (int i=0; i<5; ++i) {
          CrawledPage *cp = crawled_page_new(urls[i]);
          for (int j=0; j<n_links[i]; ++j)
               crawled_page_add_link(cp, links[i][j].url, links[i][j].score);
          cp->score = i/5.0;
          crawled_page_set_hash64(cp, i);

          PageInfoList *pil;
          CuAssert(tc,
                   db->error->message,
                   page_db_add(db, cp, &pil) == 0);
          page_info_list_delete(pil);
          crawled_page_delete(cp);
     }

     PageDBLinkStream *st;
     CuAssert(tc,
              db->error->message,
              page_db_link_stream_new(&st, db) == 0);

     Hits *hits;
     CuAssert(tc,
              hits!=0? hits->error->message: "NULL",
              hits_new(&hits, test_dir, 5) == 0);

     hits->precision = 1e-8;
     CuAssert(tc,
              hits->error->message,
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
                   db->error->message,
                   page_db_get_idx(db, page_db_hash(urls[i]), &idx) == 0);

          CuAssertPtrNotNull(tc,
                             h_score = mmap_array_idx(hits->h1, idx));
          CuAssertPtrNotNull(tc,
                             a_score = mmap_array_idx(hits->a1, idx));

          CuAssertDblEquals(tc, h_scores[i], *h_score, 1e-6);
          CuAssertDblEquals(tc, a_scores[i], *a_score, 1e-6);
     }
     CuAssert(tc,
              hits->error->message,
              hits_delete(hits) == 0);

     page_db_delete(db);
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

     PageDB *db;
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              page_db_new(&db, test_dir) == 0);
     db->persist = 0;

     char *urls[5] = {"1", "2", "3", "4", "5" };
     LinkInfo links_1[] = {{"2", 0.1}, {"5", 0.1}};
     LinkInfo links_2[] = {{"3", 0.1}, {"5", 0.1}};
     LinkInfo links_3[] = {{"4", 0.1}, {"5", 0.1}};
     LinkInfo links_4[] = {{"1", 0.1}, {"5", 0.1}};
     LinkInfo *links[5] = {
          links_1, links_2, links_3, links_4, 0
     };

     int n_links[5] = {2, 2, 2, 2, 0};

     for (int i=0; i<5; ++i) {
          CrawledPage *cp = crawled_page_new(urls[i]);
          for (int j=0; j<n_links[i]; ++j)
               crawled_page_add_link(cp, links[i][j].url, links[i][j].score);
          cp->score = i/5.0;
          crawled_page_set_hash64(cp, i);
          PageInfoList *pil;
          CuAssert(tc,
                   db->error->message,
                   page_db_add(db, cp, &pil) == 0);
          page_info_list_delete(pil);
          crawled_page_delete(cp);
     }

     PageDBLinkStream *st;
     CuAssert(tc,
              db->error->message,
              page_db_link_stream_new(&st, db) == 0);

     PageRank *pr;
     CuAssert(tc,
              pr!=0? pr->error->message: "NULL",
              page_rank_new(&pr, test_dir, 5) == 0);

     pr->precision = 1e-6;
     CuAssert(tc,
              pr->error->message,
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
                   db->error->message,
                   page_db_get_idx(db, page_db_hash(urls[i]), &idx) == 0);

          CuAssertPtrNotNull(tc,
                             score = mmap_array_idx(pr->value1, idx));

          CuAssertDblEquals(tc, scores[i], *score, 1e-6);
     }
     CuAssert(tc,
              pr->error->message,
              page_rank_delete(pr) == 0);

     page_db_delete(db);
}

void
test_hashidx_stream(CuTest *tc) {
     char test_dir[] = "test-bfs-XXXXXX";
     mkdtemp(test_dir);

     PageDB *db;
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              page_db_new(&db, test_dir) == 0);
     db->persist = 0;

     CrawledPage *cp = crawled_page_new("1");
     crawled_page_add_link(cp, "a", 0);
     crawled_page_add_link(cp, "b", 0);
     CuAssert(tc,
              db->error->message,
              page_db_add(db, cp, 0) == 0);
     crawled_page_delete(cp);

     cp = crawled_page_new("2");
     crawled_page_add_link(cp, "c", 0);
     crawled_page_add_link(cp, "d", 0);
     CuAssert(tc,
              db->error->message,
              page_db_add(db, cp, 0) == 0);
     crawled_page_delete(cp);

     HashIdxStream *stream;
     CuAssert(tc,
              db->error->message,
              hashidx_stream_new(&stream, db) == 0);

     CuAssert(tc,
              "stream was not initialized",
              stream->state == stream_state_init);

     uint64_t hash;
     size_t idx;

     uint64_t expected_hash[] = {
          page_db_hash("1"),
          page_db_hash("a"),
          page_db_hash("b"),
          page_db_hash("2"),
          page_db_hash("c"),
          page_db_hash("d")
     };

     for (int i=0; i<6; ++i) {
          CuAssert(tc,
                   "stream element expected",
                   hashidx_stream_next(stream, &hash, &idx) == stream_state_next);
          if (idx > 5)
               CuFail(tc, "unexpected index");
          CuAssert(tc,
                   "mismatch between index and hash",
                   hash == expected_hash[idx]);
     }
     hashidx_stream_delete(stream);

     page_db_delete(db);
}

CuSuite *
test_page_db_suite(TestOps ops) {
     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_page_info_serialization);
     SUITE_ADD_TEST(suite, test_page_db_simple);
     SUITE_ADD_TEST(suite, test_page_db_hits);
     SUITE_ADD_TEST(suite, test_page_db_page_rank);
     if (ops == test_all || ops == test_large)
          SUITE_ADD_TEST(suite, test_page_db_large);
     if (ops == test_all)
          SUITE_ADD_TEST(suite, test_page_db_very_large);

     SUITE_ADD_TEST(suite, test_hashidx_stream);

     return suite;
}
#endif // TEST
