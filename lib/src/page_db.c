#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif
#include <stdint.h>
#include <stdio.h>
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
          void *p = realloc(pl->link_info, 2*pl->m_links*sizeof(LinkInfo));
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
#ifdef _WIN32
#define snprintf _snprintf
#endif
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
page_info_new_link(const char *url, uint64_t linked_from, float score) {
     PageInfo *pi = calloc(1, sizeof(*pi));
     if (!pi)
          return 0;

     if (!(pi->url = strdup(url))) {
          free(pi);
          return 0;
     }

     pi->score = score;
     pi->linked_from = linked_from;
     return pi;
}

/** Create a new PageInfo from a crawled page.
 *
 * @return A pointer to the new structure or NULL if failure
 */
static PageInfo *
page_info_new_crawled(const CrawledPage *cp) {
     PageInfo *pi = page_info_new_link(cp->url, 0, cp->score);
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
     val->mv_size = sizeof(pi->linked_from) + sizeof(pi->score) + sizeof(pi->n_crawls);
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
     PAGE_INFO_WRITE(pi->linked_from);
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
     PAGE_INFO_READ(pi->linked_from);
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
     int start, end;
     uint64_t hash_full;
     uint32_t *hash_part = (uint32_t*)&hash_full;
     if (url_domain(url, &start, &end) != 0)
          hash_part[1] = 0;
     else
          hash_part[1] = XXH32(url + start, end - start + 1, 0);

     hash_part[0] = XXH32(url, strlen(url), 0);
     return hash_full;
}

uint32_t
page_db_hash_get_domain(uint64_t hash) {
     uint32_t *hash_part = (uint32_t*)&hash;
     return hash_part[1];
}

uint32_t
page_db_hash_get_url(uint64_t hash) {
     uint32_t *hash_part = (uint32_t*)&hash;
     return hash_part[0];
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
     p->domain_temp = 0;

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
                    MDB_NOTLS | MDB_NOSYNC, 0664) != 0))
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
                           uint64_t linked_from,
                           const LinkInfo *link,
                           PageInfo **page_info,
                           int *mdb_error) {
     MDB_val val;
     int mdb_rc = 0;

     PageInfo *pi = *page_info = page_info_new_link(link->url, linked_from, link->score);
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

     uint64_t cp_hash = page_db_hash(page->url);
     key.mv_size = sizeof(uint64_t);
     key.mv_data = &cp_hash;

     if (db->domain_temp) {
          domain_temp_update(db->domain_temp, (float)page->time);
          domain_temp_heat(db->domain_temp, page_db_hash_get_domain(cp_hash));
     }

     PageInfo *pi;
     if (page_db_add_crawled_page_info(cur_hash2info, &key, page, &pi, &mdb_rc) != 0) {
          error = "adding/updating page info";
          goto on_error;
     }
     if (page_info_list) {
          *page_info_list = page_info_list_new(pi, cp_hash);
          if (!*page_info_list) {
               error = "allocating new PageInfo list";
               goto on_error;
          }
     } else {
          page_info_delete(pi);
          pi = 0;
     }

     size_t n_links = crawled_page_n_links(page);
     // store here links inside the same domain as the crawled page
     uint64_t *same_id = malloc((n_links + 1)*sizeof(*same_id));
     // store here links outside the domain of the crawled page
     uint64_t *diff_id = malloc((n_links + 1)*sizeof(*diff_id));
     // next link id is going to be written here
     uint64_t *id = diff_id;
     // number of id's in same_id and diff_id. The first element of diff_id
     // array is reserved for the id of the crawled page, so we start at 1.
     // The first element of same_id will be a copy of the last element of
     // diff_id, so we start at 1 too.
     uint64_t same_i = 1;
     uint64_t diff_i = 1;
     if (!same_id || !diff_id) {
          error = "could not malloc";
          goto on_error;
     }
     // hash of the current URL
     uint64_t hash = cp_hash;
     for (size_t i=0; i <= n_links; ++i) {
          const LinkInfo *link = i > 0? crawled_page_get_link(page, i - 1): 0;
          if (link) {
               hash = page_db_hash(link->url);
               key.mv_size = sizeof(uint64_t);
               key.mv_data = &hash;

               id = same_domain(page->url, link->url)?
                    same_id + same_i++:
                    diff_id + diff_i++;
          }
          val.mv_size = sizeof(uint64_t);
          val.mv_data = &n_pages;

          switch (mdb_rc = mdb_cursor_put(cur_hash2idx, &key, &val, MDB_NOOVERWRITE)) {
          case MDB_KEYEXIST: // not really an error
               *id = *(uint64_t*)val.mv_data;
               break;
          case 0:
               *id = n_pages++;
               if (link) {
                    if (page_db_add_link_page_info(
                             cur_hash2info, &key, cp_hash, link, &pi, &mdb_rc) != 0) {
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
     // The format for the links is the following:
     //
     // KEY = ID of crawled page
     // VAL = Number of links to different domain,
     //       diff link id 1, diff link id 2, ...
     //       same link id 1, same link id 2, ...

     key.mv_size = sizeof(uint64_t);
     key.mv_data = diff_id; // remember that diff_id[0] is the id of the
                            // crawled page
     // the links are stored as deltas starting from the 'from' page, encoded
     // using varint. The '10' is because an 8 byte number can grow up to 10
     // bytes in size if encoded.
     uint8_t *buf = val.mv_data = malloc(10*(n_links + 1));
     if (!buf) {
          error = "allocating memory to store links";
          goto on_error;
     }
     // write number of diff links (substract 1 to take into account this page id)
     buf = varint_encode_uint64(diff_i - 1, buf);
     // write diff links
     for (size_t i=1; i<diff_i; ++i)
          buf = varint_encode_int64((int64_t)diff_id[i] - (int64_t)diff_id[i-1], buf);
     same_id[0] = diff_id[diff_i-1];
     for (size_t i=1; i<same_i; ++i)
          buf = varint_encode_int64((int64_t)same_id[i] - (int64_t)same_id[i-1], buf);

     val.mv_size = (char*)buf - (char*)val.mv_data;
     if ((mdb_rc = mdb_cursor_put(cur_links, &key, &val, 0)) != 0) {
          error = "storing links";
          goto on_error;
     }
     free(val.mv_data);
     free(same_id);
     free(diff_id);
     same_id = diff_id = 0;

     if (txn_manager_commit(db->txn_manager, txn) != 0) {
          error = db->txn_manager->error->message;
          goto on_error;
     }
     return db->error->code;

on_error:
     if (same_id)
          free(same_id);
     if (diff_id)
          free(diff_id);
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
     goto exit;

on_error:
     page_db_set_error(db, page_db_error_internal, __func__);
     page_db_add_error(db, error1);
     page_db_add_error(db, error2);

exit:
     mdb_cursor_close(cur_hash2info);
     mdb_cursor_close(cur_hash2idx);
     mdb_cursor_close(cur_info);
     if (txn)
          txn_manager_abort(db->txn_manager, txn);
     free(pscores);

     return db->error->code;
}

float
page_db_get_domain_crawl_rate(PageDB *db, uint32_t domain_hash) {
     if (db->domain_temp)
          return domain_temp_get(db->domain_temp, domain_hash);
     else
          return 0.0;
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
     domain_temp_delete(db->domain_temp);
     error_delete(db->error);
     free(db);
     return 0;
}

PageDBError
page_db_info_dump(PageDB *db, FILE *output) {
     MDB_txn *txn;
     MDB_cursor *cur_hash2info;
     MDB_cursor *cur_hash2idx;
     MDB_val key;
     MDB_val val;

     int mdb_rc;
     char *error = 0;

     txn = 0;
     if ((txn_manager_begin(db->txn_manager, MDB_RDONLY, &txn)) != 0)
          error = db->txn_manager->error->message;
     else if ((mdb_rc = page_db_open_hash2info(txn, &cur_hash2info)) != 0)
          error = "opening hash2info cursor";
     else if ((mdb_rc = mdb_cursor_get(cur_hash2info, &key, &val, MDB_FIRST)) != 0)
          error = "getting first element";
     else if ((mdb_rc = page_db_open_hash2idx(txn, &cur_hash2idx)) != 0)
          error = "opening hash2idx cursor";

     if (error != 0)
          goto on_error;

     int more_data = 1;
     do {
          PageInfo *pi = page_info_load(&val);
          if (!pi) {
               error = "PageInfo error format";
               goto on_error;
          }
          uint64_t idx;
          if (page_db_get_idx_cur(db, cur_hash2idx, *(uint64_t*)key.mv_data, &idx) != 0) {
               error = "Could not retrieve page index";
               goto on_error;
          }
          fprintf(output, "%016"PRIx64" ", *(uint64_t*)key.mv_data);
          fprintf(output, "%"PRIu64" ", idx);
          fprintf(output, "%s ", pi->url);
          fprintf(output, "%.1f ", pi->first_crawl);
          fprintf(output, "%.1f ", pi->last_crawl);
          fprintf(output, "%zu ", pi->n_changes);
          fprintf(output, "%zu ", pi->n_crawls);
          fprintf(output, "%.3e\n", pi->score);
          page_info_delete(pi);

          switch (mdb_rc = mdb_cursor_get(cur_hash2info, &key, &val, MDB_NEXT)) {
          case 0: // do nothing
               break;
          case MDB_NOTFOUND:
               more_data = 0;
               break;
          default:
               error = mdb_strerror(mdb_rc);
               goto on_error;
          }
     } while (more_data);

     mdb_cursor_close(cur_hash2info);
     mdb_cursor_close(cur_hash2idx);
     txn_manager_abort(db->txn_manager, txn);

     return 0;
on_error:
     if (txn)
          txn_manager_abort(db->txn_manager, txn);
     page_db_set_error(db, page_db_error_internal, __func__);
     page_db_add_error(db, error);

     return db->error->code;
}

PageDBError
page_db_links_dump(PageDB *db, FILE *output) {
     PageDBLinkStream *stream;
     if (page_db_link_stream_new(&stream, db) != 0)
          return page_db_error_internal;
     stream->only_diff_domain = 0;

     Link link;
     StreamState state;
     while ((state = page_db_link_stream_next(stream, &link)) == stream_state_next) {
          fprintf(output, "%"PRIi64" %"PRIi64"\n", link.from, link.to);
     }
     page_db_link_stream_delete(stream);

     if (state != stream_state_end)
          return page_db_error_internal;

     return 0;
}

void
page_db_set_persist(PageDB *db, int value) {
     db->persist = value;
}

PageDBError
page_db_set_domain_temp(PageDB *db, size_t n_domains, float window) {
     if (db->domain_temp)
          domain_temp_delete(db->domain_temp);

     if ((db->domain_temp = domain_temp_new(n_domains, window)) == 0) {
          page_db_set_error(db, page_db_error_internal, __func__);
          page_db_add_error(db, "could not allocate new DomainTemp struct");
          return db->error->code;
     }
     return 0;
}
/// @}

/// @addtogroup HashInfoStream
/// @{
PageDBError
hashinfo_stream_new(HashInfoStream **st, PageDB *db) {
     HashInfoStream *p = *st = calloc(1, sizeof(*p));
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
     if ((mdb_rc = page_db_open_hash2info(txn, &p->cur)) != 0) {
          error = "opening hash2info cursor";
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
hashinfo_stream_next(HashInfoStream *st, uint64_t *hash, PageInfo **pi) {
     MDB_val key;
     MDB_val val;
     switch (mdb_cursor_get(st->cur,
                            &key,
                            &val,
                            st->state == stream_state_init? MDB_FIRST: MDB_NEXT)) {
     case 0:
          *hash = *(uint64_t*)key.mv_data;
          *pi = page_info_load(&val);
          return st->state = stream_state_next;
     case MDB_NOTFOUND:
          return st->state = stream_state_end;
     default:
          return st->state = stream_state_error;
     }
}

void
hashinfo_stream_delete(HashInfoStream *st) {
     MDB_txn *txn = mdb_cursor_txn(st->cur);
     mdb_cursor_close(st->cur);
     txn_manager_abort(st->db->txn_manager, txn);
     free(st);
}
/// @}
/// @addtogroup PageDBLinkStream
/// @{

static int
page_db_link_stream_copy_links(PageDBLinkStream *es,
                               MDB_val *key,
                               MDB_val *val,
                               int only_diff_domain) {
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

     uint8_t read = 0;
     es->n_diff = varint_decode_uint64(val->mv_data, &read);

     uint8_t *pos = (uint8_t*)val->mv_data;
     uint8_t *end = pos + val->mv_size/sizeof(uint8_t);
     pos += read;
     while (pos < end) {
          es->to[es->n_to++] = id += varint_decode_int64(pos, &read);
          pos += read;

          if (only_diff_domain &&
              (es->n_to == es->n_diff))
               break;
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
     p->only_diff_domain = PAGE_DB_LINK_STREAM_DEFAULT_ONLY_DIFF_DOMAIN;

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
               if (page_db_link_stream_copy_links(es, &key, &val, es->only_diff_domain) != 0)
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
#include "test_pagedb.c"
#endif // TEST
