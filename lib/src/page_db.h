#ifndef __PAGE_DB_H__
#define __PAGE_DB_H__

#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <errno.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include "lmdb.h"
#include "xxhash.h"

#include "domain_temp.h"
#include "hits.h"
#include "link_stream.h"
#include "page_rank.h"
#include "txn_manager.h"

#define KB 1024LL
#define MB (1024*KB)
#define GB (1024*MB

/// @addtogroup CrawledPage
/// @{
/** The information that comes with a link inside a crawled page.
 *
 * The link score is used to decide which links should be crawled next. It
 * is application dependent and tipically computed by looking at the link
 * surrounding text.
 */
typedef struct {
     char *url;   /**< ASCII, null terminated string for the page URL*/
     float score; /**< An estimated value of the link score */
} LinkInfo;

/** Allocate at least this amount of memory for link info */
#define PAGE_LINKS_MIN_LINKS 10

/** A (resizable) array of page links.
 *
 * Initially:
 *   n_links = 0
 *   m_links = PAGE_LINKS_MIN_LINKS
 *
 * Always:
 *   0 <= n_links <= m_links
 *
 * */
typedef struct {
     LinkInfo *link_info; /**< Array of LinkInfo */
     size_t n_links;      /**< Number of items inside link_info */
     size_t m_links;      /**< Maximum number of items that can be stored inside link_info */
} PageLinks;

/** The information that comes with a crawled page. */
typedef struct {
     /* We do not allocate any space for error handling since the operations are very
      * simple. Functions return an int to signal the status, a 0 meaning success and -1
      * meaning error. The only type of error possible is a memory error where a malloc or
      * realloc has failed. */

     char *url;                   /**< ASCII, null terminated string for the page URL*/
     PageLinks *links;            /**< List of links inside this page */
     double time;                 /**< Number of seconds since epoch */
     float score;                 /**< A number giving an idea of the page content's value */
     char *content_hash;          /**< A hash to detect content change since last crawl.
                                       Arbitrary byte sequence */
     size_t content_hash_length;  /**< Number of byes of the content_hash */
} CrawledPage;

/** Create a new CrawledPage

    url is a new copy

    The following defaults are used for the different fields:
    - links: no links initially. Use @ref crawled_page_add_link to add some.
    - time: current time
    - score: 0. It can be setted directly.
    - content_hash: NULL. Use @ref crawled_page_set_hash to change

    @return NULL if failure, otherwise a newly allocated CrawledPage
*/
CrawledPage *
crawled_page_new(const char *url);

/** Delete a Crawled Page created with @ref crawled_page_new */
void
crawled_page_delete(CrawledPage *cp);

/** Set content hash
 *
 * The hash is a new copy
 */
int
crawled_page_set_hash(CrawledPage *cp, const char *hash, size_t hash_length);

/** Set content hash from a 128bit hash */
int
crawled_page_set_hash128(CrawledPage *cp, char *hash);

/** Set content hash from a 64bit hash */
int
crawled_page_set_hash64(CrawledPage *cp, uint64_t hash);

/** Set content hash from a 32bit hash */
int
crawled_page_set_hash32(CrawledPage *cp, uint32_t hash);

/** Add a new link to the crawled page */
int
crawled_page_add_link(CrawledPage *cp, const char *url, float score);

/** Get number of links inside page */
size_t
crawled_page_n_links(const CrawledPage *cp);

/** Get a pointer to the link */
const LinkInfo *
crawled_page_get_link(const CrawledPage *cp, size_t i);

/// @}

/// @addtogroup PageInfo
/// @{

/** The information we keep about crawled and uncrawled pages
 *
 * @ref PageInfo are created at the @ref PageDB, that's why there are
 * no public constructors/destructors available.
 */
typedef struct {
     char *url;                   /**< A copy of either @ref CrawledPage::url or
                                   * @ref CrawledPage::links[i] */
     uint64_t linked_from;        /**< The page that first linked this one */
     double first_crawl;          /**< First time this page was crawled */
     double last_crawl;           /**< Last time this page was crawled */
     size_t n_changes;            /**< Number of content changes detected between first and last crawl */
     size_t n_crawls;             /**< Number of times this page has been crawled. Can be zero if it has been observed just as a link*/
     float score;                 /**< A copy of the same field at the last crawl */
     size_t content_hash_length;  /**< Number of bytes in @ref PageInfo::content_hash */
     char *content_hash;          /**< Byte sequence with the hash of the last crawl */
} PageInfo;

/** Write printed representation of PageInfo.

    This function is intended mainly for debugging and development.
    The representation is:
        first_crawl last_crawl n_crawls n_changes url

    Each field is separated with an space. The string is null terminated.
    We use the following format for each field:
    - first_crawl: the standard fixed size (24 bytes) as output by ctime.
      For example: Mon Jan 1 08:01:59 2015
    - last_crawl: the same as first_crawl
    - n_crawls: To ensure fixed size representation this value is converted
      to double and represented in exponential notation with two digits. It has
      therefore always 8 bytes length:
            1.21e+01
    - n_changes: The same as n_crawls
    - url: This is the only variable length field. However, it is truncated at
      512 bytes length.

    @param pi The @ref PageInfo to be printed
    @param out The output buffer, which must be at least 580 bytes long
    @return size of representation or -1 if error
*/
int
page_info_print(const PageInfo *pi, char *out);

/** Estimate change rate of the given page. If no valid rate can be computed
 * return -1.0, otherwise a valid nonnegative change rate. */
float
page_info_rate(const PageInfo *pi);

/** Destroy PageInfo if not NULL, otherwise does nothing */
void
page_info_delete(PageInfo *pi);

/** A linked list of @ref PageInfo (and hash), to be returned by @ref page_db_add */
struct PageInfoList {
     uint64_t hash;        /**< Hash inside the hash2info database */
     PageInfo *page_info;  /**< Info inside the hash2info database */
     /** A pointer to the next element, or NULL */
     struct PageInfoList *next;
};
typedef struct PageInfoList PageInfoList;

/** Create a new PageInfoList, with just one element.
 *
 * @param pi The @ref PageInfo to add. From this point it is the property of the list,
 *           so deleting the list deletes this element.
 * @param hash
 *
 * @return A pointer to the first element of the list, or NULL if failure
 * */
PageInfoList *
page_info_list_new(PageInfo *pi, uint64_t hash);

/** Add a new element to the head of the list.
 * @param pi The @ref PageInfo to add. From this point it is the property of the list,
 *           so deleting the list deletes this element.
 * @param hash
 *
 * @return A pointer to the first element of the list, or NULL if failure
 * */
PageInfoList *
page_info_list_cons(PageInfoList *pil, PageInfo *pi, uint64_t hash);

/** Deletes the list and all its contents */
void
page_info_list_delete(PageInfoList *pil);

/// @}

/// @addtogroup PageDB
/// @{
#define PAGE_DB_DEFAULT_SIZE 100*MB /**< Initial size of the mmap region */

typedef enum {
     page_db_error_ok = 0,       /**< No error */
     page_db_error_memory,       /**< Error allocating memory */
     page_db_error_invalid_path, /**< File system error */
     page_db_error_internal,     /**< Unexpected error */
     page_db_error_no_page       /**< A page was requested but could not be found */
} PageDBError;

#define PAGE_DB_DEFAULT_PERSIST 1 /**< Default @ref PageDB.persist */

/** Page database.
 *
 * We are really talking about 4 diferent key/value databases:
 *   - info:
 *        contains fixed size information about the whole database. Right now
 *        it just contains the number of pages stored.
 *   - hash2idx:
 *        maps URL hash to index. Indices are consecutive identifier for every
 *        page. This allows to map pages to elements inside arrays.
 *   - hash2info:
 *        maps URL hash to a @ref PageInfo structure.
 *   - links:
 *        maps URL index to links indices. This allows us to make a fast streaming
 *        of all links inside a database.
 */
typedef struct {
     /** Path to the database directory */
     char *path;
     /** The transaction manager counts the number of read and write
         transactions active and is capable of safely performing a
         database resize */
     TxnManager* txn_manager;

     /** Track the most crawled domains */
     DomainTemp *domain_temp;

     Error *error;

// Options
// -----------------------------------------------------------------------------
     /** If true, do not delete files after deleting object*/
     int persist;
} PageDB;


/** Hash function used to convert from URL to hash.
 *
 * The hash is a 64 bit number where the first 32 bits are a hash of the domain
 * and the last 32 bits are a hash of the full URL. In this way all URLs whith
 * the same domain get grouped together in the database. This has some good
 * consequences:
 *
 * 1. We can access all pages inside a domain by accessing the first of them in
 *    the database and moving sequentially.
 * 2. When streaming links this improves locality since pages in the same domain
 *    tend to have similar links.
 */
uint64_t
page_db_hash(const char *url);

/** Extract the domain hash from the full hash */
uint32_t
page_db_hash_get_domain(uint64_t hash);

/** Extract the URL hash from the full hash */
uint32_t
page_db_hash_get_url(uint64_t hash);

/** Creates a new database and stores data inside path
 *
 * @param db In case of @ref ::page_db_error_memory *db could be NULL. In case of
 *           other failures it is nevertheles allocated memory so that the error
 *           code and message can be accessed.
 *
 * @param path Path to directory. In case it doesn't exist it will created.
 *             If it exists and a database is already present operations will
 *             resume with the existing database. Note that you must have read,
 *             write and execute permissions for the directory.
 *
 * @return 0 if success, otherwise the error code
 **/
PageDBError
page_db_new(PageDB **db, const char *path);

/** Update @ref PageDB with a new crawled page
 *
 * It performs the following actions:
 * - Compute page hash
 * - If the page is not already into the database:
 *     - It generates a new ID and stores it in hash2idx
 *     - It creates a new PageInfo and stores it in hash2info
 * - If already present if updates the PageInfo inside hash2info
 * - For each link:
 *     - Compute hash
 *     - If already present in the database just retrieves the ID
 *     - If not present:
 *         - Generate new ID and store it in hash2idx
 *         - Creates a new PageInfo and stores it in hash2info
 * - Create or overwrite list of Page ID -> Links ID mapping inside links
 *   database
 *
 * @param db The database to update
 * @param page The information of the crawled page
 * @param page_info_list If not NULL this function will allocate and populate a new
 *                       @ref PageInfoList which contains the @ref PageInfo of the updated pages.
 *                       It is your responsability to call @page_info_list_delete when you no
 *                       longer need this structure.
 * @return 0 if success, otherwise the error code
 */
PageDBError
page_db_add(PageDB *db, const CrawledPage *page, PageInfoList **page_info_list);

/** Retrieve the PageInfo stored inside the database.

    Beware that if not found it will signal success but the PageInfo will be
    NULL
 */
PageDBError
page_db_get_info(PageDB *db, uint64_t hash, PageInfo **pi);

/** Get index for the given URL */
PageDBError
page_db_get_idx(PageDB *db, uint64_t hash, uint64_t *idx);

/** Build a MMapArray with all the scores */
PageDBError
page_db_get_scores(PageDB *db, MMapArray **scores);

/** Get crawl rate for the given domain */
float
page_db_get_domain_crawl_rate(PageDB *db, uint32_t domain_hash);

/** Close database, delete files if it should not be persisted, and free memory */
PageDBError
page_db_delete(PageDB *db);

/** Set persist option for database */
void
page_db_set_persist(PageDB *db, int value);

/** Set domain temperature tracking options */
PageDBError
page_db_set_domain_temp(PageDB *db, size_t n_domains, float window);

/** Dump database to file in human readable format */
PageDBError
page_db_info_dump(PageDB *db, FILE *output);

/** Dump database to file in human readable format */
PageDBError
page_db_links_dump(PageDB *db, FILE *output);
/// @}

/// @addtogroup LinkStream
/// @{

/** Default value for PageDBLinkStream::only_diff_domain */
#define PAGE_DB_LINK_STREAM_DEFAULT_ONLY_DIFF_DOMAIN 1

typedef struct {
     PageDB *db; /** PageDB where links database is stored */
     MDB_cursor *cur; /**< Cursor to the links database */

     uint64_t from; /**< Current page */
     uint64_t *to;  /**< A list of links */
     size_t n_to;   /**< Number of links */
     size_t i_to;   /**< Current position inside @ref to */
     size_t m_to;   /**< Allocated memory for @ref to. It must be that @ref n_to <= @ref m_to. */
     size_t n_diff; /**< Number of out domain links */

     StreamState state;

     /** If true only links that go to a different domain will be streamed */
     int only_diff_domain;
} PageDBLinkStream;

/** Create a new stream from the given PageDB.
 *
 * @param es The new stream or NULL
 * @param db
 * @return 0 if success, otherwise the error code.
 */
PageDBError
page_db_link_stream_new(PageDBLinkStream **es, PageDB *db);

/** Rewind stream to the beginning */
StreamState
page_db_link_stream_reset(void *es);

/** Get next element inside stream.
 *
 * @return @ref ::link_stream_state_next if success
 */
StreamState
page_db_link_stream_next(void *es, Link *link);

/** Delete link stream and free any transaction hold inside the database. */
void
page_db_link_stream_delete(PageDBLinkStream *es);

/// @}

/// @addtogroup HashInfoStream
/// @{

/** Stream over HashInfo inside PageDB */
typedef struct {
     PageDB *db;
     MDB_cursor *cur;   /**< Cursor to info database */
     StreamState state;
} HashInfoStream;

/** Create a new stream */
PageDBError
hashinfo_stream_new(HashInfoStream **st, PageDB *db);

/** Get next element in stream */
StreamState
hashinfo_stream_next(HashInfoStream *st, uint64_t *hash, PageInfo **pi);

/** Free stream */
void
hashinfo_stream_delete(HashInfoStream *st);

/// @}

/// @addtogroup HashIdxStream
/// @{

/** Stream over hash/index pairs inside PageDB */
typedef struct {
     PageDB *db;
     MDB_cursor *cur;   /**< Cursor to the hash2idx database */
     StreamState state;
} HashIdxStream;

/** Create a new stream */
PageDBError
hashidx_stream_new(HashIdxStream **st, PageDB *db);

/** Get next element in stream */
StreamState
hashidx_stream_next(HashIdxStream *st, uint64_t *hash, size_t *idx);

/** Free stream */
void
hashidx_stream_delete(HashIdxStream *st);

/// @}

#if (defined TEST) && TEST
#include "CuTest.h"
CuSuite *
test_page_db_suite(size_t n_pages);
#endif

#endif // __PAGE_DB_H
