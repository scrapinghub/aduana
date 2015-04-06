#ifndef __PAGE_DB_H__
#define __PAGE_DB_H__

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
#define GB (1024*MB

/** The information that comes with a crawled page */
typedef struct {
     char *url;                   /**< ASCII, null terminated string for the page URL*/
     char **links;                /**< Each links[i] is also a URL */
     size_t n_links;              /**< Number of links */
     time_t time;                 /**< UNIX time of the crawl */
     float score;                 /**< A number giving an idea of the page content's value */
     char *content_hash;          /**< A hash to detect content change since last crawl. Arbitrary byte sequence */
     size_t content_hash_length;  /**< Number of byes of the content_hash */
} CrawledPage;

/// @addtogroup PageInfo
/// @{

/** The information we keep about crawled and uncrawled pages */
typedef struct {
     char *url;                   /**< A copy of either @ref CrawledPage::url or @ref CrawledPage::links[i] */
     time_t first_crawl;          /**< First UNIX time this page was crawled */
     time_t last_crawl;           /**< Last UNIX time this page was crawled */
     size_t n_changes;            /**< Number of content changes detected between first and last crawl */
     size_t n_crawls;             /**< Number of times this page has crawled. Can be zero if it has been observed just as a link*/
     float score;                 /**< A copy of the same field at the last crawl */
     size_t content_hash_length;  /**< Number of bytes in @ref content_hash */
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

/** Destroy PageInfo if not NULL, otherwise does nothing */
void
page_info_delete(PageInfo *pi);
/// @}

/// @addtogroup PageDB
/// @{
#define PAGE_DB_MAX_ERROR_LENGTH 10000
#define PAGE_DB_DEFAULT_SIZE 100*MB

typedef enum {
     page_db_error_ok = 0,       /**< No error */
     page_db_error_memory,       /**< Error allocating memory */
     page_db_error_invalid_path, /**< File system error */
     page_db_error_internal,     /**< Unexpected error */
     page_db_error_no_page       /**< A page was requested but could not be found */
} PageDBError;

/** Function to call when a PageInfo is modified */
typedef int (SchedulerAddFunc)(MDB_cursor *cur, MDB_val *hash, PageInfo *pi);
/** Function to call to retrieve a new page */
typedef int (SchedulerGetFunc)(MDB_cursor *cur, MDB_val *hash);

// TODO Make the building of the links database optional. The are many more links
// that pages and it takes lot of space to store this structure. We should only
// build the links database if we are going to use them, for example, to compute
// PageRank or HITS scores.

/** Page database.
 *
 * We are really talking about 5 diferent key/value databases:
 *   - info
 *        Contains fixed size information about the whole database. Right now
 *        it just contains the number pages stored.
 *   - hash2idx
 *        Maps URL hash to index. Indices are consecutive identifier for every
 *        page. This allows to map pages to elements inside arrays.
 *   - hash2info
 *        Maps URL hash to a @ref PageInfo structure.
 *   - links
 *        Maps URL index to links indices. This allows us to make a fast streaming
 *        of all links inside a database.
 *   - schedule
 *        Maps score (float) to hash
 */
typedef struct {
     MDB_env *env;

     SchedulerAddFunc *sched_add;
     SchedulerGetFunc *sched_get;

     PageDBError error;
     /** A descriptive message associated with @ref error */
     char error_msg[PAGE_DB_MAX_ERROR_LENGTH+1];
} PageDB;

/** Creates a new database and stores data inside path
 *
 * @param db In case of @ref ::page_db_error_memory *db could be NULL, otherwise
 *           it is allocated memory so that the @ref PageDB::error_msg can be
 *           accessed and its your responsability to call @ref page_db_delete.
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
 * It perform the following actions:
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
 * @return 0 if success, otherwise the error code
 */
PageDBError
page_db_add(PageDB *db, const CrawledPage *page);

/** Retrieve the PageInfo stored inside the database.

    Beware that if not found it will signal success but the PageInfo will be
    NULL
 */
PageDBError
page_db_get_info(PageDB *db, const char *url, PageInfo **pi);

/** Get index for the given URL */
PageDBError
page_db_get_idx(PageDB *db, const char *url, uint64_t *idx);

/** Retrieve an array of the next pages that should be crawled, 
    according to the scheduler.

    @param db The database
    @param n_pages The maximum number of pages
    @param pi An array of @ref n_pages @ref PageInfo pointers. 
              If less than @ref n_pages are retrieved the remaining elements of 
	      the array are set to NULL
 */
PageDBError
page_db_request(PageDB *db, size_t n_pages, PageInfo **pi);

/** Close database */
void
page_db_delete(PageDB *db);
/// @}

/// @addtogroup LinkStream
/// @{

typedef struct {
     int64_t from;
     int64_t to;
} Link;

typedef enum {
     link_stream_state_init, /**< Stream ready */
     link_stream_state_next, /**< A new element has been obtained */
     link_stream_state_end,  /**< No more elements */
     link_stream_state_error /**< Unexpected error */
} LinkStreamState;

typedef LinkStreamState (LinkStreamNextFunc)(void *state, Link *link);
typedef LinkStreamState (LinkStreamResetFunc)(void *state);

typedef struct {
     MDB_cursor *cur; /**< Cursor to the links database */

     uint64_t from; /**< Current page */
     uint64_t *to;  /**< A list of links */
     size_t n_to;   /**< Number of links */
     size_t i_to;   /**< Current position inside @ref to */
     size_t m_to;   /**< Allocated memory for @ref to. It must be that @ref n_to <= @ref m_to. */

     LinkStreamState state;
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
LinkStreamState
page_db_link_stream_reset(PageDBLinkStream *es);

/** Get next element inside stream.
 *
 * @return @ref ::link_stream_state_next if success
 */
LinkStreamState
page_db_link_stream_next(PageDBLinkStream *es, Link *link);

/** Delete link stream and free any transaction hold inside the database. */
void
page_db_link_stream_delete(PageDBLinkStream *es);
/// @}

char *
build_path(const char *path, const char *fname);

#if TEST
#include "CuTest.h"
CuSuite *
test_page_db_suite(void);
#endif

#endif // __PAGE_DB_H
