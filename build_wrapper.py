import platform
import cffi

aduana_src_root = 'lib/src/'
aduana_lib_root = 'lib/lib/'

aduana_src = [
    aduana_lib_root + x for x in [
        'smaz.c',
        'xxhash.c',
        'lmdb/mdb.c',
        'lmdb/midl.c',
    ]] + [
    aduana_src_root + x for x in [
        'mmap_array.c',
        'page_db.c',
        'hits.c',
        'page_rank.c',
        'scheduler.c',
        'bf_scheduler.c',
        'util.c',
        'page_rank_scorer.c',
        'hits_scorer.c',
        'txn_manager.c',
        'domain_temp.c',
        'freq_scheduler.c',
        'freq_algo.c'
    ]]

if platform.system() == 'Windows':
    aduana_src.append(aduana_lib_root + 'mman.c')

aduana_include = [
    aduana_lib_root,
    aduana_lib_root + 'lmdb',
    aduana_src_root
]

aduana_define = [
    ('MDB_MAXKEYSIZE', 500)
]

aduana_compile_args = [
    '-std=c99',
    '-m64',
    '-msse2',
    '-pthread'
]

aduana_libraries = ['m']

ffi = cffi.FFI()
ffi.set_source(
    '_aduana',
    '''
    #include "bf_scheduler.h"
    #include "domain_temp.h"
    #include "hits.h"
    #include "hits_scorer.h"
    #include "link_stream.h"
    #include "mmap_array.h"
    #include "page_db.h"
    #include "page_rank.h"
    #include "page_rank_scorer.h"
    #include "scheduler.h"
    #include "txn_manager.h"
    #include "util.h"
    #include "freq_scheduler.h"
    #include "freq_algo.h"
    ''',
    sources            = aduana_src,
    include_dirs       = aduana_include,
    define_macros      = aduana_define,
    extra_compile_args = aduana_compile_args,
    libraries          = aduana_libraries
)

ffi.cdef(
    """
    const char *
    error_message(const void *error);

    int
    error_code(const void *error);
    """
)

ffi.cdef(
    """
    typedef struct {
         char *url;
         uint64_t linked_from;
         uint64_t depth;
         double first_crawl;
         double last_crawl;
         uint64_t n_changes;
         uint64_t n_crawls;
         float score;
         uint64_t content_hash_length;
         char *content_hash;
    } PageInfo;

    float
    page_info_rate(const PageInfo *pi);

    int
    page_info_is_seed(const PageInfo *pi);

    void
    page_info_delete(PageInfo *pi);

    typedef struct {
        char *url;   /**< ASCII, null terminated string for the page URL*/
        float score; /**< An estimated value of the link score */
    } LinkInfo;

    typedef struct {
         LinkInfo *link_info; /**< Array of LinkInfo */
         size_t n_links;      /**< Number of items inside link_info */
         size_t m_links;      /**< Maximum number of items that can be stored inside link_info */
    } PageLinks;

    typedef struct {
        char *url;                   /**< ASCII, null terminated string for the page URL*/
        PageLinks *links;            /**< List of links inside this page */
        double time;                 /**< Number of seconds since epoch */
        float score;                 /**< A number giving an idea of the page content's value */
        char *content_hash;          /**< A hash to detect content change since last crawl.
                                          Arbitrary byte sequence */
        size_t content_hash_length;  /**< Number of byes of the content_hash */
    } CrawledPage;

    CrawledPage *
    crawled_page_new(const char *url);

    void
    crawled_page_delete(CrawledPage *cp);

    int
    crawled_page_add_link(CrawledPage *cp, const char *url, float score);

    size_t
    crawled_page_n_links(const CrawledPage *cp);

    int
    crawled_page_set_hash64(CrawledPage *cp, uint64_t hash);

    const LinkInfo *
    crawled_page_get_link(const CrawledPage *cp, size_t i);

    typedef enum {
         page_db_error_ok = 0,       /**< No error */
         page_db_error_memory,       /**< Error allocating memory */
         page_db_error_invalid_path, /**< File system error */
         page_db_error_internal,     /**< Unexpected error */
         page_db_error_no_page       /**< A page was requested but could not be found */
    } PageDBError;

    typedef struct {
         char *path;
         void* txn_manager;
         void *domain_temp;
         void *error;
         int persist;
    } PageDB;

    uint64_t
    page_db_hash(const char *url);

    PageDBError
    page_db_new(PageDB **db, const char *path);

    PageDBError
    page_db_get_info(PageDB *db, uint64_t hash, PageInfo **pi);

    PageDBError
    page_db_add(PageDB *db, const CrawledPage *page, void **page_info_list);

    PageDBError
    page_db_delete(PageDB *db);

    void
    page_db_set_persist(PageDB *db, int value);

    typedef enum {
         stream_state_init,
         stream_state_next,
         stream_state_end,
         stream_state_error
     } StreamState;

    typedef struct {
         PageDB *db;
         void *cur;
         StreamState state;
    } HashInfoStream;

    PageDBError
    hashinfo_stream_new(HashInfoStream **st, PageDB *db);

    StreamState
    hashinfo_stream_next(HashInfoStream *st, uint64_t *hash, PageInfo **pi);

    void
    hashinfo_stream_delete(HashInfoStream *st);
    """
)

ffi.cdef(
    """
    typedef enum {
         page_rank_scorer_error_ok = 0,    /**< No error */
         page_rank_scorer_error_memory,    /**< Error allocating memory */
         page_rank_scorer_error_internal,  /**< Unexpected error */
         page_rank_scorer_error_precision  /**< Could not achieve precision in maximum number of loops */
    } PageRankScorerError;

    typedef struct {
         void *page_rank;
         PageDB *page_db;
         void *error;
         int persist;
         int use_content_scores;
    } PageRankScorer;

    PageRankScorerError
    page_rank_scorer_new(PageRankScorer **prs, PageDB *db);

    PageRankScorerError
    page_rank_scorer_delete(PageRankScorer *prs);

    void
    page_rank_scorer_setup(PageRankScorer *prs, void *scorer);

    void
    page_rank_scorer_set_persist(PageRankScorer *prs, int value);

    void
    page_rank_scorer_set_use_content_scores(PageRankScorer *prs, int value);

    void
    page_rank_scorer_set_damping(PageRankScorer *prs, float value);
    """
)

ffi.cdef(
    """
    typedef enum {
         hits_scorer_error_ok = 0,   /**< No error */
         hits_scorer_error_memory,   /**< Error allocating memory */
         hits_scorer_error_internal, /**< Unexpected error */
         hits_scorer_error_precision /**< Could not achieve precision in maximum number of loops */
    } HitsScorerError;

    typedef struct {
         void *hits;
         PageDB *page_db;
         void *error;
         int persist;
         int use_content_scores;
    } HitsScorer;

    HitsScorerError
    hits_scorer_new(HitsScorer **hs, PageDB *db);

    HitsScorerError
    hits_scorer_delete(HitsScorer *hs);

    void
    hits_scorer_setup(HitsScorer *hs, void *scorer);

    void
    hits_scorer_set_persist(HitsScorer *hs, int value);

    void
    hits_scorer_set_use_content_scores(HitsScorer *hs, int value);
    """
)

ffi.cdef(
    """
    typedef struct {
         char **urls;
         size_t n_urls;
    } PageRequest;

    PageRequest*
    page_request_new(size_t n_urls);

    void
    page_request_delete(PageRequest *req);

    int
    page_request_add_url(PageRequest *req, const char *url);
    """
)

ffi.cdef(
    """
    typedef enum {
         bf_scheduler_error_ok = 0,       /**< No error */
         bf_scheduler_error_memory,       /**< Error allocating memory */
         bf_scheduler_error_invalid_path, /**< File system error */
         bf_scheduler_error_internal,     /**< Unexpected error */
         bf_scheduler_error_thread        /**< Error inside the threading library */
    } BFSchedulerError;

    typedef struct {
         PageDB *page_db;
         void *scorer;
         void *txn_manager;
         char *path;
         void *update_thread;
         void *error;
         int persist;
         float max_soft_domain_crawl_rate;
         float max_hard_domain_crawl_rate;
         uint64_t max_crawl_depth;
    } BFScheduler;

    BFSchedulerError
    bf_scheduler_new(BFScheduler **sch, PageDB *db, const char *path);

    BFSchedulerError
    bf_scheduler_add(BFScheduler *sch, const CrawledPage *page);

    BFSchedulerError
    bf_scheduler_request(BFScheduler *sch, size_t n_pages, PageRequest **request);

    void
    bf_scheduler_delete(BFScheduler *sch);

    BFSchedulerError
    bf_scheduler_update_start(BFScheduler *sch);

    BFSchedulerError
    bf_scheduler_update_stop(BFScheduler *sch);

    void
    bf_scheduler_set_persist(BFScheduler *sch, int value);

    BFSchedulerError
    bf_scheduler_set_max_domain_crawl_rate(BFScheduler *sch,
                                           float max_soft_crawl_rate,
                                           float max_hard_crawl_rate);
    void
    bf_scheduler_set_max_crawl_depth(BFScheduler *sch, uint64_t value);

    typedef int... time_t;

    void
    bf_scheduler_set_update_interval(BFScheduler *sch, time_t value);
    """
)

ffi.cdef(
    """
    typedef enum {
         freq_scheduler_error_ok = 0,       /**< No error */
         freq_scheduler_error_memory,       /**< Error allocating memory */
         freq_scheduler_error_invalid_path, /**< File system error */
         freq_scheduler_error_internal     /**< Unexpected error */
    } FreqSchedulerError;

    typedef struct {
         char *path;
         PageDB *page_db;
         void *txn_manager;
         void *error;
         int persist;
         float margin;
         size_t max_n_crawls;
    } FreqScheduler;

    FreqSchedulerError
    freq_scheduler_new(FreqScheduler **sch, PageDB *db, const char *path);

    FreqSchedulerError
    freq_scheduler_load_simple(FreqScheduler *sch,
                               float freq_default,
                               float freq_scale);
    FreqSchedulerError
    freq_scheduler_load_mmap(FreqScheduler *sch, void *freqs);

    FreqSchedulerError
    freq_scheduler_request(FreqScheduler *sch,
                           size_t max_requests,
                           PageRequest **request);

    FreqSchedulerError
    freq_scheduler_add(FreqScheduler *sch, const CrawledPage *page);

    void
    freq_scheduler_delete(FreqScheduler *sch);

    FreqSchedulerError
    freq_scheduler_cursor_open(FreqScheduler *sch, void **cursor);

    FreqSchedulerError
    freq_scheduler_cursor_commit(FreqScheduler *sch, void *cursor);

    void
    freq_scheduler_cursor_abort(FreqScheduler *sch, void *cursor);

    FreqSchedulerError
    freq_scheduler_cursor_write(FreqScheduler *sch,
                                void *cursor,
                                uint64_t hash,
                                float freq);
    """
)

ffi.cdef(
    """
    int
    freq_algo_simple(PageDB *db, void **freqs, const char *path, char **error_msg);
    """
)

if __name__ == '__main__':
    ffi.compile()
