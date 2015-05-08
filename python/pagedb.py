import ctypes as ct

C_PAGE_DB = ct.cdll.LoadLibrary('libpagedb.so')

C_PAGE_DB.error_message.argtypes = [ct.c_void_p]
C_PAGE_DB.error_message.restype = ct.c_char_p
C_PAGE_DB.error_code.argtypes = [ct.c_void_p]
C_PAGE_DB.error_code.restype = ct.c_int

class PageDBException(Exception):
    @classmethod
    def from_error(cls, c_error):
        return cls(
            message = C_PAGE_DB.error_message(c_error),
            code = C_PAGE_DB.error_code(c_error))

    def __init__(self, message, code=None):
        self.message = message
        self.code = code

    def __str__(self):
        r = self.message
        if self.code:
            r += " (code={0})".format(self.code)
        return r

########################################################################
# CrawledPage Wrappers
########################################################################
class c_LinkInfo(ct.Structure):
    _fields_ = [
        ("url", ct.c_char_p),
        ("score", ct.c_float)
    ]

class c_PageLinks(ct.Structure):
    _fields_ = [
        ("link_info", ct.POINTER(c_LinkInfo)),
        ("n_links", ct.c_size_t),
        ("m_links", ct.c_size_t)
    ]
class c_CrawledPage(ct.Structure):
    _fields_ = [
        ("url", ct.c_char_p),
        ("links", ct.POINTER(c_PageLinks)),
        ("time", ct.c_double),
        ("score", ct.c_float),
        ("content_hash", ct.c_char_p),
        ("content_hash_length", ct.c_size_t)
    ]

C_PAGE_DB.crawled_page_new.argtypes = [ct.c_char_p]
C_PAGE_DB.crawled_page_new.restype = ct.POINTER(c_CrawledPage)

C_PAGE_DB.crawled_page_delete.argtypes = [ct.POINTER(c_CrawledPage)]
C_PAGE_DB.crawled_page_delete.restype = None

C_PAGE_DB.crawled_page_add_link.argtypes = [
    ct.POINTER(c_CrawledPage),
    ct.c_char_p,
    ct.c_float
]
C_PAGE_DB.crawled_page_add_link.restype = ct.c_int

C_PAGE_DB.crawled_page_n_links.argtypes = [ct.POINTER(c_CrawledPage)]
C_PAGE_DB.crawled_page_n_links.restype = ct.c_size_t

C_PAGE_DB.crawled_page_set_hash64.argtypes = [ct.POINTER(c_CrawledPage), ct.c_uint64]
C_PAGE_DB.crawled_page_set_hash64.restype = ct.c_int

C_PAGE_DB.crawled_page_get_link.argtypes = [
    ct.POINTER(c_CrawledPage),
    ct.c_size_t
]
C_PAGE_DB.crawled_page_get_link.restype = ct.POINTER(c_LinkInfo)

class CrawledPage(object):
    def __init__(self, url, links=[]):
        """Parameters:
            - url: a string
            - links: a list where each element can be:
                         a. A link URL
                         b. A pair made from a link URL and a score between 0 and 1
                     If the first option is used then score is assumed 0.0
        """
        # make sure we keep a reference to the module
        self._c_page_db = C_PAGE_DB

        self._crawled_page = self._c_page_db.crawled_page_new(url)
        if not self._crawled_page:
            raise PageDBException(
                "Error inside crawled_page_new: returned NULL")

        for pair in links:
            if (isinstance(pair, basestring)):
                url = pair
                score = 0.0
            else:
                url = pair[0]
                score = pair[1]

            ret = self._c_page_db.crawled_page_add_link(
                self._crawled_page,
                url,
                ct.c_float(score))
            if ret != 0:
                raise PageDBException(
                    "Error inside crawled_page_add_link: returned %d" % ret)

    @property
    def score(self):
        return self._crawled_page.contents.score

    @score.setter
    def score(self, value):
        self._crawled_page.contents.score = value

    @property
    def hash(self):
        ret = None
        phash = ct.cast(self._crawled_page.contents.content_hash,
                        ct.POINTER(ct.c_uint64))
        if phash:
            ret = phash.contents
        return ret

    @hash.setter
    def hash(self, value):
        ret = self._c_page_db.crawled_page_set_hash64(
            self._crawled_page, ct.c_uint64(value))
        if ret != 0:
            raise PageDBException(
                "Error inside crawled_page_set_hash64: returned %d" % ret)

    def get_links(self):
        links = []
        for i in xrange(self._c_page_db.crawled_page_n_links(self._crawled_page)):
            pLi = self._c_page_db.crawled_page_get_link(self._crawled_page, i)
            links.append((pLi.contents.url, pLi.contents.score))
        return links

    def __del__(self):
        self._c_page_db.crawled_page_delete(self._crawled_page)

########################################################################
# PageDB Wrappers
########################################################################
class c_PageDB(ct.Structure):
    _fields_ = [
        ("path", ct.c_char_p),
        ("txn_manager", ct.c_void_p),
        ("error", ct.c_void_p),
        ("persist", ct.c_int)
    ]

C_PAGE_DB.page_db_hash.argtypes = [ct.c_char_p]
C_PAGE_DB.page_db_hash.restype = ct.c_uint64

C_PAGE_DB.page_db_new.argtypes = [
    ct.POINTER(ct.POINTER(c_PageDB)),
    ct.c_char_p
]
C_PAGE_DB.page_db_new.restype = ct.c_int

C_PAGE_DB.page_db_delete.argtypes = [ct.POINTER(c_PageDB)]
C_PAGE_DB.page_db_delete.restype = ct.c_int

C_PAGE_DB.page_db_set_persist.argtypes = [ct.POINTER(c_PageDB), ct.c_int]
C_PAGE_DB.page_db_set_persist.restype = None

class PageDB(object):
    @staticmethod
    def urlhash(url):
        return C_PAGE_DB.page_db_hash(url)

    def __init__(self, path, persist=0):
        # save to make sure lib is available at destruction time
        self._c_page_db = C_PAGE_DB

        self._page_db = ct.POINTER(c_PageDB)()
        ret = self._c_page_db.page_db_new(ct.byref(self._page_db), path)
        if ret != 0:
            if self._page_db:
                raise PageDBException.from_error(self._page_db.contents.error)
            else:
                raise PageDBException("Error inside page_db_new", ret)

        self.persist = persist

    @property
    def persist(self):
        return self._page_db.contents.persist

    @persist.setter
    def persist(self, value):
        self._c_page_db.page_db_set_persist(self._page_db, value)

    def __del__(self):
        self._c_page_db.page_db_delete(self._page_db)


########################################################################
# Scorers
########################################################################
class c_PageRankScorer(ct.Structure):
    _fields_ = [
        ("page_rank", ct.c_void_p),
        ("page_db", ct.c_void_p),
        ("error", ct.c_void_p),
        ("persist", ct.c_int),
        ("use_content_scores", ct.c_int)
    ]

C_PAGE_DB.page_rank_scorer_new.argtypes = [
    ct.POINTER(ct.POINTER(c_PageRankScorer)), ct.POINTER(c_PageDB)]
C_PAGE_DB.page_rank_scorer_new.restype = ct.c_int

C_PAGE_DB.page_rank_scorer_delete.argtypes = [ct.POINTER(c_PageRankScorer)]
C_PAGE_DB.page_rank_scorer_delete.restype = ct.c_int

C_PAGE_DB.page_rank_scorer_setup.argtypes = [ct.POINTER(c_PageRankScorer), ct.c_void_p]
C_PAGE_DB.page_rank_scorer_setup.restype = None

C_PAGE_DB.page_rank_scorer_set_persist.argtypes = [ct.POINTER(c_PageRankScorer), ct.c_int]
C_PAGE_DB.page_rank_scorer_set_persist.restype = None

C_PAGE_DB.page_rank_scorer_set_use_content_scores.argtypes = [ct.POINTER(c_PageRankScorer), ct.c_int]
C_PAGE_DB.page_rank_scorer_set_use_content_scores.restype = None

class PageRankScorer(object):
    def __init__(self, page_db):
        self._c_page_db = C_PAGE_DB

        self._scorer = ct.POINTER(c_PageRankScorer)()
        self._c_page_db.page_rank_scorer_new(ct.byref(self._scorer), page_db._page_db)

    def __del__(self):
        self._c_page_db.page_rank_scorer_delete(self._scorer)

    def setup(self, scorer):
        self._c_page_db.page_rank_scorer_setup(self._scorer, scorer)

    @property
    def persist(self):
        return self._scorer.contents.persist

    @persist.setter
    def persist(self, value):
        self._c_page_db.page_rank_scorer_set_persist(self._scorer, value)

    @property
    def use_content_scores(self):
        return self._scorer.contents.use_content_scores

    @use_content_scores.setter
    def use_content_scores(self, value):
        self._c_page_db.page_rank_scorer_set_use_content_scores(self._scorer, value)


class c_HitsScorer(ct.Structure):
    _fields_ = [
        ("hits", ct.c_void_p),
        ("page_db", ct.c_void_p),
        ("error", ct.c_void_p),
        ("persist", ct.c_int),
        ("use_content_scores", ct.c_int)
    ]

C_PAGE_DB.hits_scorer_new.argtypes = [
    ct.POINTER(ct.POINTER(c_HitsScorer)), ct.POINTER(c_PageDB)]
C_PAGE_DB.hits_scorer_new.restype = ct.c_int

C_PAGE_DB.hits_scorer_delete.argtypes = [ct.POINTER(c_HitsScorer)]
C_PAGE_DB.hits_scorer_delete.restype = ct.c_int

C_PAGE_DB.hits_scorer_setup.argtypes = [ct.POINTER(c_HitsScorer), ct.c_void_p]
C_PAGE_DB.hits_scorer_setup.restype = None

C_PAGE_DB.hits_scorer_set_persist.argtypes = [ct.POINTER(c_HitsScorer), ct.c_int]
C_PAGE_DB.hits_scorer_set_persist.restype = None

C_PAGE_DB.hits_scorer_set_use_content_scores.argtypes = [ct.POINTER(c_HitsScorer), ct.c_int]
C_PAGE_DB.hits_scorer_set_use_content_scores.restype = None

class HitsScorer(object):
    def __init__(self, page_db):
        self._c_page_db = C_PAGE_DB

        self._scorer = ct.POINTER(c_HitsScorer)()
        self._c_page_db.hits_scorer_new(ct.byref(self._scorer), page_db._page_db)

    def __del__(self):
        self._c_page_db.hits_scorer_delete(self._scorer)

    def setup(self, scorer):
        self._c_page_db.hits_scorer_setup(self._scorer, scorer)

    @property
    def persist(self):
        return self._scorer.contents.persist

    @persist.setter
    def persist(self, value):
        self._c_page_db.hits_scorer_set_persist(self._scorer, value)

    @property
    def use_content_scores(self):
        return self._scorer.contents.use_content_scores

    @use_content_scores.setter
    def use_content_scores(self, value):
        self._c_page_db.hits_scorer_set_use_content_scores(self._scorer, value)

########################################################################
# Scheduler Wrappers
########################################################################
class c_PageRequest(ct.Structure):
    _fields_ = [
        ("urls", ct.POINTER(ct.c_char_p)),
        ("n_urls", ct.c_size_t)
    ]

C_PAGE_DB.page_request_new.argtypes = [ct.c_size_t]
C_PAGE_DB.page_request_new.restype = ct.POINTER(c_PageRequest)

C_PAGE_DB.page_request_delete.argtypes = [ct.POINTER(c_PageRequest)]
C_PAGE_DB.page_request_delete.restype = None

C_PAGE_DB.page_request_add_url.argtypes = [ct.POINTER(c_PageRequest), ct.c_char_p]
C_PAGE_DB.page_request_delete.restype = ct.c_int

class c_BFScheduler(ct.Structure):
    _fields_ = [
        ("page_db", ct.POINTER(c_PageDB)),
        ("scorer", ct.c_void_p),
        ("txn_manager", ct.c_void_p),
        ("path", ct.c_char_p),
        ("update_thread", ct.c_void_p),
        ("error", ct.c_void_p),
        ("persist", ct.c_int)
    ]
C_PAGE_DB.bf_scheduler_new.argtypes = [
    ct.POINTER(ct.POINTER(c_BFScheduler)), ct.POINTER(c_PageDB)]
C_PAGE_DB.bf_scheduler_new.restype = ct.c_int

C_PAGE_DB.bf_scheduler_add.argtypes = [
    ct.POINTER(c_BFScheduler), ct.POINTER(c_CrawledPage)]
C_PAGE_DB.bf_scheduler_add.restype = ct.c_int

C_PAGE_DB.bf_scheduler_request.argtypes = [
    ct.POINTER(c_BFScheduler),
    ct.c_size_t,
    ct.POINTER(ct.POINTER(c_PageRequest))
]
C_PAGE_DB.bf_scheduler_request.restype = ct.c_int

C_PAGE_DB.bf_scheduler_delete.argtypes = [ct.POINTER(c_BFScheduler)]
C_PAGE_DB.bf_scheduler_delete.restype = None

C_PAGE_DB.bf_scheduler_update_start.argtypes = [ct.POINTER(c_BFScheduler)]
C_PAGE_DB.bf_scheduler_update_start.restype = ct.c_int

C_PAGE_DB.bf_scheduler_update_stop.argtypes = [ct.POINTER(c_BFScheduler)]
C_PAGE_DB.bf_scheduler_update_stop.restype = ct.c_int

C_PAGE_DB.bf_scheduler_set_persist.argtypes = [ct.POINTER(c_BFScheduler), ct.c_int]
C_PAGE_DB.bf_scheduler_set_persist.restype = None

class BFScheduler(object):
    def __init__(self, page_db, persist=0, scorer=None):
        # save to make sure lib is available at destruction time
        self._c_page_db = C_PAGE_DB

        self._page_db = page_db
        self._page_db.persist = persist

        self._pBF = ct.POINTER(c_BFScheduler)()

        ret = self._c_page_db.bf_scheduler_new(
            ct.byref(self._pBF), self._page_db._page_db)
        if ret != 0:
            if self._pBF:
                raise PageDBException.from_error(self._pBF.contents.error)
            else:
                raise PageDBException("Error inside bf_scheduler_new", ret)

        self._c_page_db.bf_scheduler_set_persist(self._pBF, persist)

        if scorer:
            self._scorer = scorer
            self._scorer.setup(self._pBF.contents.scorer)
            ret = self._c_page_db.bf_scheduler_update_start(self._pBF)
            if ret != 0:
                raise PageDBException.from_error(self._pBF.contents.error)

    def __del__(self):
        ret = self._c_page_db.bf_scheduler_update_stop(self._pBF)
        if ret != 0:
            raise PageDBException.from_error(self._pBF.contents.error)
        self._c_page_db.bf_scheduler_delete(self._pBF)

    def add(self, crawled_page):
        # better to signal this as an error here than in bf_scheduler_add
        if not isinstance(crawled_page, CrawledPage):
            raise PageDBException("argument to function must be a CrawledPage instance")

        ret = self._c_page_db.bf_scheduler_add(self._pBF, crawled_page._crawled_page)
        if ret != 0:
            raise PageDBException.from_error(self._pBF.contents.error)

    def requests(self, n_pages):
        pReq = ct.POINTER(c_PageRequest)()
        ret = self._c_page_db.bf_scheduler_request(self._pBF, n_pages, ct.byref(pReq))
        if ret != 0:
            raise PageDBException.from_error(self._pBF.contents.error)
        reqs = [pReq.contents.urls[i] for i in xrange(pReq.contents.n_urls)]
        self._c_page_db.page_request_delete(pReq)
        return reqs


if __name__ == '__main__':
    db = PageDB('./test_python_bindings')
    scorer = PageRankScorer(db)
    bf = BFScheduler(db, scorer=scorer)
    for i in xrange(100000):
        cp = CrawledPage(str(i), [str(i + j) for j in xrange(10)])
        bf.add(cp)
