import ctypes as ct

c_page_db = ct.cdll.LoadLibrary('../debug/libpagedb.so')

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

c_page_db.crawled_page_new.argtypes = [ct.c_char_p]
c_page_db.crawled_page_new.restype = ct.POINTER(c_CrawledPage)

c_page_db.crawled_page_delete.argtypes = [ct.POINTER(c_CrawledPage)]
c_page_db.crawled_page_delete.restype = None

c_page_db.crawled_page_add_link.argtypes = [
    ct.POINTER(c_CrawledPage),
    ct.c_char_p,
    ct.c_float
]
c_page_db.crawled_page_add_link.restype = ct.c_int

c_page_db.crawled_page_n_links.argtypes = [ct.POINTER(c_CrawledPage)]
c_page_db.crawled_page_n_links.restype = ct.c_size_t

c_page_db.crawled_page_set_hash64.argtypes = [ct.POINTER(c_CrawledPage), ct.c_uint64]
c_page_db.crawled_page_set_hash64.restype = ct.c_int

c_page_db.crawled_page_get_link.argtypes = [
    ct.POINTER(c_CrawledPage),
    ct.c_size_t
]
c_page_db.crawled_page_get_link.restype = ct.POINTER(c_LinkInfo)

class CrawledPage(object):
    def __init__(self, url, links=[]):
        self._c = c_page_db
        self._cp = self._c.crawled_page_new(url)
        for pair in links:
            if (isinstance(pair, basestring)):
                url = pair
                score = 0.0
            else:
                url = pair[0]
                score = pair[1]

            self._c.crawled_page_add_link(
                self._cp,
                url,
                ct.c_float(score))

    @property
    def score(self):
        return self._cp.contents.score

    @score.setter
    def score(self, value):
        self._cp.contents.score = value

    @property
    def hash(self):
        ret = None
        phash = ct.cast(self._cp.contents.content_hash,
                        ct.POINTER(ct.c_uint64))
        if phash:
            ret = phash.contents
        return ret

    @hash.setter
    def hash(self, value):
        self._c.crawled_page_set_hash64(self._cp, ct.c_uint64(value))

    def get_links(self):
        links = []
        for i in xrange(self._c.crawled_page_n_links(self._cp)):
            pLi = self._c.crawled_page_get_link(self._cp, i)
            links.append((pLi.contents.url, pLi.contents.score))
        return links

    def __del__(self):
        self._c.crawled_page_delete(self._cp)

class c_PageDB(ct.Structure):
    _fields_ = [
        ("path", ct.c_char_p),
        ("txn_manager", ct.c_void_p),
        ("error", ct.c_void_p),
        ("persist", ct.c_int)
    ]

c_page_db.page_db_hash.argtypes = [
    ct.c_char_p
]
c_page_db.page_db_hash.restype = ct.c_uint64

c_page_db.page_db_new.argtypes = [
    ct.POINTER(ct.POINTER(c_PageDB)),
    ct.c_char_p
]
c_page_db.page_db_new.restype = ct.c_int
c_page_db.page_db_delete.argtypes = [ct.POINTER(c_PageDB)]
c_page_db.page_db_delete.restype = ct.c_int
c_page_db.page_db_set_persist.argtypes = [ct.POINTER(c_PageDB), ct.c_int]
c_page_db.page_db_set_persist.restype = None

class PageDB(object):
    @classmethod
    def urlhash(cls, url):
        return c_page_db.page_db_hash(url)

    def __init__(self, path, persist=0):
        # save to make sure lib is available at destruction time
        self._c = c_page_db
        self._db = ct.POINTER(c_PageDB)()
        self._c.page_db_new(ct.byref(self._db), path)
        self.persist = persist

    @property
    def persist(self):
        return self._db.contents.persist

    @persist.setter
    def persist(self, value):
        self._db.contents.persist = value

    def __del__(self):
        self._c.page_db_delete(self._db)

c_pPageRankScorer = ct.c_void_p
c_page_db.page_rank_scorer_new.argtypes = [
    ct.POINTER(c_pPageRankScorer), ct.POINTER(c_PageDB)]
c_page_db.page_rank_scorer_new.restype = ct.c_int
c_page_db.page_rank_scorer_delete.argtypes = [c_pPageRankScorer]
c_page_db.page_rank_scorer_delete.restype = ct.c_int
c_page_db.page_rank_scorer_setup.argtypes = [c_pPageRankScorer, ct.c_void_p]
c_page_db.page_rank_scorer_setup.restype = None

class PageRankScorer(object):
    def __init__(self, page_db):
        self._c = c_page_db

        self._pRS = c_pPageRankScorer()
        self._c.page_rank_scorer_new(ct.byref(self._pRS), page_db._db)

    def __del__(self):
        self._c.page_rank_scorer_delete(self._pRS)

    def setup(self, scorer):
        self._c.page_rank_scorer_setup(self._pRS, scorer)

class c_PageRequest(ct.Structure):
    _fields_ = [
        ("urls", ct.POINTER(ct.c_char_p)),
        ("n_urls", ct.c_size_t)
    ]

c_page_db.page_request_new.argtypes = [ct.c_size_t]
c_page_db.page_request_new.restype = ct.POINTER(c_PageRequest)

c_page_db.page_request_delete.argtypes = [ct.POINTER(c_PageRequest)]
c_page_db.page_request_delete.restype = None

c_page_db.page_request_add_url.argtypes = [ct.POINTER(c_PageRequest), ct.c_char_p]
c_page_db.page_request_delete.restype = ct.c_int

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
c_page_db.bf_scheduler_new.argtypes = [
    ct.POINTER(ct.POINTER(c_BFScheduler)), ct.POINTER(c_PageDB)]
c_page_db.bf_scheduler_new.restype = ct.c_int
c_page_db.bf_scheduler_add.argtypes = [
    ct.POINTER(c_BFScheduler), ct.POINTER(c_CrawledPage)]
c_page_db.bf_scheduler_add.restype = ct.c_int
c_page_db.bf_scheduler_request.argtypes = [
    ct.POINTER(c_BFScheduler),
    ct.c_size_t,
    ct.POINTER(ct.POINTER(c_PageRequest))
]
c_page_db.bf_scheduler_request.restype = ct.c_int
c_page_db.bf_scheduler_delete.argtypes = [ct.POINTER(c_BFScheduler)]
c_page_db.bf_scheduler_delete.restype = None
c_page_db.bf_scheduler_update_start.argtypes = [ct.POINTER(c_BFScheduler)]
c_page_db.bf_scheduler_update_start.restype = None
c_page_db.bf_scheduler_update_stop.argtypes = [ct.POINTER(c_BFScheduler)]
c_page_db.bf_scheduler_update_stop.restype = None
c_page_db.bf_scheduler_set_persist.argtypes = [ct.POINTER(c_BFScheduler), ct.c_int]
c_page_db.bf_scheduler_set_persist.restype = None

class BFScheduler(object):
    def __init__(self, path, persist=0, scorer_class=None):
        # save to make sure lib is available at destruction time
        self._c = c_page_db

        self._db = PageDB(path, persist)
        self._pBF = ct.POINTER(c_BFScheduler)()

        self._c.bf_scheduler_new(ct.byref(self._pBF), self._db._db)
        self._c.bf_scheduler_set_persist(self._pBF, persist)

        if scorer_class:
            self._scorer = scorer_class(self._db)
            self._scorer.setup(self._pBF.contents.scorer)
            self._c.bf_scheduler_update_start(self._pBF)

    def __del__(self):
        self._c.bf_scheduler_update_stop(self._pBF)
        self._c.bf_scheduler_delete(self._pBF)

    def add(self, crawled_page):
        self._c.bf_scheduler_add(self._pBF, crawled_page._cp)

    def requests(self, n_pages):
        pReq = ct.POINTER(c_PageRequest)()
        self._c.bf_scheduler_request(self._pBF, n_pages, ct.byref(pReq))
        reqs = [pReq.contents.urls[i] for i in xrange(pReq.contents.n_urls)]
        self._c.page_request_delete(pReq)
        return reqs


if __name__ == '__main__':
    bf = BFScheduler("./test_python_bindings", scorer_class=PageRankScorer)
    for i in xrange(100000):
        cp = CrawledPage(str(i), [str(i + j) for j in xrange(10)])
        bf.add(cp)
        if i % 10 == 0:
            for url in bf.requests(10):
                print url,', ',
            print
