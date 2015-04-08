import ctypes as ct
import shutil

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

c_page_db.crawled_page_get_link.argtypes = [
    ct.POINTER(c_CrawledPage),
    ct.c_size_t
]
c_page_db.crawled_page_get_link.restype = ct.POINTER(c_LinkInfo)

class CrawledPage(object):
    def __init__(self, url, links=[]):
        self._cp = c_page_db.crawled_page_new(url)
        for url, score in links:
            c_page_db.crawled_page_add_link(
                self._cp,
                url,
                ct.c_float(score))

    @property
    def score(self):
        return self._cp.contents.score

    @score.setter
    def score(self, value):
        self._cp.contents.score = value

    def get_links(self):
        links = []
        for i in xrange(c_page_db.crawled_page_n_links(self._cp)):
            pLi = c_page_db.crawled_page_get_link(self._cp, i)
            links.append((pLi.contents.url, pLi.contents.score))
        return links

    def __del__(self):
        c_page_db.crawled_page_delete(self._cp)


class c_PageInfo(ct.Structure):
    _fields_ = [
        ("url", ct.c_char_p),
        ("first_crawl", ct.c_double),
        ("last_crawl", ct.c_double),
        ("n_changes", ct.c_size_t),
        ("n_crawls", ct.c_size_t),
        ("score", ct.c_float),
        ("content_hash_length", ct.c_char_p)
    ]

c_page_db.page_info_delete.argtypes = [ct.POINTER(c_PageInfo)]
c_page_db.page_info_delete.restype = None

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

c_SchedulerAddFunc = ct.CFUNCTYPE(ct.c_int, ct.c_void_p, ct.c_void_p, ct.POINTER(c_PageInfo))
c_SchedulerGetFunc = ct.CFUNCTYPE(ct.c_int, ct.c_void_p, ct.c_void_p)

c_PAGE_DB_MAX_ERROR_LENGTH = 10000
class c_PageDB(ct.Structure):
    _fields_ = [
        ("env", ct.c_void_p),
        ("sched_add", c_SchedulerAddFunc),
        ("sched_del", c_SchedulerGetFunc),
        ("error", ct.c_int),
        ("error_msg", ct.c_char*c_PAGE_DB_MAX_ERROR_LENGTH)
    ]

c_page_db.page_db_new.argtypes = [
    ct.POINTER(ct.POINTER(c_PageDB)),
    ct.c_char_p
]
c_page_db.page_db_new.restype = ct.c_int
c_page_db.page_db_delete.argtypes = [ct.POINTER(c_PageDB)]
c_page_db.page_db_delete.restype = None
c_page_db.page_db_add.argtypes = [
    ct.POINTER(c_PageDB),
    ct.POINTER(c_CrawledPage)
]
c_page_db.page_db_add.restype = ct.c_int
c_page_db.page_db_get_info.argtypes = [
    ct.POINTER(c_PageDB),
    ct.c_char_p,
    ct.POINTER(ct.POINTER(c_PageInfo))
]
c_page_db.page_db_get_info.restype = ct.c_int
c_page_db.page_db_request.argtypes = [
    ct.POINTER(c_PageDB),
    ct.c_size_t,
    ct.POINTER(ct.POINTER(c_PageRequest))
]

class PageDB(object):
    def __init__(self, path):
        # save to make sure lib is available at destruction time
        self._c = c_page_db
        self._db = ct.POINTER(c_PageDB)()
        self._c.page_db_new(ct.byref(self._db), path)

    def __del__(self):
        self._c.page_db_delete(self._db)

    def add(self, crawled_page):
        self._c.page_db_add(self._db, crawled_page._cp)

    def request(self, n_pages):
        req = ct.POINTER(c_PageRequest)()
        rc = self._c.page_db_request(
            self._db,
            n_pages,
            ct.byref(req))
        if rc != 0:
            return []

        reqs = []
        for i in xrange(req.contents.n_urls):
            if not req.contents.urls[i]:
                break
            reqs.append(req.contents.urls[i])
        self._c.page_request_delete(req)

        return reqs

if __name__ == '__main__':
    db = PageDB("./test_python_bindings")
    db.add(CrawledPage("bar", [("x", 1.0), ("y", 0.4)]))
    db.add(CrawledPage("foo", [("a", 1.0), ("b", 0.5)]))
    db.add(CrawledPage("a"))

    print "Expected:", ["x", "b", "y"]
    print "  Actual:", db.request(10)

    shutil.rmtree("./test_python_bindings")
