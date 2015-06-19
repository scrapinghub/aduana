from _aduana import lib as C_ADUANA
from _aduana import ffi

class PageDBException(Exception):
    @classmethod
    def from_error(cls, c_error):
        return cls(
            message = C_ADUANA.error_message(c_error),
            code = C_ADUANA.error_code(c_error))

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
        self._c_aduana = C_ADUANA

        self._crawled_page = self._c_aduana.crawled_page_new(url)
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

            ret = self._c_aduana.crawled_page_add_link(
                self._crawled_page,
                url,
                ffi.cast("float", score))
            if ret != 0:
                raise PageDBException(
                    "Error inside crawled_page_add_link: returned %d" % ret)

    @property
    def score(self):
        return self._crawled_page.score

    @score.setter
    def score(self, value):
        self._crawled_page.score = value

    @property
    def hash(self):
        ret = None
        phash = ffi.cast('uint64_t *', self._crawled_page.content_hash)
        if phash:
            ret = phash[0]
        return ret

    @hash.setter
    def hash(self, value):
        ret = self._c_aduana.crawled_page_set_hash64(
            self._crawled_page, ffi.cast('uint64_t', value))
        if ret != 0:
            raise PageDBException(
                "Error inside crawled_page_set_hash64: returned %d" % ret)

    def get_links(self):
        links = []
        for i in xrange(self._c_aduana.crawled_page_n_links(self._crawled_page)):
            pLi = self._c_aduana.crawled_page_get_link(self._crawled_page, i)
            links.append((pLi[0].url, pLi[0].score))
        return links

    def __del__(self):
        self._c_aduana.crawled_page_delete(self._crawled_page)

########################################################################
# PageDB Wrappers
########################################################################
class PageDB(object):
    @staticmethod
    def urlhash(url):
        return C_ADUANA.page_db_hash(url)

    def __init__(self, path, persist=0):
        # save to make sure lib is available at destruction time
        self._c_aduana = C_ADUANA

        self._page_db = ffi.new('PageDB **')
        ret = self._c_aduana.page_db_new(self._page_db, path)
        if ret != 0:
            if self._page_db:
                raise PageDBException.from_error(self._page_db[0].error)
            else:
                raise PageDBException("Error inside page_db_new", ret)

        self.persist = persist

    @property
    def persist(self):
        return self._page_db[0].persist

    @persist.setter
    def persist(self, value):
        self._c_aduana.page_db_set_persist(self._page_db[0], value)

    def __del__(self):
        self._c_aduana.page_db_delete(self._page_db[0])


########################################################################
# Scorers
########################################################################
class PageRankScorer(object):
    def __init__(self, page_db):
        self._c_aduana = C_ADUANA

        self._scorer = ffi.new('PageRankScorer **')
        self._c_aduana.page_rank_scorer_new(self._scorer, page_db._page_db[0])

    def __del__(self):
        self._c_aduana.page_rank_scorer_delete(self._scorer[0])

    def setup(self, scorer):
        self._c_aduana.page_rank_scorer_setup(self._scorer[0], scorer)

    @property
    def persist(self):
        return self._scorer[0].persist

    @persist.setter
    def persist(self, value):
        self._c_aduana.page_rank_scorer_set_persist(self._scorer[0], value)

    @property
    def use_content_scores(self):
        return self._scorer[0].use_content_scores

    @use_content_scores.setter
    def use_content_scores(self, value):
        self._c_aduana.page_rank_scorer_set_use_content_scores(self._scorer[0], value)

    @property
    def damping(self):
        return self._scorer[0].page_rank.damping

    @damping.setter
    def damping(self, value):
        self._c_aduana.page_rank_scorer_set_damping(self._scorer[0], value)

class HitsScorer(object):
    def __init__(self, page_db):
        self._c_aduana = C_ADUANA

        self._scorer = ffi.new('HitsScorer **')
        self._c_aduana.hits_scorer_new(self._scorer, page_db._page_db[0])

    def __del__(self):
        self._c_aduana.hits_scorer_delete(self._scorer[0])

    def setup(self, scorer):
        self._c_aduana.hits_scorer_setup(self._scorer[0], scorer)

    @property
    def persist(self):
        return self._scorer[0].persist

    @persist.setter
    def persist(self, value):
        self._c_aduana.hits_scorer_set_persist(self._scorer[0], value)

    @property
    def use_content_scores(self):
        return self._scorer[0].use_content_scores

    @use_content_scores.setter
    def use_content_scores(self, value):
        self._c_aduana.hits_scorer_set_use_content_scores(self._scorer[0], value)

########################################################################
# Scheduler Wrappers
########################################################################
class BFScheduler(object):
    def __init__(self, page_db, persist=0, scorer=None):
        # save to make sure lib is available at destruction time
        self._c_aduana = C_ADUANA

        self._page_db = page_db
        self._page_db.persist = persist

        self._pBF = ffi.new('BFScheduler **')

        ret = self._c_aduana.bf_scheduler_new(self._pBF, self._page_db._page_db[0])
        if ret != 0:
            if self._pBF:
                raise PageDBException.from_error(self._pBF[0].error)
            else:
                raise PageDBException("Error inside bf_scheduler_new", ret)

        self._c_aduana.bf_scheduler_set_persist(self._pBF[0], persist)

        if scorer:
            self._scorer = scorer
            self._scorer.setup(self._pBF[0].scorer)
            ret = self._c_aduana.bf_scheduler_update_start(self._pBF[0])
            if ret != 0:
                raise PageDBException.from_error(self._pBF[0].error)

    def __del__(self):
        ret = self._c_aduana.bf_scheduler_update_stop(self._pBF[0])
        if ret != 0:
            raise PageDBException.from_error(self._pBF[0].error)
        self._c_aduana.bf_scheduler_delete(self._pBF[0])

    def add(self, crawled_page):
        # better to signal this as an error here than in bf_scheduler_add
        if not isinstance(crawled_page, CrawledPage):
            raise PageDBException("argument to function must be a CrawledPage instance")

        ret = self._c_aduana.bf_scheduler_add(self._pBF[0], crawled_page._crawled_page)
        if ret != 0:
            raise PageDBException.from_error(self._pBF[0].error)

    def requests(self, n_pages):
        pReq = ffi.new('PageRequest **')
        ret = self._c_aduana.bf_scheduler_request(self._pBF[0], n_pages, pReq)
        if ret != 0:
            raise PageDBException.from_error(self._pBF[0].error)
        reqs = [ffi.string(pReq[0].urls[i]) for i in xrange(pReq[0].n_urls)]
        self._c_aduana.page_request_delete(pReq[0])
        return reqs

    def set_crawl_rate(self, soft_rate, hard_rate):
        self._c_aduana.bf_scheduler_set_max_domain_crawl_rate(self._pBF[0], soft_rate, hard_rate)

if __name__ == '__main__':
    db = PageDB('./test_python_bindings')
    scorer = PageRankScorer(db)
    bf = BFScheduler(db, scorer=scorer)
    for i in xrange(100000):
        cp = CrawledPage(str(i), [str(i + j) for j in xrange(10)])
        bf.add(cp)
