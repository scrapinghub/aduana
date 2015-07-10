import functools
import warnings
import re


from _aduana import lib as C_ADUANA
from _aduana import ffi

########################################################################
# PageDB Wrappers
########################################################################
class AduanaException(Exception):
    @classmethod
    def from_error(cls, c_error):
        return cls(
            message = C_ADUANA.error_message(c_error),
            code = C_ADUANA.error_code(c_error))

    def __init__(self, message, code=None):
        if isinstance(message, basestring):
            self.message = message
        else:
            self.message = ffi.string(self.message)
        self.code = code

    def __str__(self):
        if self.code:
            return "{0} (code={1})".format(self.message, self.code)
        else:
            return self.message


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
            raise AduanaException(
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
                raise AduanaException(
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
            raise AduanaException(
                "Error inside crawled_page_set_hash64: returned %d" % ret)

    @property
    def time(self):
        return self._crawled_page.time

    @time.setter
    def time(self, value):
        self._crawled_page.time = value

    def get_links(self):
        links = []
        for i in xrange(self._c_aduana.crawled_page_n_links(self._crawled_page)):
            pLi = self._c_aduana.crawled_page_get_link(self._crawled_page, i)
            links.append((pLi[0].url, pLi[0].score))
        return links

    def __del__(self):
        self._c_aduana.crawled_page_delete(self._crawled_page)


class PageInfo(object):
    def __init__(self, page_hash, c_page_info):
        self._c_aduana = C_ADUANA

        self._page_info = c_page_info
        self._hash = page_hash

    @property
    def url(self):
        return ffi.string(self._page_info.url)

    def __getattr__(self, name):
        return getattr(self._page_info, name)

    def __del__(self):
        self._c_aduana.page_info_delete(self._page_info)

    def __hash__(self):
        return self._hash

    @property
    def rate(self):
        return self._c_aduana.page_info_rate(self._page_info)

    @property
    def is_seed(self):
        return self._c_aduana.page_info_is_seed(self._page_info)


def only_if_open(f):
    @functools.wraps(f)
    def dec(*args, **kwargs):
        obj = args[0]
        if not obj.closed:
            return f(*args, **kwargs)
        else:
            raise AduanaException('Object has already been closed')

    return dec

def close_method(f):
    @functools.wraps(f)
    def dec(*args, **kwargs):
        obj = args[0]
        if not obj._closed:
            obj._closed = True
            return f(*args, **kwargs)

    return dec

class PageDB(object):
    @staticmethod
    def urlhash(url):
        return C_ADUANA.page_db_hash(url)

    def __init__(self, path, persist=0):
        # save to make sure lib is available at destruction time
        self._c_aduana = C_ADUANA

        self._closed = False
        self._page_db = ffi.new('PageDB **')
        ret = self._c_aduana.page_db_new(self._page_db, path)
        if ret != 0:
            if self._page_db:
                raise AduanaException.from_error(self._page_db[0].error)
            else:
                raise AduanaException("Error inside page_db_new", ret)

        self.persist = persist

    @property
    def closed(self):
        return self._closed

    @property
    @only_if_open
    def persist(self):
        return self._page_db[0].persist

    @persist.setter
    @only_if_open
    def persist(self, value):
        self._c_aduana.page_db_set_persist(self._page_db[0], value)

    def __del__(self):
        self.close()

    @close_method
    def close(self):
        self._c_aduana.page_db_delete(self._page_db[0])

    @only_if_open
    def iter_page_info(self):
        st = ffi.new('HashInfoStream **')
        ret = self._c_aduana.hashinfo_stream_new(st, self._page_db[0])
        if ret != 0:
            raise AduanaException.from_error(self._page_db[0].error)

        page_hash = ffi.new('uint64_t *')
        pi = ffi.new('PageInfo **')
        while True:
            ss = self._c_aduana.hashinfo_stream_next(st[0], page_hash, pi)
            if ss != self._c_aduana.stream_state_next:
                break
            yield PageInfo(page_hash[0], pi[0])

        self._c_aduana.hashinfo_stream_delete(st[0])

    @only_if_open
    def page_info(self, page_hash):
        pi = ffi.new('PageInfo **')
        ret = self._c_aduana.page_db_get_info(
            self._page_db[0], ffi.cast('uint64_t', page_hash), pi)
        if ret != 0:
            raise AduanaException.from_error(self._page_db[0].error)
        return PageInfo(page_hash, pi[0])

    @only_if_open
    def add(self, crawled_page):
        self._c_aduana.page_db_add(
            self._page_db[0],
            crawled_page._crawled_page,
            ffi.NULL
        )
########################################################################
# Scorers
########################################################################
class PageRankScorer(object):
    def __init__(self, page_db):
        self._c_aduana = C_ADUANA

        self._closed = False
        self._scorer = ffi.new('PageRankScorer **')
        self._c_aduana.page_rank_scorer_new(self._scorer, page_db._page_db[0])

    @property
    def closed(self):
        return self._closed

    def __del__(self):
        self.close()

    @close_method
    def close(self):
        if not self._closed:
            self._closed = True
            self._c_aduana.page_rank_scorer_delete(self._scorer[0])

    @only_if_open
    def setup(self, scorer):
        self._c_aduana.page_rank_scorer_setup(self._scorer[0], scorer)

    @property
    @only_if_open
    def persist(self):
        return self._scorer[0].persist

    @persist.setter
    @only_if_open
    def persist(self, value):
        self._c_aduana.page_rank_scorer_set_persist(self._scorer[0], value)

    @property
    @only_if_open
    def use_content_scores(self):
        return self._scorer[0].use_content_scores

    @use_content_scores.setter
    @only_if_open
    def use_content_scores(self, value):
        self._c_aduana.page_rank_scorer_set_use_content_scores(
            self._scorer[0], 1 if value else 0)

    @property
    @only_if_open
    def damping(self):
        return self._scorer[0].page_rank.damping

    @damping.setter
    @only_if_open
    def damping(self, value):
        self._c_aduana.page_rank_scorer_set_damping(self._scorer[0], value)

class HitsScorer(object):
    def __init__(self, page_db):
        self._c_aduana = C_ADUANA

        self._closed = False
        self._scorer = ffi.new('HitsScorer **')
        self._c_aduana.hits_scorer_new(self._scorer, page_db._page_db[0])

    @property
    def closed(self):
        return self._closed

    def __del__(self):
        self.close()

    @close_method
    def close(self):
        self._c_aduana.hits_scorer_delete(self._scorer[0])

    @only_if_open
    def setup(self, scorer):
        self._c_aduana.hits_scorer_setup(self._scorer[0], scorer)

    @property
    @only_if_open
    def persist(self):
        return self._scorer[0].persist

    @persist.setter
    @only_if_open
    def persist(self, value):
        self._c_aduana.hits_scorer_set_persist(self._scorer[0], value)

    @property
    @only_if_open
    def use_content_scores(self):
        return self._scorer[0].use_content_scores

    @use_content_scores.setter
    @only_if_open
    def use_content_scores(self, value):
        self._c_aduana.hits_scorer_set_use_content_scores(
            self._scorer[0], 1 if value else 0)

########################################################################
# Scheduler Wrappers
########################################################################
class SchedulerCore(object):
    def __init__(self, scheduler, scheduler_add, scheduler_request):
        self._c_aduana = C_ADUANA

        self._sch = scheduler
        self._scheduler_add = scheduler_add
        self._scheduler_request = scheduler_request

    def add(self, crawled_page):
        # better to signal this as an error here than in bf_scheduler_add
        if not isinstance(crawled_page, CrawledPage):
            raise AduanaException("argument to function must be a CrawledPage instance")

        ret = self._scheduler_add(self._sch[0], crawled_page._crawled_page)
        if ret != 0:
            raise AduanaException.from_error(self._sch[0].error)

    def requests(self, n_pages):
        pReq = ffi.new('PageRequest **')
        ret = self._scheduler_request(self._sch[0], n_pages, pReq)
        if ret != 0:
            raise AduanaException.from_error(self._sch[0].error)
        reqs = [ffi.string(pReq[0].urls[i]) for i in xrange(pReq[0].n_urls)]
        self._c_aduana.page_request_delete(pReq[0])
        return reqs

class BFScheduler(object):
    def __init__(self, page_db, persist=0, scorer=None, path=None):
        # save to make sure lib is available at destruction time
        self._c_aduana = C_ADUANA

        self._closed = False
        self._page_db = page_db
        self._page_db.persist = persist

        self._sch = ffi.new('BFScheduler **')
        self._core = SchedulerCore(
            self._sch,
            self._c_aduana.bf_scheduler_add,
            self._c_aduana.bf_scheduler_request
        )

        ret = self._c_aduana.bf_scheduler_new(
            self._sch,
            self._page_db._page_db[0],
            path or ffi.NULL
        )
        if ret != 0:
            if self._sch:
                raise AduanaException.from_error(self._sch[0].error)
            else:
                raise AduanaException("Error inside bf_scheduler_new", ret)

        self._c_aduana.bf_scheduler_set_persist(self._sch[0], persist)

        if scorer:
            self._scorer = scorer
            self._scorer.setup(self._sch[0].scorer)
            ret = self._c_aduana.bf_scheduler_update_start(self._sch[0])
            if ret != 0:
                raise AduanaException.from_error(self._sch[0].error)

    @property
    def closed(self):
        return self._closed

    def __del__(self):
        self.close()

    @close_method
    def close(self):
        ret = self._c_aduana.bf_scheduler_update_stop(self._sch[0])
        if ret != 0:
            raise AduanaException.from_error(self._sch[0].error)
        self._c_aduana.bf_scheduler_delete(self._sch[0])

    @classmethod
    def from_settings(cls, page_db, settings, logger=None):
        scorer_class = settings.get('SCORER', None)
        if scorer_class is None:
            if logger:
                logger.backend.warning(
                    'No SCORER setting. Using default content scorer')
            scorer = None
        else:
            scorer = scorer_class(page_db)
            use_scores = settings.get('USE_SCORES', False)
            if use_scores:
                if scorer_class == PageRankScorer:
                    scorer.damping = settings.get('PAGE_RANK_DAMPING', 0.85)
                scorer.use_content_scores = use_scores

        scheduler = cls(page_db, scorer=scorer, persist=page_db.persist)

        soft_crawl_limit = settings.get('SOFT_CRAWL_LIMIT', 0.25)
        hard_crawl_limit = settings.get('HARD_CRAWL_LIMIT', 100.0)
        scheduler.set_crawl_rate(soft_crawl_limit, hard_crawl_limit)

        max_crawl_depth = settings.get('MAX_CRAWL_DEPTH', None)
        if max_crawl_depth:
            scheduler.set_max_crawl_depth(max_crawl_depth)

        update_interval = settings.get('SCORE_UPDATE_INTERVAL', None)
        if update_interval:
            scheduler.set_update_interval(update_interval)

        return scheduler

    @only_if_open
    def add(self, crawled_page):
        return self._core.add(crawled_page)

    @only_if_open
    def requests(self, n_pages):
        return self._core.requests(n_pages)

    @only_if_open
    def set_crawl_rate(self, soft_rate, hard_rate):
        self._c_aduana.bf_scheduler_set_max_domain_crawl_rate(self._sch[0], soft_rate, hard_rate)

    @only_if_open
    def set_max_crawl_depth(self, max_crawl_depth=0):
        self._c_aduana.bf_scheduler_set_max_crawl_depth(self._sch[0], max_crawl_depth)

    @only_if_open
    def set_update_interval(self, update_interval):
        self._c_aduana.bf_scheduler_set_update_interval(self._sch[0], update_interval)

class FreqScheduler(object):
    def __init__(self, page_db, persist=0, path=None):
        # save to make sure lib is available at destruction time
        self._c_aduana = C_ADUANA

        self._closed = False
        self._page_db = page_db
        self._page_db.persist = persist

        self._sch = ffi.new('FreqScheduler **')
        self._core = SchedulerCore(
            self._sch,
            self._c_aduana.freq_scheduler_add,
            self._c_aduana.freq_scheduler_request
        )

        ret = self._c_aduana.freq_scheduler_new(
            self._sch,
            self._page_db._page_db[0],
            path or ffi.NULL
        )
        if ret != 0:
            if self._sch:
                raise AduanaException.from_error(self._sch[0].error)
            else:
                raise AduanaException("Error inside freq_scheduler_new", ret)

        self._sch[0].persist = persist

    @property
    def closed(self):
        return self._closed

    @classmethod
    def from_settings(cls, page_db, settings, logger=None):
        scheduler = cls(page_db, persist=page_db.persist)

        max_n_crawls = settings.get('MAX_N_CRAWLS', None)
        if max_n_crawls:
            scheduler.max_n_crawls = max_n_crawls

        spec_path = settings.get('FREQ_SPEC', None)
        if spec_path:
            if isinstance(spec_path, basestring):
                with open(spec_path, 'r') as spec:
                    scheduler.load(freq_spec(page_db, spec))
            else:
                scheduler.load(freq_spec(page_db, spec_path))
        else:
            freq_default = settings.get('FREQ_DEFAULT', 0.1)
            freq_scale = settings.get('FREQ_SCALE', -1.0)
            scheduler.load_simple(freq_default, freq_scale)

        freq_margin = settings.get('FREQ_MARGIN', -1.0)
        scheduler.margin = freq_margin

        return scheduler

    @only_if_open
    def load_simple(self, freq_default=1.0, freq_scale=None):
        self._c_aduana.freq_scheduler_load_simple(
            self._sch[0], freq_default, freq_scale or -1.0)

    @only_if_open
    def load(self, freq_iter):
        cur = ffi.new('void **')
        ret = self._c_aduana.freq_scheduler_cursor_open(self._sch[0], cur)
        if ret != 0:
            raise AduanaException.from_error(self._sch[0].error)

        for page_hash, page_freq in freq_iter:
            self._c_aduana.freq_scheduler_cursor_write(
                self._sch[0],
                cur[0],
                ffi.cast('uint64_t', page_hash),
                page_freq
            )
        ret = self._c_aduana.freq_scheduler_cursor_commit(self._sch[0], cur[0])
        if ret != 0:
            raise AduanaException.from_error(self._sch[0].error)

    @only_if_open
    def add(self, crawled_page):
        return self._core.add(crawled_page)

    @only_if_open
    def requests(self, n_pages):
        return self._core.requests(n_pages)

    def __del__(self):
        self.close()

    @close_method
    def close(self):
        self._c_aduana.freq_scheduler_delete(self._sch[0])

    @property
    @only_if_open
    def max_n_crawls(self):
        return self._sch[0].max_n_crawls

    @max_n_crawls.setter
    @only_if_open
    def max_n_crawls(self, value):
        self._sch[0].max_n_crawls = value

    @property
    @only_if_open
    def margin(self):
        return self._sch[0].margin

    @margin.setter
    @only_if_open
    def margin(self, value):
        self._sch[0].margin = value

def freq_spec(page_db, spec):
    rules = []
    for line in spec:
        cols = line.split()
        if len(cols) == 2:
            rules.append(
                (re.compile(cols[0]), cols[1]))

    for page_info in page_db.iter_page_info():
        for regexp, action in rules:
            if regexp.match(page_info.url):
                if action[0] == 'x':
                    try:
                        mult = float(action[1:])
                    except ValueError:
                        warning.warn("Could not parse multiplier: ", action)
                    yield hash(page_info), mult*page_info.rate
                    break # stop after we find a rule
                else:
                    try:
                        interval = float(action)
                    except ValueError:
                        warning.warn("Could not parse interval: ", action)
                    yield hash(page_info), 1.0/interval
                    break

if __name__ == '__main__':
    db = PageDB('./test_python_bindings')
    scorer = PageRankScorer(db)
    bf = BFScheduler(db, scorer=scorer)
    for i in xrange(100000):
        cp = CrawledPage(str(i), [str(i + j) for j in xrange(10)])
        bf.add(cp)
