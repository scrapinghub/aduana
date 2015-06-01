import frontera
from frontera.core.models import Request
import tempfile
import pagedb

class Backend(frontera.Backend):
    def __init__(self,
                 db=None,
                 scorer_class=pagedb.HitsScorer,
                 use_scores=False,
                 soft_crawl_limit=0.25,
                 hard_crawl_limit=100,
                 **kwargs):
        if db:
            db_path = db
            persist = 1
        else:
            db_path = tempfile.mkdtemp(prefix='frontera_', dir='./')
            persist = 0

        self._page_db = pagedb.PageDB(db_path)

        if scorer_class is None:
            self._scorer = None
        else:
            self._scorer = scorer_class(self._page_db)
            if use_scores:
                if scorer_class == pagedb.PageRankScorer:
                    self._scorer.damping = kwargs.get('page_rank_damping', 0.85)
                self._scorer.use_content_scores = use_scores

        self._scheduler = pagedb.BFScheduler(
            self._page_db,
            scorer=self._scorer,
            persist=persist
        )
        self._scheduler.set_crawl_rate(soft_crawl_limit, hard_crawl_limit)

        self._n_seeds = 0

    @classmethod
    def from_manager(cls, manager):
        scorer = manager.settings.get('SCORER', None)
        if scorer:
            try:
                scorer_class = getattr(pagedb, scorer)
            except AttributeError:
                manager.logger.backend.warning(
                    'Cannot load scorer class {0}. Using content scorer'.format(scorer))
                scorer_class = None
        else:
            manager.logger.backend.warning(
                'No SCORER setting. Using default content scorer')
            scorer_class = None

        return cls(
            db=manager.settings.get('PAGE_DB_PATH', None),
            use_scores=manager.settings.get('USE_SCORES', False),
            scorer_class=scorer_class,
            soft_crawl_limit=manager.settings.get('SOFT_CRAWL_LIMIT', 0.25),
            hard_crawl_limit=manager.settings.get('SOFT_CRAWL_LIMIT', 100.0),
            page_rank_damping=manager.settings.get('PAGE_RANK_DAMPING', 0.85)
        )

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        self._scheduler.add(
            pagedb.CrawledPage(
                '_seed_{0}'.format(self._n_seeds),
                [(link.url, 1.0) for link in seeds]
            )
        )
        self._n_seeds += 1

    def request_error(self, page, error):
        pass

    def page_crawled(self, response, links):
        cp = pagedb.CrawledPage(
            response.url,
            [(link.url, link.meta['scrapy_meta']['score']) for link in links])
        try:
            cp.score = response.meta['score']
        except KeyError:
            cp.score = 0.0

        self._scheduler.add(cp)

    def get_next_requests(self, max_n_requests, **kwargs):
        return map(Request, self._scheduler.requests(max_n_requests))
