import frontera
from frontera.core.models import Request
import tempfile
import pagedb

class Backend(frontera.Backend):
    def __init__(self, db=None, scorer_class=pagedb.HitsScorer, use_scores=False):
        if db:
            db_path = db
            persist = 1
        else:
            db_path = tempfile.mkdtemp(prefix='frontera_', dir='./')
            persist = 0

        self._page_db = pagedb.PageDB(db_path)
        self._scorer = scorer_class(self._page_db)
        if use_scores:
            self._scorer.use_content_scores = 1

        self._scheduler = pagedb.BFScheduler(
            self._page_db,
            scorer = self._scorer
        )

    @classmethod
    def from_manager(cls, manager):
        return cls(
            db=manager.settings.get('PAGE_DB_PATH', None),
            use_scores=manager.settings.get('USE_SCORES', False)
        )

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        self._scheduler.add(
            pagedb.CrawledPage(
                '_start_',
                [(link.url, 1.0) for link in seeds]
            )
        )

    def request_error(self, page, error):
        pass

    def page_crawled(self, response, links):
        cp = pagedb.CrawledPage(
            response.url,
            [(link.url, link.meta['score']) for link in links])

        self._scheduler.add(cp)

    def get_next_requests(self, max_n_requests, **kwargs):
        return map(Request, self._scheduler.requests(max_n_requests))
