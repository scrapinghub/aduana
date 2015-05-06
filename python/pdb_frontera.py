import frontera
from frontera.core.models import Request
import tempfile
import pagedb

class Backend(frontera.Backend):
    def __init__(self, db=None, scorer_class=pagedb.PageRankScorer):
        if db:
            db_path = db
            persist = 1
        else:
            db_path = tempfile.mkdtemp(prefix='frontera_', dir='./')
            persist = 0

        self._scheduler = pagedb.BFScheduler(
            db_path, 
            persist=persist, 
            scorer_class=scorer_class
        )

    @classmethod
    def from_manager(cls, manager):
        return cls(manager.settings.get('PAGE_DB_PATH', None))

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        self._scheduler.add(
            pagedb.CrawledPage(
                '_start_', 
                [link.url for link in seeds]
            )
        )

    def request_error(self, page, error):
        pass

    def page_crawled(self, response, links):
        self._scheduler.add(
            pagedb.CrawledPage(response.url,
                               [link.url for link in links])
        )

    def get_next_requests(self, max_n_requests, **kwargs):
        return map(Request, self._scheduler.requests(max_n_requests))
