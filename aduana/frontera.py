from __future__ import absolute_import

import frontera
from frontera.core.models import Request
import tempfile
import aduana
import requests
import requests.adapters

class Backend(frontera.Backend):
    def __init__(self,
                 logger,
                 db=None,
                 scorer_class=aduana.HitsScorer,
                 use_scores=False,
                 soft_crawl_limit=0.25,
                 hard_crawl_limit=100,
                 **kwargs):
        self.logger = logger
        if db:
            db_path = db
            persist = 1
        else:
            db_path = tempfile.mkdtemp(prefix='frontera_', dir='./')
            persist = 0

        self._page_db = aduana.PageDB(db_path)

        if scorer_class is None:
            self._scorer = None
        else:
            self._scorer = scorer_class(self._page_db)
            if use_scores:
                if scorer_class == aduana.PageRankScorer:
                    self._scorer.damping = kwargs.get('page_rank_damping', 0.85)
                self._scorer.use_content_scores = use_scores

        self._scheduler = aduana.BFScheduler(
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
                scorer_class = getattr(aduana, scorer)
            except AttributeError:
                manager.logger.backend.warning(
                    'Cannot load scorer class {0}. Using content scorer'.format(scorer))
                scorer_class = None
        else:
            manager.logger.backend.warning(
                'No SCORER setting. Using default content scorer')
            scorer_class = None

        return cls(
            logger=manager.logger.backend,
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
            aduana.CrawledPage(
                '_seed_{0}'.format(self._n_seeds),
                [(link.url, 1.0) for link in seeds]
            )
        )
        self._n_seeds += 1

    def request_error(self, page, error):
        pass

    def page_crawled(self, response, links):
        cp = aduana.CrawledPage(
            response.url,
            [(link.url, link.meta['scrapy_meta']['score']) for link in links])
        try:
            cp.score = response.meta['score']
        except KeyError:
            cp.score = 0.0

        self._scheduler.add(cp)

    def get_next_requests(self, max_n_requests, **kwargs):
        return map(Request, self._scheduler.requests(max_n_requests))


class IgnoreHostNameAdapter(requests.adapters.HTTPAdapter):
    def cert_verify(self, conn, url, verify, cert):
        conn.assert_hostname = False
        return super(IgnoreHostNameAdapter,
                     self).cert_verify(conn, url, verify, cert)

class WebBackend(frontera.Backend):
    def __init__(self,
                 logger,
                 server_name='localhost',
                 server_port=8000,
                 server_cert=None):
        self.logger = logger
        schema = 'https' if server_cert else 'http'
        self.server = '{0}://{1}:{2}'.format(schema, server_name, server_port)
        self.server_cert = server_cert
        self.session = requests.Session()
        self.session.mount('https://', IgnoreHostNameAdapter())

    @classmethod
    def from_manager(cls, manager):
        return cls(logger=manager.logger.backend,
                   server_name=manager.settings.get('SERVER_NAME', 'localhost'),
                   server_port=manager.settings.get('SERVER_PORT', 8000),
                   server_cert=manager.settings.get('SERVER_CERT', None))

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        pass

    def request_error(self, page, error):
        pass

    def page_crawled(self, response, links):
        r = self.session.post(
            self.server + '/crawled',
            json={
                'url': response.url,
                'score': response.meta.get('score', 0.0),
                'links': [(link.url, link.meta['scrapy_meta']['score']) for link in links]
            },
            verify=self.server_cert is not None
        )
        if r.status_code != 201:
            self.logger.warning(r.text)

    def get_next_requests(self, max_n_requests, **kwargs):
        r = self.session.get(
            self.server + '/request',
            params={'n': max_n_requests},
            verify=self.server_cert
        )
        if r.status_code != 200:
            self.logger.warning(r.text)
            return []
        else:
            return map(Request, r.json())
