from __future__ import absolute_import

import frontera
from frontera.core.models import Request
import tempfile
import aduana
import requests
import requests.adapters
import requests.auth


class Backend(frontera.Backend):
    def __init__(self, scheduler):
        self._scheduler = scheduler
        self._n_seeds = 0

    @classmethod
    def from_manager(cls, manager):
        db_path = manager.settings.get('PAGE_DB_PATH', None)
        if db_path:
            persist = 1
        else:
            db_path = tempfile.mkdtemp(prefix='frontera_', dir='./')
            persist = 0
        page_db = aduana.PageDB(db_path, persist=persist)

        scheduler_class = manager.settings.get('BACKEND_SCHEDULER', None)
        if scheduler_class is None:
            scheduler_class = aduana.BFScheduler
            manager.logger.backend.warning(
                'No SCHEDULER setting. Using default BFScheduler')

        if scheduler_class is aduana.BFScheduler:
            return cls.from_manager_bf_scheduler(manager, page_db)
        elif scheduler_class is aduana.FreqScheduler:
            return cls.from_manager_freq_scheduler(manager, page_db)

    @classmethod
    def from_manager_bf_scheduler(cls, manager, page_db):
        scorer_class = manager.settings.get('SCORER', False)
        if scorer_class is None:
            manager.logger.backend.warning(
                'No SCORER setting. Using default content scorer')
            scorer = None
        else:
            scorer = scorer_class(page_db)
            use_scores = manager.settings.get('USE_SCORES', False)
            if use_scores:
                if scorer_class == aduana.PageRankScorer:
                    scorer.damping = manager.settings.get('PAGE_RANK_DAMPING', 0.85)
                scorer.use_content_scores = use_scores

        scheduler = aduana.BFScheduler(
            page_db, scorer=scorer, persist=page_db.persist)
        soft_crawl_limit = manager.settings.get('SOFT_CRAWL_LIMIT', 0.25)
        hard_crawl_limit = manager.settings.get('HARD_CRAWL_LIMIT', 100.0)
        scheduler.set_crawl_rate(soft_crawl_limit, hard_crawl_limit)

        max_crawl_depth = manager.settings.get('MAX_CRAWL_DEPTH', None)
        if max_crawl_depth:
            scheduler.set_max_crawl_depth(max_crawl_depth)

        return cls(scheduler)

    @classmethod
    def from_manager_freq_scheduler(cls, manager, page_db):
        scheduler = aduana.FreqScheduler(page_db, persist=page_db.persist)
        scheduler.load_simple()

        max_n_crawls = manager.settings.get('MAX_N_CRAWLS', None)
        if max_n_crawls:
            scheduler.max_n_crawls = max_n_crawls

        return cls(scheduler)

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
                 server_cert=None,
                 user=None,
                 passwd=None):
        self.logger = logger
        schema = 'https' if server_cert else 'http'
        self.server = '{0}://{1}:{2}'.format(schema, server_name, server_port)
        self.server_cert = server_cert
        self.session = requests.Session()
        self.session.mount('https://', IgnoreHostNameAdapter())
        if user and passwd:
            self.session.auth = requests.auth.HTTPBasicAuth(user, passwd)

    @classmethod
    def from_manager(cls, manager):
        return cls(logger=manager.logger.backend,
                   server_name=manager.settings.get('SERVER_NAME', 'localhost'),
                   server_port=manager.settings.get('SERVER_PORT', 8000),
                   server_cert=manager.settings.get('SERVER_CERT', None),
                   user=manager.settings.get('USER', None),
                   passwd=manager.settings.get('PASSWD', None))

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
