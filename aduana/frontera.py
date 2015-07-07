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
            return cls(
                aduana.BFScheduler.from_settings(
                    page_db, manager.settings, manager.logger))
        elif scheduler_class is aduana.FreqScheduler:
            return cls(
                aduana.FreqScheduler.from_settings(
                    page_db, manager.settings))

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
            cp.score = response.meta['scrapy_meta']['score']
        except KeyError:
            cp.score = 0.0

        try:
            cp.hash = response.meta['scrapy_meta']['content_hash']
        except KeyError:
            pass

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
        try:
            score = response.meta['scrapy_meta']['score']
        except KeyError:
            score = 0.0

        try:
            content_hash = response.meta['scrapy_meta']['content_hash']
        except KeyError:
            content_hash = None

        message = {
            'url': response.url,
            'score': score,
            'links': [(link.url, link.meta['scrapy_meta']['score']) for link in links],
        }
        if content_hash:
            message['content_hash'] = content_hash

        r = self.session.post(
            self.server + '/crawled',
            json=message,
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
