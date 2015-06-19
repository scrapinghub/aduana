#!/usr/bin/env python
from __future__ import print_function
import sys
import json
import imp
import argparse

import falcon
from talons.auth import basicauth, middleware, external
import gevent.pywsgi

import aduana

class Settings(object):
    """Stores server settings.

    PAGE_DB_PATH      Path to PageDB
    SCORER            Type of scorer to use. Can be any scorer class inside aduana
    USE_SCORES        True to merge content scores with whatever scorer in SCORER
    PAGE_RANK_DAMPING A value between 0 and 1. Only used if SCORER=PageRankScorer
    PERSIST           If False the PageDB and Scheduler databases will be deleted
                      after closing the server
    SOFT_CRAWL_LIMIT  Try to keep all domains below this limit of requests per second
    HARD_CRAWL_LIMIT  Force all domains below this limit
    SEEDS             A file with one URL per line
    DEFAULT_REQS      If not specified by WebBackend return this number of requests
    ADDRESS           Server will list on this address
    PORT              Server will listen using this port
    PASSWDS           A dictionary mapping login name to password
    SSL_KEY           Path to SSL keyfile
    SSL_CERT          Path to SSL certificate
    """

    # Default settings
    PAGE_DB_PATH = 'test-server'
    SCORER = 'HitsScorer'
    USE_SCORES = True
    PAGE_RANK_DAMPING = 0.85
    PERSIST = False
    SOFT_CRAWL_LIMIT = 0.25
    HARD_CRAWL_LIMIT = 100.0
    DEFAULT_REQS = 10
    ADDRESS = '0.0.0.0'
    PORT = 8000
    SEEDS = None
    PASSWDS = None
    SSL_KEY = None
    SSL_CERT = None

    def __init__(self, settings_module=None):
        """settings_module can be a python module which will be used to
        overwrite the default settings"""
        if settings_module:
            try:
                self.config = imp.load_source('config', settings_module)
            except ImportError as e:
                print('WARNING: could not load server configuration ({0}): {1}'.format(
                    e,  settings_module), file=sys.stderr)
                self.config = None
        else:
            self.config = None
        if self.config is None:
            print('WARNING: using config defaults', file=sys.stderr)

    def __call__(self, name):
        """Return setting by its name"""
        return getattr(self.config, name, getattr(self, name, None))


def error_response(resp, message):
    resp.content_type = "text/plain"
    resp.data = message
    resp.status = falcon.HTTP_400


class Crawled(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler

    def on_post(self, req, resp):
        """Serves POST requests for crawled pages.

        Syntax example:

        { "url": "http://scrapinghub.com",
          "score": 0.5,
          "links": [["http://scrapinghub.com/professional-services/", 1.0],
                    ["http://scrapinghub.com/platform/", 0.5],
                    ["http://scrapinghub.com/pricing/", 0.8],
                    ["http://scrapinghub.com/clients/", 0.9]] }

        Only the URL field is mandatory. If there are no links that field can
        be left out of the dictionary. If no score it will be assumed 0.0.
        """
        try:
            data = json.loads(req.stream.read())
        except ValueError:
            error_response(resp, 'ERROR: could not parse JSON')
            return

        try:
            url = data['url'].encode('ascii', 'ignore')
        except KeyError:
            error_response(resp, 'ERROR: could not find "url" field in request')
            return

        try:
            links = [(lurl.encode('ascii', 'ignore'), score)
                     for (lurl, score) in data.get('links', [])]
            cp = aduana.CrawledPage(url, links)
            cp.score = data.get('score', 0.0)
        except TypeError as e:
            error_response(resp, 'ERROR: Incorrect data inside CrawledPage. ' + str(e))
            return

        self.scheduler.add(cp)
        resp.status = falcon.HTTP_201


class Request(object):
    def __init__(self, scheduler, default_reqs = 10):
        self.scheduler = scheduler
        self.default_reqs = default_reqs

    def on_get(self, req, resp):
        """Serve GET requests, which returns a list of pages to be crawled.

        The (maximum) number of elements in the list can be specified as a query
        string parameter 'n'. Syntax example:

             http://localhost:8000?n=42
        """
        try:
            n_reqs = int(req.params.get('n', self.default_reqs))
        except ValueError:
            error_response(resp, 'ERROR: Incorrect number of requests')
            return

        urls = self.scheduler.requests(n_reqs)
        resp.data = json.dumps(urls, ensure_ascii=True)
        resp.content_type = "application/json"
        resp.status = falcon.HTTP_200


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start Aduana server.')
    parser.add_argument('settings',
                        nargs='?',
                        default=None,
                        help='Path to python module containing server settings')
    parser.add_argument('--seeds',
                        nargs='?',
                        default=None,
                        help='Path to seeds file')

    args = parser.parse_args()
    if args.settings is None:
        settings = Settings()
    else:
        settings = Settings(args.settings)

    if args.seeds:
        settings.SEEDS = args.seeds

    page_db = aduana.PageDB(settings('PAGE_DB_PATH'))

    scorer_name = settings('SCORER')
    if scorer_name:
        scorer_class = getattr(aduana, settings('SCORER'))
        scorer = scorer_class(page_db)
        scorer.use_content_scores = settings('USE_SCORES')
        if isinstance(scorer, aduana.PageRankScorer):
            scorer.damping = settings('PAGE_RANK_DAMPING')
    else:
        scorer = None

    scheduler = aduana.BFScheduler(
        page_db, scorer=scorer, persist=settings('PERSIST'))
    scheduler.set_crawl_rate(
        settings('SOFT_CRAWL_LIMIT'), settings('HARD_CRAWL_LIMIT'))

    seeds_path = settings('SEEDS')
    if seeds_path is None:
        sys.exit("ERROR: SEEDS setting is missing and mandatory. Exit")

    with open(seeds_path, 'r') as seeds:
        for i, line in enumerate(seeds):
            scheduler.add(
                aduana.CrawledPage('_seed_{0}'.format(i), [(line.strip(), 1.0)]))

    middlewares = []
    passwds = settings('PASSWDS')
    if passwds:
        def authenticate(identity):
            try:
                return identity.key == passwds[identity.login]
            except KeyError:
                return False

        def authorize(identity, request_action):
            return True

        auth = middleware.create_middleware(
            identify_with=[basicauth.Identifier],
            authenticate_with=external.Authenticator(
                external_authn_callable=authenticate),
            authorize_with=external.Authorizer(
                external_authz_callable=authorize)
        )
        middlewares.append(auth)

    crawled = Crawled(scheduler)
    request = Request(scheduler, settings('DEFAULT_REQS'))
    app = application = falcon.API(before=middlewares)
    app.add_route('/crawled', crawled)
    app.add_route('/request', request)

    key_path = settings('SSL_KEY')
    cert_path = settings('SSL_CERT')

    if (key_path and not cert_path) or (cert_path and not key_path):
        print('WARNING: Both key file and certificate file are necessary. Disabling HTTPS',
              file=sys.stderr)
        key_path = None
        cert_path = None

    server = gevent.pywsgi.WSGIServer(
        (settings('ADDRESS'), settings('PORT')),
        app,
        keyfile=key_path,
        certfile=cert_path
    )
    print("Press Ctrl-C to exit")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Exit")
