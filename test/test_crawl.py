import gzip
import random
import os
import os.path
import tempfile

import pytest
import networkx as nx

import aduana
import aduana.frontera


class FakeResponse(object):
    def __init__(self, url, score=None):
        self.url = url
        self.meta = {'score': score or random.random()}

class FakeLink(object):
    def __init__(self, url, score=None):
        self.url = url
        self.meta = {
            'scrapy_meta': {
                'score': score or random.random()
            }
        }

class WebGraph(object):
    def __init__(self, nodes, links):
        self.graph = nx.DiGraph()

        n = 0
        with gzip.open(nodes) as fn:
            for line in fn:
                n += 1

        idx2url = n*[None]
        with gzip.open(nodes, 'r') as fn:
            for line in fn:
                url, idx = line.split()
                idx2url[int(idx)] = url
                self.graph.add_node(url)

        with gzip.open(links, 'r') as fl:
            for line in fl:
                a, b = map(int, line.split())
                self.graph.add_edge(idx2url[a], idx2url[b])

    def crawl(self, url):
        return self.graph.successors(url)

@pytest.fixture(scope='module')
def web():
    datadir = os.environ.get('DATAPATH', '.')
    return WebGraph(
        os.path.join(datadir, 'nodes.txt.gz'),
        os.path.join(datadir, 'links.txt.gz'))

def depth_crawl(web, depth):
    random.seed(42)
    backend = aduana.frontera.Backend(aduana.BFScheduler.from_settings(
        aduana.PageDB(tempfile.mkdtemp(prefix='test-', dir='./')),
        settings={
            'MAX_CRAWL_DEPTH': depth
        }
    ))
    backend.add_seeds([FakeLink('https://news.ycombinator.com/', 1.0)])

    crawled = []
    requests = True
    while requests:
        requests = backend.get_next_requests(10)
        for req in requests:
            crawled.append(req.url)
            links = web.crawl(req.url)
            backend.page_crawled(FakeResponse(req.url), map(FakeLink, links))

    dist = nx.single_source_shortest_path_length(
        web.graph,
        'https://news.ycombinator.com/',
        cutoff=depth
    )
    for page in crawled:
        assert dist[page] <= (depth - 1)

def test_depth_crawl_1(web):
    depth_crawl(web, 1)

def test_depth_crawl_2(web):
    depth_crawl(web, 2)

def test_depth_crawl_3(web):
    depth_crawl(web, 3)
