import collections
import tempfile

import aduana

def test_freq_spec():
    page_db = aduana.PageDB(tempfile.mkdtemp(prefix='test-', dir='./'))

    for i in xrange(1000):
        cp = aduana.CrawledPage('https://a.com')
        cp.time = float(i)
        cp.hash = i
        page_db.add(cp)

    cp = aduana.CrawledPage('http://www.b')
    page_db.add(cp)

    cp = aduana.CrawledPage('http://c.com')
    page_db.add(cp)

    cp = aduana.CrawledPage('http://d.com')
    page_db.add(cp)

    sch = aduana.FreqScheduler.from_settings(
        page_db,
        settings={
            'FREQ_SPEC': [
                'https://.*       x0.001',
                'http://www\..*   200.0',
                '.*               500.0'
            ]
        }
    )


    n_requests = 10000
    req = []
    for i in xrange(n_requests):
        req += sch.requests(1)
    counts = collections.Counter(req)

    urls = ['https://a.com', 'http://www.b', 'http://c.com']
    freqs = {}
    for url in urls:
        freqs[url] = float(counts[url])/float(n_requests)

    expected = dict(zip(urls, [0.001, 1.0/200.0, 1.0/500.0]))
    for i in xrange(len(urls) - 1):
        r1 = freqs[urls[i]]/freqs[urls[i+1]]
        r2 = expected[urls[i]]/expected[urls[i+1]]

        assert r2 - 1e-3 <= r1 <= r2 + 1e-3

    sch.close()
    page_db.close()
