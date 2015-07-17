from bs4 import BeautifulSoup
import xxhash
from langdetect import detect

from scrapy.http import Request
from scrapy.spiders import Spider
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.selector import Selector
from scrapy.exceptions import DontCloseSpider
from scrapy import signals

from geonames import GeoNames

def triangle(a):
    def f(x):
        if x <= a:
            return 1.0 - (a - x)/a
        else:
            return 1.0 - (x - a)/(1.0 - a)
    return f

scorer = triangle(0.005)

class MySpider(Spider):
    name = 'locations'
    link_extractor = LxmlLinkExtractor()
    gn = GeoNames()

    def parse(self, response):
        soup = BeautifulSoup(response.body)
        for script in soup(["script", "style"]):
            script.extract()
        text = soup.get_text()

        if text:
            response.meta.update(
                content_hash=xxhash.xxh64(text.encode('ascii', 'ignore')).intdigest())

            try:
                langid = detect(text)
            except LangDetectException:
                return

            if langid == 'en':
                locations = MySpider.gn.count(text)
                score = scorer(
                    float(sum(locations.itervalues()))/
                    float(len(text))
                )
                response.meta.update(score=score)
                for link in self.link_extractor.extract_links(response):
                    request = Request(url=link.url)
                    request.meta.update(link_text=link.text)
                    request.meta.update(score=score)
                    yield request
            else:
                response.meta.update(score=0)


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        raise DontCloseSpider
