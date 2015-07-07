from bs4 import BeautifulSoup
import xxhash

from scrapy.http import Request
from scrapy.spider import Spider
from scrapy.contrib.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.selector import Selector
from scrapy.exceptions import DontCloseSpider
from scrapy import signals

from scorer import KeywordScorer

class MySpider(Spider):
    name = 'example'
    link_extractor = LxmlLinkExtractor()

    def parse(self, response):
        soup = BeautifulSoup(response.body)
        for script in soup(["script", "style"]):
            script.extract()
        text = soup.get_text()

        response.meta.update(score=KeywordScorer.score(text))
        response.meta.update(
            content_hash=xxhash.xxh64(text.encode('ascii', 'ignore')).intdigest())

        for link in self.link_extractor.extract_links(response):
            request = Request(url=link.url)
            request.meta.update(link_text=link.text)
            link_score = KeywordScorer.score(link.text)
            request.meta.update(score=link_score)
            yield request

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        raise DontCloseSpider
