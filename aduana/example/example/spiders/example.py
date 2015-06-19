from bs4 import BeautifulSoup

from scrapy.http import Request
from scrapy.spider import Spider
from scrapy.contrib.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.selector import Selector

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

        for link in self.link_extractor.extract_links(response):
            request = Request(url=link.url)
            request.meta.update(link_text=link.text)
            link_score = KeywordScorer.score(link.text)
            request.meta.update(score=link_score)
            yield request
