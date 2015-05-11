from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize

from scrapy.http import Request
from scrapy.spider import Spider
from scrapy.contrib.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.selector import Selector

def load_keywords(fname):
    with open(fname, 'r') as kw_file:   
        kw = [unicode(line.strip(), "utf-8") for line in kw_file]
    return kw

class MySpider(Spider):
    name = 'example'
    link_extractor = LxmlLinkExtractor()
    keywords = load_keywords('keywords.txt')
    max_score = float(len(keywords))
        
    def parse(self, response):
        soup = BeautifulSoup(response.body)
        for script in soup(["script", "style"]):
            script.extract()
        text = soup.get_text()

        score = 0.0
        for kw in self.keywords:
            if kw in text:
                score += 1.0
        score /= self.max_score
        response.meta.update(score=score)

        for link in self.link_extractor.extract_links(response):
            request = Request(url=link.url)
            request.meta.update(link_text=link.text)
            request.meta.update(score=score)
            yield request
