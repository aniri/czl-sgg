# -*- coding: utf-8 -*-
import scrapy

from scrapy.crawler import CrawlerProcess
import logging
import json
import scraperwiki
from datetime import datetime, timedelta
import hashlib

INDEX_URL = "http://www.sgg.ro/legislativ/index.php/"

class Publication(scrapy.Item):
    institution = scrapy.Field()
    identifier = scrapy.Field()
    type = scrapy.Field()
    date = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    documents = scrapy.Field()
    contact = scrapy.Field()
    feedback_days = scrapy.Field()
    max_feedback_date = scrapy.Field()

PUBLISH_DATE_FORMAT = '%Y-%m-%d'
DOC_EXTENSIONS = [".docs", ".doc", ".txt", ".crt", ".xls", ".xml", ".pdf", ".docx", ".xlsx", ]

def extract_documents(selector_list):
    """
    Extract white-listed documents from CSS selectors.
    Generator function. Search for links to white-listed document types and
    return all matching ones. Each entry has two properties. "type" contains
    the link text, "url" contains the link URL.
    :param selector_list: a SelectorList
    :return: a generator
    """
    for link_selector in selector_list:
        url = link_selector.css('::attr(href)').extract_first()
        if any(url.endswith(ext) for ext in DOC_EXTENSIONS):
            yield {
                'type': link_selector.css('::text').extract_first(),
                'url': url,
            }
def identify(institution, titlu):
    return " : ".join([hashlib.md5(titlu.encode('utf-8')).hexdigest(), institution])

def xtract(obj, sel):
    ret = obj.xpath(sel).extract_first()

    if ret:
        ret = " ".join(map(lambda s : s.strip(), ret.splitlines()))
        return ret
    return ""

class SggSpider(scrapy.Spider):
    name = "sgg"
    allowed_domains = ["www.sgg.ro"]
    start_urls = [INDEX_URL]

    def parse(self, response):
        links = response.css('a::attr(href)').extract()
        links = list(set([response.urljoin(link) for link in links if "domeniu.php" in link]))

        for link in links:
            yield scrapy.Request(response.urljoin(link), callback=self.parse_article)


    def parse_article(self, response):
        institution = response.xpath('//h2/text()').extract()[0].strip()
        logging.warn("scrapping: %s - %s"%(response.url, institution))

        for tr in response.xpath('//table[@class="fancy"]/tr'):

            if tr.xpath('td[1]'):
                publication = Publication()
                titlu =  xtract(tr, 'td[1]//div/text()')
                type_ = xtract(tr, 'td[2]//div//strong/text()')
                consult = xtract(tr, 'td[3]//div/text()')
                avizare = xtract(tr, 'td[4]//div/text()')
                avizori = xtract(tr, 'td[5]//div/text()')
                termen_avize = xtract(tr, 'td[6]//div/text()')
                mfp_mj = xtract(tr, 'td[7]//div/text()')
                reavizare = xtract(tr, 'td[8]//div/text()')

                documents = [
                    {
                        'type': doc['type'],
                        'url': response.urljoin(doc['url']),
                    } for doc in
                    extract_documents(tr.css('a'))
                ]
                json_documents = json.dumps(documents)

                publication['identifier'] = identify(institution, titlu)
                publication['title'] = titlu
                publication['type'] = type_
                publication['institution'] = "sgg"
                publication['date'] = self.parse_date(consult)
                publication['description'] = ""
                publication['feedback_days'] = None
                publication['contact'] = None
                publication['documents'] = json_documents

                scraperwiki.sqlite.save(unique_keys=['identifier'], data=dict(publication))

    def parse_date(self, text):
        try:
            date_obj = datetime.strptime(text, PUBLISH_DATE_FORMAT)
            date = date_obj.date().isoformat()
        except ValueError:
            date = None
        return date

if __name__ == '__main__':
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
        'LOG_LEVEL' : 'WARNING'
    })

    process.crawl(SggSpider)
    process.start()
