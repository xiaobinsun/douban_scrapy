import os
import sys
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

import conf
from spiders.douban_spider import MTSubjectSpider, ScoreSpider

if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())

    process.crawl(MTSubjectSpider)
    process.crawl(ScoreSpider)

    process.start()
