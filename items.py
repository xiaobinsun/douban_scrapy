# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class Celebrity(scrapy.Item):
    cid = scrapy.Field()
    name = scrapy.Field()
    gender = scrapy.Field()
    birth_date = scrapy.Field()
    death_date = scrapy.Field()

class MovieTV(scrapy.Item):
    sid = scrapy.Field()
    title = scrapy.Field()
    di = scrapy.Field()
    sw = scrapy.Field()
    act = scrapy.Field()
    region = scrapy.Field()
    rdate = scrapy.Field()
    tp =scrapy.Field()
    score_num = scrapy.Field()
    score = scrapy.Field()
    score_5 = scrapy.Field()
    score_4 = scrapy.Field()
    score_3 = scrapy.Field()
    score_2 = scrapy.Field()
    score_1 = scrapy.Field()

class Score(scrapy.Item):
    sid = scrapy.Field()
    score_num = scrapy.Field()
    score = scrapy.Field()
    score_5 = scrapy.Field()
    score_4 = scrapy.Field()
    score_3 = scrapy.Field()
    score_2 = scrapy.Field()
    score_1 = scrapy.Field()
