import logging
import re
import random
import threading
import scrapy
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy_splash import SplashRequest
from scrapy.exceptions import CloseSpider

from douban.items import MovieTV, Celebrity, Score, Tag, Seed
from douban.settings import LOGGING, DOUBAN_SEEDS_NUMBER

logging.config.dictConfig(LOGGING)

logger = logging.getLogger('douban.' + __name__)

class DoubanSet(set):
    def __init__(self, it=(), limit=DOUBAN_SEEDS_NUMBER):
        super().__init__(it)
        self.limit = limit

    def add(self, elm):
        if len(self) > self.limit:
            return
        super().add(elm)

class DoubanSpider(scrapy.Spider):
    name = "Douban"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.re = re.compile(r'^http.*/subject/(\d+)/.*')

    def sid_to_request(self, sid):
        baseurl = 'https://movie.douban.com'
        url = '/'.join([baseurl, 'subject', str(sid)])
        url += '/'

        return scrapy.Request(url=url, callback=self.parse_subject)
        '''
        return SplashRequest(url=url, callback=self.parse_subject,
                            endpoint='render.html',
                            args={'wait': 0.5})
        '''

    def subject_url_to_request(self, url):
        return scrapy.Request(url=url, callback=self.parse_subject)
        '''
        return SplashRequest(url=url, callback=self.parse_subject,
                            endpoint='render.html',
                            args={'wait': 0.5})

        '''

    def cid_to_request(self, cid, cb_kwargs):
        baseurl = 'https://movie.douban.com'
        url = '/'.join([baseurl, 'celebrity', cid])
        url += '/'

        logger.debug('retrieving celebrity: %s', cid)
        return scrapy.Request(url=url, callback=self.parse_celebrity,
                                priority=100, cb_kwargs=cb_kwargs)
        '''
        return SplashRequest(url=url, callback=self.parse_celebrity,
                              priority=100,
                              cb_kwargs=cb_kwargs,
                              endpoint='render.html',
                              args={'wait': 0.5})
        '''

    def url_to_sid(self, url):
        m = self.re.match(url)
        return m.group(1)

    def sid_retrieved(self, sid):
        query = 'select * from movie_tv where id = "{}"'
        if self.db_cur.execute(query.format(sid)):
            return True
        return False


class MTSubjectSpider(DoubanSpider):
    name = "MTSubjectSpider"
    start_urls = ['https://movie.douban.com/']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seeds = DoubanSet()
        self.newseeds = DoubanSet()
        self.cre = re.compile('^/celebrity/(\d+)/$')

    def start_requests(self):
        '''
        for url in self.start_urls:
            yield SplashRequest(url=url, callback=self.parse_startpage,
                            endpoint='render.html',
                            args={'wait': 0.5})
        '''

        for sid in self.seeds:
            if not self.sid_retrieved(sid):
                logger.debug('From seed, Retrieving subject: %s', sid)
                yield self.sid_to_request(sid)

    def parse_startpage(self, response):
        # 正在热映
        for url in response.css('.article .title a::attr(href)').getall():
            sid = self.url_to_sid(url)
            if not self.sid_retrieved(sid):
                self.newseeds.add(sid)
                logger.debug('From startpage, Retrieving subject: %s', sid)
                yield self.sid_to_request(sid)

        # 最近热门电影/电视剧
        for url in response.css('.list-wp .item::attr(href)').getall():
            sid = self.url_to_sid(url)
            if not self.sid_retrieved(sid):
                self.newseeds.add(sid)
                logger.debug('From startpage, Retrieving subject: %s', sid)
                yield self.sid_to_request(sid)

    def parse_subject(self, response):
        if response.status == 403:
            logger.error('403 received, abort')
            raise CloseSpider('403 received')

        sid = self.url_to_sid(response.url)

        try:
            self.newseeds.remove(sid)
        except KeyError:
            pass

        # retrieve subject data
        if not self.sid_retrieved(sid):
            directors = {}
            actors = {}
            scriptwriters = {}

            logger.debug('subject(%s): start parsing...', sid)

            path = ('//div[@id="info"]/span[span]/span[text()="{}"]'
                        '/following-sibling::span/a/@href')
            title = response.css('#content h1 span::text').get()

            region = response.xpath('//div[@id="info"]/span[text()="制片国家/地区:"]'
                                    '/following-sibling::text()[position()=1]').get()
            if region is not None:
                region = region.strip()

            rstr = response.xpath('//div[@id="info"]/'
                        'span[@property="v:initialReleaseDate"]/text()').getall()
            if len(rstr) == 0:
                release_date = None
            else:
                rstr = [s[0:10] for s in rstr]
                rstr.sort()
                release_date = rstr[0]
                if len(release_date) != 10:
                    release_date = None

            tp = response.xpath('//div[@id="info"]/span[text()="集数:"]').get()
            if tp is None:
                tp = 'film'
            else:
                tp = 'tv'

            score = score_num = None
            score_5 = score_4 = score_3 = score_2 = score_1 = None
            score = response.xpath('//strong[@class="ll rating_num"]/text()').get()
            if score is not None:
                score_num = response.xpath('//a[@class="rating_people"]/span/text()').get()
                score_5 = response.xpath('//span[@class="stars5 starstop"]'
                                         '/following-sibling::span/text()').get()
                score_5 = score_5.rstrip('%')

                score_4 = response.xpath('//span[@class="stars4 starstop"]'
                                     '/following-sibling::span/text()').get()
                score_4 = score_4.rstrip('%')

                score_3 = response.xpath('//span[@class="stars3 starstop"]'
                                     '/following-sibling::span/text()').get()
                score_3 = score_3.rstrip('%')

                score_2 = response.xpath('//span[@class="stars2 starstop"]'
                                     '/following-sibling::span/text()').get()
                score_2 = score_2.rstrip('%')

                score_1 = response.xpath('//span[@class="stars1 starstop"]'
                                     '/following-sibling::span/text()').get()
                score_1 = score_1.rstrip('%')

            # tags
            tl = [t for t in response.xpath('//div[@class="tags-body"]/a/text()').getall()]

            item = MovieTV(sid=sid, title=title, di=directors, sw=scriptwriters,
                          act=actors, region=region, rdate=release_date, tp=tp,
                          score_num=score_num, score=score, score_5=score_5,
                          score_4=score_4, score_3=score_3, score_2=score_2,
                          score_1=score_1, tags=tl)

            yield_now = True
            for href in response.xpath(path.format('导演')).getall():
                slist = []
                may_crawl = self._may_crawl_celebrity(href, slist)
                if may_crawl:
                    yield_now = False
                    yield self.cid_to_request(slist[0],
                                              {'item': item, 'cid': slist[0]})

                if len(slist) > 0:
                    directors[slist[0]] = False if may_crawl else True

            for href in response.xpath(path.format('编剧')).getall():
                slist = []
                may_crawl = self._may_crawl_celebrity(href, slist)
                if may_crawl:
                    yield_now = False
                    yield self.cid_to_request(slist[0],
                                              {'item': item, 'cid': slist[0]})

                if len(slist) > 0:
                    scriptwriters[slist[0]] = False if may_crawl else True

            for href in response.xpath(path.format('主演')).getall():
                slist = []
                may_crawl = self._may_crawl_celebrity(href, slist)
                if may_crawl:
                    yield_now = False
                    yield self.cid_to_request(slist[0],
                                              {'item': item, 'cid': slist[0]})

                if len(slist) > 0:
                    actors[slist[0]] = False if may_crawl else True

            # yield here if no need to retrieve celebrity
            if yield_now:
                logger.debug('subject(%s): no need to retrieve celebrity, yield now',
                                sid)
                yield item

            # retrieve links
            for url in response.xpath('//div[@class="recommendations-bd"]'
                                        '/*/dd/a/@href').getall():
                sid = self.url_to_sid(url)
                if not self.sid_retrieved(sid):
                    logger.debug('From recommendations: '
                                 'subject(%s) not retrieved, add to seeds', sid)
                    self.newseeds.add(sid)
                    yield self.subject_url_to_request(url)

        else:
            logger.debug('subject(%s) already retrieved, skip parsing', sid)

    def parse_celebrity(self, response, item, cid):
        if response.status == 403:
            logger.error('403 received, abort')
            raise CloseSpider('403 received')

        logger.debug('celebrity(%s): start parsing...', cid)

        path = (r'//div[@class="info"]/ul/li/span[text()="{}"]'
                    '/following-sibling::text()')

        name = response.xpath('//div[@id="content"]/h1/text()').get()

        gender = response.xpath(path.format('性别')).get()
        if gender is not None:
            gender = gender.split()[1]

        death_date = None
        birth_date = response.xpath(path.format('出生日期')).get()
        if birth_date is not None:
            birth_date = birth_date.split()[1]
        else:
            bd = response.xpath(path.format('生卒日期')).get()
            if bd is not None:
                birth_date = bd.split()[1]
                death_date = bd.split()[3]

        yield Celebrity(cid=cid, name=name, gender=gender,
                        birth_date=birth_date,
                        death_date=death_date)

        if cid in item['di'].keys():
            item['di'][cid] = True
        elif cid in item['sw'].keys():
            item['sw'][cid] = True
        else:
            item['act'][cid] = True
        # check if need to yield MovieTV
        if False not in item['di'].values() and \
            False not in item['sw'].values() and \
            False not in item['act'].values():
            logger.debug('subject(%s): all celebrity retrieved, yield item now',
                            item['sid'])
            yield item

    
    def _may_crawl_celebrity(self, href, out):
        if not href.startswith('/celebrity'):
            return False

        m = self.cre.match(href)
        cid = m.group(1)
        out.append(cid)

        query = 'select * from celebrity where id = "{}"'
        if self.db_cur.execute(query.format(cid)):
            return False

        return True

class TagSpider(DoubanSpider):
    name = 'TagSpider'

    def start_requests(self):
        query = ('select id from movie_tv m '
                 'where not exists '
                 '(select * from tag t where m.id = t.id)')
        self.db_cur.execute(query)
        for sid in self.db_cur.fetchall():
            yield self.sid_to_request(sid[0])

    def parse_subject(self, response):
        if response.status == 403:
            logger.error('403 received, abort')
            raise CloseSpider('403 received')

        sid = self.url_to_sid(response.url)

        tl = [t for t in
                response.xpath('//div[@class="tags-body"]/a/text()').getall()]
        yield Tag(sid=sid, tags=tl)

class SeedSpider(DoubanSpider):
    name = 'SeedSpider'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cnt = 0

    def start_requests(self):
        query = 'select id from movie_tv'
        self.db_cur.execute(query)
        ss = self.db_cur.fetchall()
        while (self.cnt < DOUBAN_SEEDS_NUMBER):
            idx = random.randrange(0, len(ss))
            sid = ss[idx][0]
            yield self.sid_to_request(sid)

    def parse_subject(self, response):
        if response.status == 403:
            logger.error('403 received, abort')
            raise CloseSpider('403 received')

        for url in response.xpath('//div[@class="recommendations-bd"]'
                                        '/*/dd/a/@href').getall():
            sid = self.url_to_sid(url)
            if not self.sid_retrieved(sid):
                logger.debug('From recommendations: '
                            'subject(%s) not retrieved, add to seeds', sid)
                self.cnt += 1
                yield Seed(sid=sid)


class ScoreSpider(DoubanSpider):
    name = "ScoreSpider"

    def start_requests(self):
        query = ('select id from movie_tv '
                 'where release_date is not NULL and '
                 'datediff(curdate(), release_date) < 700')
        self.db_cur.execute(query)
        for sid in self.db_cur.fetchall():
            yield self.sid_to_request(sid[0])

    def parse_subject(self, response):
        if response.status == 403:
            logger.error('403 received, abort')
            raise CloseSpider('403 received')

        sid = self.url_to_sid(response.url)

        score = score_num = None
        score_5 = score_4 = score_3 = score_2 = score_1 = None
        score = response.xpath('//strong[@class="ll rating_num"]/text()').get()
        if score is not None:
            score_num = response.xpath('//a[@class="rating_people"]/span/text()').get()
            score_5 = response.xpath('//span[@class="stars5 starstop"]'
                                     '/following-sibling::span/text()').get()
            score_5 = score_5.rstrip('%')

            score_4 = response.xpath('//span[@class="stars4 starstop"]'
                                         '/following-sibling::span/text()').get()
            score_4 = score_4.rstrip('%')

            score_3 = response.xpath('//span[@class="stars3 starstop"]'
                                         '/following-sibling::span/text()').get()
            score_3 = score_3.rstrip('%')

            score_2 = response.xpath('//span[@class="stars2 starstop"]'
                                         '/following-sibling::span/text()').get()
            score_2 = score_2.rstrip('%')

            score_1 = response.xpath('//span[@class="stars1 starstop"]'
                                         '/following-sibling::span/text()').get()
            score_1 = score_1.rstrip('%')

        yield Score(sid=sid, score_num=score_num, score=score,
                    score_5=score_5, score_4=score_4,
                    score_3=score_3, score_2=score_2,
                    score_1=score_1)
