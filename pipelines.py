# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import logging
import datetime
import pymysql

from scrapy.exceptions import DropItem

from douban.settings import DOUBAN_SEEDS_NUMBER
from spiders.douban_spider import MTSubjectSpider
from .items import Celebrity, MovieTV, Score, Tag, Seed

logger = logging.getLogger('douban.' + __name__)

class MysqlPipeline(object):
    def process_item(self, item, spider):
        if isinstance(item, Celebrity):
            if item['name'] is None:
                logger.warning('drop celebrity(%s)', item['cid'])
                raise DropItem

            logger.debug('insert celebrity(%s) into db', item['cid'])
            if item['gender'] is None:
                item['gender'] = 'NULL'
            if item['birth_date'] is None:
                item['birth_date'] = 'NULL'
            if item['death_date'] is None:
                item['death_date'] = 'NULL'
            query = 'insert ignore into celebrity values("{}", "{}", "{}", "{}", "{}")'
            self.db_cur.execute(query.format(item['cid'], item['name'],
                                             item['gender'], item['birth_date'],
                                             item['death_date']))
        elif isinstance(item, MovieTV):
            if item['title'] is None:
                logger.warning('drop subject(%s)', item['sid'])
                raise DropItem

            logger.debug('insert subject(%s) into db', item['sid'])

            if item['region'] is None:
                item['region'] = 'NULL'
            if item['rdate'] is None:
                item['rdate'] = 'NULL'
            query = 'insert ignore into movie_tv values("{}", "{}", "{}", "{}", "{}")'
            self.db_cur.execute(query.format(item['sid'], item['title'], item['tp'],
                                             item['region'], item['rdate']))

            query = 'insert ignore into participant values("{}", "{}", "{}")'
            for director in item['di']:
                self.db_cur.execute(query.format(item['sid'], director,
                                                 'director'))
            for writer in item['sw']:
                self.db_cur.execute(query.format(item['sid'], writer,
                                                 'scriptwriter'))
            for actor in item['act']:
                self.db_cur.execute(query.format(item['sid'], actor, 'actor'))

            query = ('insert ignore into score values'
                        '("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}")')
            if item['score_num'] is not None:
                self.db_cur.execute(query.format(
                                        item['sid'],
                                        datetime.date.today(),
                                        item['score'], item['score_num'],
                                        item['score_5'], item['score_4'],
                                        item['score_3'], item['score_2'],
                                        item['score_1']))

            if len(item['tags']) > 0:
                query = 'insert ignore into tag values'
                for t in item['tags']:
                    query += '("{}", "{}"),'.format(item['sid'], t)

                query = query.rstrip(',')
                self.db_cur.execute(query)

        elif isinstance(item, Score):
            if item['score'] is None:
                logger.warning('drop score(%s)', item['sid'])
                raise DropItem

            query = ('replace into score values'
                        '("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}")')
            self.db_cur.execute(query.format(
                                        item['sid'],
                                        datetime.date.today(),
                                        item['score'], item['score_num'],
                                        item['score_5'], item['score_4'],
                                        item['score_3'], item['score_2'],
                                        item['score_1']))
        elif isinstance(item, Tag):
            if len(item['tags']) == 0:
                logger.warning('drop Tag(%s)', item['sid'])
                raise DropItem

            query = 'insert ignore into tag values'
            for t in item['tags']:
                query += '("{}", "{}"),'.format(item['sid'], t)

            query = query.rstrip(',')
            self.db_cur.execute(query)

        elif isinstance(item, Seed):
            query = 'insert ignore into seed values("{}", "movie_tv")'
            self.db_cur.execute(query.format(item['sid']))

        self.db_conn.commit()

    def open_spider(self, spider):
        db = spider.settings.get('MYSQL_DB_NAME', 'douban_scrapy')
        host = spider.settings.get('MYSQL_HOST', 'localhost')
        port = spider.settings.get('MYSQL_PORT', 3306)
        user = spider.settings.get('MYSQL_USER', 'sun')
        password = spider.settings.get('MYSQL_PASSWORD', 'sun1588sun.MYSQL')

        self.db_conn = pymysql.connect(host=host, port=port, db=db, user=user,
                                       passwd=password, charset='utf8')
        self.db_cur = self.db_conn.cursor()

        # check if tables exist
        self.db_cur.execute('show tables')
        tbls = self.db_cur.fetchall()
        tb_names = [t[0] for t in tbls]

        tbl_mt = ('create table movie_tv('
                    'id int unique not null,'
                    'title varchar(50) not null,'
                    'type varchar(10) not null,'
                    'region varchar(30),'
                    'release_date date,'
                    'primary key(id),'
                    'index(title),'
                    'index(release_date))'
        )
        if 'movie_tv' not in tb_names:
            self.db_cur.execute(tbl_mt)

        tbl_score = ('create table score('
                    'id int not null,'
                    'score_date date not null,'
                    'score float not null,'
                    'votes int not null,'
                    'five_star float not null,'
                    'four_star float not null,'
                    'three_star float not null,'
                    'two_star float not null,'
                    'one_star float not null,'
                    'primary key(id, score_date),'
                    'index(score),'
                    'foreign key(id) references movie_tv(id) on delete cascade)'
        )
        if 'score' not in tb_names:
            self.db_cur.execute(tbl_score)

        tbl_celebrity = ('create table celebrity('
                        'id int not null,'
                        'name varchar(50) not null,'
                        'gender char(1),'
                        'birth_date date,'
                        'death_date date,'
                        'primary key(id),'
                        'index(name))'
        )
        if 'celebrity' not in tb_names:
            self.db_cur.execute(tbl_celebrity)

        tbl_participant = ('create table participant('
                            'subject_id int not null,'
                            'celebrity_id int not null,'
                            'role varchar(15) not null,'
                            'primary key(subject_id, celebrity_id, role),'
                            'foreign key(subject_id) references '
                                        'movie_tv(id) on delete cascade,'
                            'foreign key(celebrity_id) references '
                                        'celebrity(id) on delete cascade)'
        )
        if 'participant' not in tb_names:
            self.db_cur.execute(tbl_participant)

        tbl_tag = ('create table tag('
                    'id int not null,'
                    'tag varchar(15) not null,'
                    'primary key(id, tag),'
                    'foreign key(id) references movie_tv(id) on delete cascade,'
                    'index(tag))')
        if 'tag' not in tb_names:
            self.db_cur.execute(tbl_tag)

        tbl_seed = ('create table seed('
                    'sid int not null,'
                    'stype varchar(15) not null,'
                    'primary key(sid))'
        )
        if 'seed' not in tb_names:
            self.db_cur.execute(tbl_seed)

        if isinstance(spider, MTSubjectSpider):
            # load seeds
            logger.debug('Retrieve seeds')
            query = 'select sid from seed where stype = "movie_tv"'
            self.db_cur.execute(query)
            for s in self.db_cur.fetchall():
                spider.seeds.add(str(s[0]))

            for s in spider.seeds:
                logger.debug('%s', s)

        # check events
        if not self.db_cur.execute('show events like \'eliminate_score\''):
            ''' update score table according to the release date(rd)

            1. rd < 30days: record for every day
            2. rd < 60days: record for every 2 days
            3. rd < 120days: record for every 3 days
            4. rd < 250days: record for every 5 days
            5. rd < 500days: record for every 15 days
            6. rd < 700days: record for every 25 days
            7. rd > 700days: record only once
            '''
            query = ('create event eliminate_score '
                'on schedule every 1 day '
                'do '
                'delete from score s '
                'where 1 != '
                '(select rid from '
                    '(select s.*, row_number() over '
                        '(partition by id, floor(dayofyear(score_date)/'
                            '(case when datediff(curdate(), m.release_date) <= 60 then 2 '
                                  'when datediff(curdate(), m.release_date) <= 120 then 3 '
                                  'when datediff(curdate(), m.release_date) <= 250 then 5 '
                                  'when datediff(curdate(), m.release_date) <= 500 then 15 '
                                  'when datediff(curdate(), m.release_date) <= 700 then 25 '
                                  'else 367 '
                            'end)) order by score_date desc) rid '
                    'from score s natural join movie_tv m '
                    'where m.release_date is not NULL and '
                        'datediff(curdate(), m.release_date) > 30) t '
                'where s.id = t.id and s.score_date = t.score_date)'
            )
            self.db_cur.execute(query)

        spider.db_cur = self.db_cur

    def close_spider(self, spider):
        # save seeds
        if isinstance(spider, MTSubjectSpider):
            query = 'truncate table seed'
            self.db_cur.execute(query)

            query = 'insert into seed values'
            cnt = 0
            try:
                while cnt < DOUBAN_SEEDS_NUMBER:
                    query += '("{}", "movie_tv"),'.format(spider.newseeds.pop())
                    cnt += 1
            except KeyError:
                pass

            if cnt > 0:
                query = query.rstrip(',')
                self.db_cur.execute(query)

        self.db_conn.commit()
        self.db_conn.close()
