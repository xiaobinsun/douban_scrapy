# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import datetime
import pymysql

from . spiders.douban_spider import MTSubjectSpider
from . items import Celebrity, MovieTV

class MysqlPipeline(object):
    def process_item(self, item, spider):
        if isinstance(item, Celebrity):
            if item['name'] is None:
                raise DropItem

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
                raise DropItem

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

            self.db_conn.commit()
            raise DropItem

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

        tbl_seed = ('create table seed('
                    'sid int not null,'
                    'stype varchar(15) not null,'
                    'primary key(sid))'
        )
        if 'seed' not in tb_names:
            self.db_cur.execute(tbl_seed)
        elif isinstance(spider, MTSubjectSpider):
            # load seeds
            query = 'select sid from seed where stype = "movie_tv"'
            self.db_cur.execute(query)
            for s in self.db_cur.fetchall():
                spider.seeds.add(s[0])

        spider.db_cur = self.db_cur

    def close_spider(self, spider):
        # save seeds
        if isinstance(spider, MTSubjectSpider):
            query = 'truncate table seed'
            self.db_cur.execute(query)

            query = 'insert into seed values'
            cnt = 0
            try:
                while cnt < 60:
                    query += '("{}", "movie_tv"),'.format(spider.seeds.pop())
                    cnt += 1
            except KeyError:
                pass

            if cnt > 0:
                query = query.rstrip(',')
                self.db_cur.execute(query)

        self.db_conn.commit()
        self.db_conn.close()
