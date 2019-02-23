#! usr/bin/env python
# coding: utf-8
"""
@author = Chilson
爬取私募排排网基金信息
"""
import scrapy
from scrapy import Request
import json
import pymongo
import datetime

conn = pymongo.MongoClient('localhost', 27017)
db = conn.simuwang


class SimuSpider(scrapy.Spider):
    name = 'simuwang'
    allowed_domains = ['dc.simuwang.com']
    start_urls = ['http://dc.simuwang.com']

    def parse(self, response):

        info_url = 'http://dc.simuwang.com/ranking/get?page={}&condition=fund_type:1,6,4,3,8,2;' \
                   'ret:1;rating_year:1;istiered:0;company_type:1;sort_name:profit_col2;sort_asc:desc;keyword:'

        meta_dic = {'info_url': info_url}

        yield Request(info_url.format('1'), meta=meta_dic, callback=self.parse_page_count)

    def parse_page_count(self, response):
        """通过首页解析总页数,遍历爬取"""
        content = json.loads(response.body.decode('utf-8'))
        page_count = content['pager']['pagecount']
        for page in range(1, page_count + 1):
            url = response.meta['info_url'].format(str(page))
            yield Request(url, callback=self.parse_info)

    def parse_info(self, response):
        """解析私募基金信息"""
        content = json.loads(response.body.decode('utf-8'))
        data = content['data']
        daily_url = 'http://dc.simuwang.com/index.php?m=Data2' \
                    '&c=Chart&a=jzdb_fund&fund_id={}' \
                    '&muid=470422&index_type=0&rz_type=0' \
                    '&nav_flag=2&period=0'

        for d in data:
            d['code'] = d.pop('fund_id')
            d['name'] = d.pop('fund_short_name')
            db.fundInfo.update({'code': d['code'],
                                 'name': d['name']},
                                {'$set': d},
                                upsert=True)

            url = daily_url.format(d['code'])
            meta_dic = {
                'code': d['code'],
                'name': d['name']
            }
            yield Request(url, meta=meta_dic, callback=self.parse_daily_value)

    def parse_daily_value(self, response):
        """基金净值存入mongodb"""
        content = json.loads(response.body.decode('utf-8'))
        dates = content['categories']
        values = content['data'][0]
        datas = zip(dates, values)
        for data in datas:
            date = datetime.datetime.strptime(data[0], '%Y-%m-%d')
            data_dic = {
                'date': date,
                'code': response.meta['code'],
                'name': response.meta['name'],
                'asset': 1 + float(data[1]['value']),
                'accumulatedUnitNetValue': 1 + float(data[1]['value']),
                'updatedAt': datetime.datetime.today(),
                'createdAt': datetime.datetime.today(),
                'source': 'simuwang'
            }
            db.fundDaily.update({'code': response.meta['code'],
                                      'date': date},
                                     {'$set': data_dic},
                                     upsert=True)
