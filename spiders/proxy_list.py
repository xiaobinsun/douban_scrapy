import requests
import scrapy

url = 'http://localhost:8111/proxy?count=200&protocol=https'
try:
    res = requests.get(url, timeout=2)
except requests.exceptions.Timeout:
    print('proxy_list daemon not started, run git/proxy_list/run.py')
    exit()

with open('proxy_list.txt', 'w') as f:
    for l in res.json():
        f.write('https://{}:{}'.format(l[0], l[1]))
        f.write('\n')

'''
class ProxyIpSpider(scrapy.Spider):
    name = "ProxyIp"
    start_urls = ['https://github.com/dxxzst/free-proxy-list']

    def parse(self, response):
        http = []
        https = []

        path = '//div[@class="Box-body"]/*/table/tbody/tr'
        for row in response.xpath(path):
            col = row.xpath('td/text()').getall()
            if col[3] == 'high':
                if col[2] == 'http':
                    http.append('http://{}:{}'.format(col[0], col[1]))
                elif col[2] == 'https':
                    https.append('https://{}:{}'.format(col[0], col[1]))

        with open('proxy_list.txt', 'w') as f:
            for item in https:
                f.write(item)
                f.write('\n')
'''
