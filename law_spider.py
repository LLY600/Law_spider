# -*- coding: utf-8 -*-
# @author：LLY


import re
import json
import requests
import logging
from lxml import etree
from time import sleep
from retrying import retry
from pymongo import MongoClient


logging.basicConfig(
    # 日志级别
    level=logging.INFO,
    # 日志格式
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    # 打印日志时间
    datefmt='%a, %d %b %Y %H:%M:%S',
    # 日志文件
    filename='run.log',
    # 打开日志方式
    filemode='a'
)



class LawSpider:


    # 初始化构造数据
    def __init__(self):
        # URL链接是以关键字搜索抓包获取的链接，可自行更改
        # self.url = 'http://law.npc.gov.cn/FLFG/getAllList.action'
        self.url = 'http://law.npc.gov.cn/FLFG/getAllList.action?SFYX=%E6%9C%89%E6%95%88&zlsxid=&bmflid=&zdjg=&txtid=&resultSearch=false&lastStrWhere=%E7%94%B5%E5%AD%90%E5%95%86%E5%8A%A1%E6%B3%95&keyword=%E7%94%B5%E5%AD%90%E5%95%86%E5%8A%A1%E6%B3%95&pagesize=20'
        # 此处请求头信息的User-Agent需要根据运行程序的浏览器获取，目的是不让爬取的服务器认为当前是一个爬虫在获取数据
        self.headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}
        response = requests.get(self.url, headers=self.headers, timeout=5)
        html_str = response.content.decode()
        content = etree.HTML(html_str)
        page_count = int(content.xpath('//*[@id="pageCount"]/@value')[0])
        logging.info('当前爬取数据共%s页' % page_count)
        print('当前爬取数据共%s页' % page_count)
        self.datas = []
        for i in range(1, page_count + 1):
            self.data = {'pagesize': 20, 'curPage': i}
            self.datas.append(self.data)



    # 获取爬取的url链接
    # 加上retry装饰器，如下函数在运行中出现异常，会再次请求，最多请求3次（次数可自定义）
    @retry(stop_max_attempt_number=3)
    def get_url_list(self):
        href_list = []
        for self.data in self.datas:
            response = requests.post(self.url, headers=self.headers, data=self.data, timeout=5)
            html_str = response.content.decode()
            content = etree.HTML(html_str)
            hrefs = content.xpath('//*/td/a[not(@title)]/@href')
            for href in hrefs:
                if href is not None:
                    flfgID = str(re.findall("\('[0-9]+", str(href))[0]).replace("('", "")
                    zlsxid = str(re.findall("[0-9]+'\)", str(href))[0]).replace("')", "")
                    href = 'http://law.npc.gov.cn:80/FLFG/flfgByID.action' + "?flfgID=" + flfgID + "&zlsxid=" + zlsxid
                    yield href
                    href_list.append(href)
        return href_list


    # 解析爬取的url链接
    @retry(stop_max_attempt_number=3)
    def _parse_url(self, url):
        requests.encoding = 'utf-8'
        response = requests.post(url, headers=self.headers, data=self.data, timeout=5)
        return response.text


    # 此函数在解析URL异常时返回None
    def parse_url(self, url):
        try:
            html_str = self._parse_url(url)
        except:
            html_str = None
        return html_str


    # 提取爬取的数据
    def get_content_list(self, html_str):
        try:
            html = etree.HTML(html_str)
            tbody_list = html.xpath('//*[@id="content"]')
            content_list = []
            for tbody in tbody_list:
                item = {}
                title = tbody.xpath('./div/div[@class="bt"]/text()')
                item["title"] = title[0].strip() if len(title)>0 else None
                dataAttribute = tbody.xpath('.//tr[1]/td[2]/text()')
                item["dataAttribute"] = dataAttribute[0].strip() if len(dataAttribute) > 0 else None
                departmentClassification = tbody.xpath('.//tr[1]/td[4]/text()')
                item["departmentClassification"] = departmentClassification[0].strip() if len(departmentClassification)>0 else None
                establishmentOrgan = tbody.xpath('.//tr[2]/td[2]/text()')
                item["establishmentOrgan"] = establishmentOrgan[0].strip() if len(establishmentOrgan)>0 else None
                enactment = tbody.xpath('.//tr[2]/td[2]/text()')
                item["enactment"] = enactment[0].strip() if len(enactment)>0 else None
                issueDate = tbody.xpath('.//tr[3]/td[2]/text()')
                item["issueDate"] = issueDate[0].strip() if len(issueDate)>0 else None
                executeDate = tbody.xpath('.//tr[4]/td[2]/text()')
                item["executeDate"] = executeDate[0].strip() if len(executeDate)>0 else None
                timeliness = tbody.xpath('.//tr[4]/td[4]/text()')
                item["timeliness"] = timeliness[0].strip() if len(timeliness)>0 else None
                item["articleText"] = tbody.xpath('.//text()')
                articleText = [i.strip() for i in item["articleText"]]
                item["articleText"] = str(articleText).replace("''", '').replace(' ,', '').replace(r'\\u3000', '') if len(articleText) > 0 else None
                content_list.append(item)
            return content_list
        except:
            logging.info('数据异常')
            print('数据异常')


    # 保存爬取的数据
    def save_content_list(self, content_list):
        try:
            # 1.保存本地，可注释
            with open('law.txt', 'a', encoding='utf-8') as f:
                for content in content_list:
                    f.write(json.dumps(content,ensure_ascii=False,indent=2))
                    f.write('\n')
                    logging.info('保存本地成功')
                    print('保存本地成功')
            # 2.保存数据库，可自行选择数据库
            for content in content_list:
                try:
                    conn = MongoClient('127.0.0.1', 27017)
                    db = conn.law
                    collection = db.law_info2
                    collection._one(dict(content), upsert=True)
                    sleep(1.5)
                    logging.info('已存入数据库')
                    print('已存入数据库')
                except:
                    logging.info('插入数据异常')
                    print('插入数据异常')
        except:
            logging.info('保存失败')
            print('保存失败')


    # 实现爬虫的主要逻辑
    def run(self):
        # 1.构造URL数据
        url_list = self.get_url_list()
        # 2.发送请求获取响应
        for url in url_list:
            logging.info('正在爬取URL链接:%s' % url)
            print('正在爬取URL链接:%s' % url)
            html_str = self.parse_url(url)
            # 3.提取数据
            content_list = self.get_content_list(html_str)
            # 4.保存数据
            self.save_content_list(content_list)


if __name__ == '__main__':
    law = LawSpider()
    law.run()
