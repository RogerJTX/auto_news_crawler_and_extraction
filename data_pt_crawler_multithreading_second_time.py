# encoding=utf-8
"""
第二次跑维护表 有新闻列表页储存
通用企业官网自动新闻动态资讯采集程序 2020-06-09 2级
author：jtx
"""

import sys,os
sys.path.append('...')

import re
from lxml import etree
from bs4 import BeautifulSoup
from urllib.request import urlopen
import logging
import pymongo
import base64
import urllib
import time, requests
import datetime, random
from etl.utils.log_conf import configure_logging
import traceback
from etl.data_gather.settings import SAVE_MONGO_CONFIG, RESOURCE_DIR
from etl.common_spider.donwloader import Downloader
from urllib.parse import urljoin
from urllib import parse
from urllib.parse import urlunparse
from posixpath import normpath
import chardet
from boilerpipe.extract import Extractor
from goose3 import Goose
from goose3.text import StopWordsChinese
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
from scipy.linalg import norm
import threading
 

class MultipleThreading(threading.Thread):

    def __init__(self, func, args=(), kwargs=None):
        threading.Thread.__init__(self)
        self.func = func
        self.args = args
        if kwargs is None:
            kwargs = {}
        self.kwargs = kwargs

    def run(self):  ##重写run()方法
        print('func_name is: {}'.format(self.func.__name__))
        return self.func(*self.args, **self.kwargs)




class ListDetailSpider(object):
    def __init__(self, config, proj=None):
        # config..........
    
        self.start_down_time = datetime.datetime.now()
        self.down_retry = 3
        configure_logging("QYGW_news_all01.log")  # 日志文件名
        self.logger = logging.getLogger("spider")
        self.downloader = Downloader(self.logger, need_proxy=False)  # 注意是否需要使用代理更改参数
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0",
            # 'Referer': '',
            # 'Host': self.host,
        }
        self.save = 0

    def get_mongo(self, host, port, db, username, password):
        if username and password:
            url = "mongodb://%s:%s@%s:%s/%s" % (username, password, host, port, db)
        else:
            url = "mongodb://%s:%s" % (host, port)
        return pymongo.MongoClient(url)


    def save_record(self, records, coll_name, pk):
        self.save += 1
        # print('------------------save:', self.save)
        tmp = []

        for k, v in pk.items():
            tmp.append("%s=%s" % (k, v))
            # # print( tmp)
        show = "  ".join(tmp)
        # r_in_db = coll_name.find_one(pk)
        # # print ('show：' + show)
        # if not r_in_db:
        coll_name.insert_one(records)
        self.logger.info("成功插入(%s)  %s save(%s)" % (records['url'], show, self.save))

    # TODO 切分mongo数据库 进行多线程采集
    def mongo_skip(self, mongo_name, split_number, specified_number):
        number_all = mongo_name.find().count()
        self.logger.info("number_all:%s" % number_all)
        each = number_all / split_number
        remainder = number_all % split_number
        if remainder == 0:
            block_begin = (specified_number-1)*each
            block_end = specified_number*each
        else:
            if specified_number < split_number:
                block_begin = (specified_number - 1) * each
                block_end = specified_number * each
            else:
                block_begin = (specified_number - 1) * each
                block_end = specified_number * each + remainder
                each = each+remainder

        block_begin = int(block_begin)
        block_end = int(block_end)
        each = int(each)
        self.logger.info("block_begin:%s, block_end:%s" % (str(block_begin), str(block_end)))
        return block_begin, each





    def run(self, split_number, specified_number, start_time):
        """
        数据采集主入口
        :return:
        """
        self.logger.info("Begin Run")
        c = 0

        list_data = []
        # 数据库中获取主页url
        # with open(txt_name, 'r', encoding='utf-8') as f:
        block_begin, each_block_data = self.mongo_skip(self.mongo_read_col1, split_number, specified_number)
        # 跳过区间条目数据
        for each in self.mongo_read_col1.find().limit(block_begin).skip(each_block_data):
            dict_data = {}
            dict_data['news_list_page_url'] = each['news_list_page_url']
            dict_data['website'] = each['website']
            dict_data['company_name'] = each['company_name']
            list_data.append(dict_data)

        for each in list_data:
            judge_news_list_page_url = each['news_list_page_url']
            url = each['website']
            if judge_news_list_page_url and url:

                c += 1
                self.logger.info('第%s个企业' % (c))

                company_name = each['company_name']
                arr = parse.urlparse(url)
                Host = arr[1]
                # # print('Host:', Host)
                self.headers['Host'] = Host
                # # print(self.headers)
                if c < 0:
                    continue

                # if company_name != '启明星辰信息技术集团股份有限公司':
                #     continue

                for news_list_page_url in judge_news_list_page_url:
                    if Host in news_list_page_url:
                        pass
                    else:
                        self.logger.info('news_list_url中没有Host')
                        continue
                    resp = self.downloader.crawl_data(news_list_page_url, None, self.headers, "get")
                    if resp:
                        content_first = resp.content
                        # 判断编码
                        try:
                            bianma = chardet.detect(content_first)
                            # # print("编码-----------: {} \t detail_url: {} \t ".format(bianma, charset_judge_url))
                            # # print(bianma['encoding'])
                            result_bianma = bianma['encoding']
                            self.bianma = result_bianma
                        except Exception as e:
                            self.logger.info(e)
                            self.bianma = 'utf-8'
                        # self.logger.info('编码：%s' % self.bianma)

                        info, news_page_url = self.news_list_page_extraction(news_list_page_url)
                        if not info:
                            # # print('No info')
                            continue

                        # 这里开始找每一个时间的最近的a标签的中文url
                        for i in info:
                            i_element_text_list = i.xpath('text()')
                            if i_element_text_list:
                                i_clean = (i_element_text_list[0]).replace('\n', '').replace('\r', '').strip()
                                if len(i_clean) > 2:
                                    # # print(i_clean)

                                    list_a, list_b = self.data_match(i_clean)
                                    # # print('list_a:', list_a)
                                    # # print('list_b:', list_b)
                                    if list_b:
                                        # 日期
                                        # # print(list_b)
                                        time_b = list_b[0]
                                        publish_date = time_b
                                        if '2020' in publish_date:
                                            # 日期时间判断，获取某段时间的数据
                                            try:
                                                judge_date_linshi = datetime.datetime.strptime(start_time, "%Y-%m-%d")
                                                judge_time = datetime.datetime.strptime(publish_date, "%Y-%m-%d")
                                            except Exception as judge_date_ERROR:
                                                self.logger.info("judge_date_ERROR(%s)  %s " % (judge_date_ERROR, '跳过'))
                                                continue


                                            # # print("2017-11-02大于2017-01-04：", judge_time > end_date)
                                            if (judge_time >= judge_date_linshi):



                                                # 找到日期对应的标签和标题title

                                                try:
                                                    detail_url_linshi, detail_title_linshi = self.find_detial_page_href(
                                                        i)
                                                    detail_url_linshi = detail_url_linshi[0]
                                                except Exception as e4:
                                                    self.logger.info('detail_url_linshi ERROR:', e4)
                                                    continue

                                                if detail_title_linshi:
                                                    try:
                                                        detail_title_linshi = detail_title_linshi[0]
                                                    except Exception as e4:
                                                        self.logger.info('detail_title_linshi ERROR:', e4)
                                                        detail_title_linshi = ''
                                                else:
                                                    detail_title_linshi = ''
                                                # # print('detail_url_linshi:', detail_url_linshi)
                                                # try:
                                                #     # print('detail_title_linshi:', detail_title_linshi.encode('GBK','ignore').decode('GBk'))
                                                # except Exception as detail_title_linshi_ERROR:
                                                #     self.logger.info(detail_title_linshi_ERROR)


                                                # 拼接新闻详情页的链接
                                                detail_url = self.url_pinjie(news_list_page_url, detail_url_linshi)
                                                if Host in detail_url:
                                                    if detail_url.endswith("/"):
                                                        detail_url = detail_url[:-1]
                                                    else:
                                                        detail_url = detail_url
                                                    record = {}
                                                    # # print(detail_url)

                                                    # 去重与储存

                                                    record["website"] = url
                                                    record["company_name"] = company_name
                                                    record["news_list_page_url"] = news_list_page_url
                                                    record["url"] = detail_url
                                                    record["publish_date"] = publish_date
                                                    record["title"] = detail_title_linshi
                                                    record["crawl_time"] = self.start_down_time
                                                    r_in_db = self.mongo_read_col2.find_one({"url": detail_url})  # 唯一标识字段来去重
                                                    if not r_in_db:
                                                        # self.logger.info("页数(%s)" % (page_current))
                                                        # info.pop('institution_linshi')
                                                        try:
                                                            # 详情页的自动正文，图片，作者，标题抓取
                                                            records = self.parse_detail_extraction(record, detail_url)
                                                            # # print(records)
                                                            if not records:
                                                                continue
                                                            #判断是否是新闻详情页 1：详情页标题和新闻列表页标题是否基本一样 2:日期是否一致 3：正文长度
                                                            detail_page_judge, records = self.judge_whether_news_detail_page(detail_url, detail_title_linshi, records)
                                                        except Exception as e:
                                                            self.logger.info(e)
                                                            # # print('跳过详情页%s' % detail_url)
                                                            continue
                                                        if detail_page_judge == True:
                                                            if records:
                                                                # # print(records)
                                                                records.pop("time_detail_page_list")
                                                                self.save_record(records, self.mongo_read_col2, {"url": records["url"]})
                                                        else:
                                                            self.logger.info('详情页不匹配%s' % (records["url"]))

                                                    else:
                                                        self.logger.info("重复数据(%s)" % (record['url']))
                                                else:
                                                    # # print('detail_url中没有host')
                                                    self.logger.info('detail_url中没有host')
                                            else:
                                                self.logger.info("过期数据(%s), pulish_date(%s)" % (news_list_page_url, publish_date ))
                                        else:
                                            # # print('---------------------------------------------------非2020年新闻数据,',news_list_page_url,publish_date)
                                            self.logger.info('---------------------------------------------------非2020年新闻数据,%s,%s' % (news_list_page_url,publish_date))
                                    else:
                                        # # print('没有list_b,没有时间')
                                        pass

                    else:
                        self.logger.info('未响应, 下一条新闻列表页链接')
                        continue

                self.logger.info('下一个企业')

        self.logger.info("Finish Run")

    # TODO 主页的抽取
    def host_page_extraction(self, url):
        resp = self.downloader.crawl_data(url, None, self.headers, "get")
        # # print('resp:', resp)
        content_first = resp.content

        if resp:
            try:
                bianma = chardet.detect(content_first)
                # # print("编码-----------: {} \t detail_url: {} \t ".format(bianma, charset_judge_url))
                # # print(bianma['encoding'])
                result_bianma = bianma['encoding']
                self.bianma = result_bianma
            except Exception as e:
                self.logger.info(e)
                self.bianma = 'utf-8'
            self.logger.info('编码：%s' % self.bianma)
            resp.encoding = self.bianma
            content = resp.text
            soup = BeautifulSoup(content, 'lxml')
            url_list = []

            links = soup.find_all('a')
            for i in links:
                link = i.get('href')
                # if link and link.startswith('http') and any(c.isdigit() for c in link if c) and link not in url_list:
                # any(c.isdigit() for c in link if c) 是筛选了标题中含有数字的网站

                # if link and link.startswith('http') and (link not in url_list):
                #     url_list.append(link)
                #     if url in link:
                #         # print(link)

                # if link and ('news' in link) and (link not in url_list):
                #     url_list.append(link)
                #     # print(link)

                if link and ((('新闻' in str(i)) and (link not in url_list)) or (('动态' in str(i)) and (link not in url_list)) or (('news' in str(i)) and (link not in url_list)) or (('News' in str(i)) and (link not in url_list))):
                    url_list.append(link)
                    # # print(link)

            # myList = ['青海省', '内蒙古自治区', '西藏自治区', '新疆维吾尔自治区', '广西壮族自治区']
            url_list = sorted(url_list, key=lambda i: len(i), reverse=False)
            # # print(url_list)

            return url_list
        else:
            self.logger.info('主页未响应:%s' % (url))
            return None

    # TODO 列表页的抽取
    def news_list_page_extraction(self, url):
        resp = self.downloader.crawl_data(url, None, self.headers, "get")
        # # print(resp)
        if resp:
            resp.encoding = self.bianma
            htmlstr = resp.text
            # # print(htmlstr)
            # # print('url2', url)

            # # 网页转跳重定向部分
            # redirect_list = re.findall(r"window.location.href='(.+?)'",htmlstr)
            # if redirect_list:
            #     redirect_url = redirect_list[0].replace('./', '')
            #     url = self.url_pinjie(url, redirect_url)
            #     # print(url)
            #
            #     resp = self.downloader.crawl_data(url, None, self.headers, "get")
            #     # print(resp)
            #     bianma = self.judge_charset(url)
            #     resp.encoding = bianma
            #     htmlstr = resp.text


            soup = BeautifulSoup(htmlstr, 'lxml')
            # 去除属性script
            # [s.extract() for s in soup("script")]
            # # print(soup)
            # # print(soup)
            # # print(resp.text)
            # code = etree.HTML(htmlstr)
            # info = code.xpath('//*/text()')
            # # print(info)
            try:
                html=etree.HTML(str(soup),etree.HTMLParser())
                info=html.xpath('//*')  #//代表获取子孙节点，*代表获取所有
                # # print('info:', info)
                return info, url
            except Exception as e:
                self.logger.info('html=etree.HTML出错: %s' % e)
                return None, url
        else:
            self.logger.info('列表页未响应:%s' % (url))
            return None, url

    # TODO 详情页的抽取
    def parse_detail_extraction(self, record, detail_url):
        img_url_list = []
        img_url = ''
        extracted_text = ''
        extractor_html = ''
        description_list = []
        content2 = ''
        title_judge2 = ''
        title_judge1 = ''
        # 正文
        try:
            extractor = Extractor(extractor='ArticleExtractor',headers=self.headers, url=detail_url)
            # extractor = Extractor(extractor='KeepEverythingExtractor', url=detail_url)
            extracted_text = extractor.getText()
            # extractor_html = extractor.getHTML()
            title_judge1 = extractor.getTitle()
            # # print('title_judge1', title_judge1)
        except:
            # # print('extractor_error')
            self.logger.info('extractot error')
        content = extracted_text



        # 图片
        resp = self.downloader.crawl_data(detail_url, None, self.headers, "get")
        # # print(resp)
        if resp:
            resp.encoding = self.bianma
            htmlstr = resp.text

            time_detail_page_linshi_list1, time_detail_page_linshi_list2 = self.data_match(htmlstr)

            soup = BeautifulSoup(htmlstr, 'lxml')
            div_judge_list = soup.find_all('div')
            density_chinese_pre = 0
            density_chinese_largest = ''
            density_chinese_largest_div =''

            for i in div_judge_list:

                len_except_chinese = len(self.remove_punctuation(str(i)))
                len_chinese = len(i.get_text().strip().replace('\n', '').replace('\r', ''))

                density_chinese = len_chinese/(len_chinese+len_except_chinese)

                if density_chinese > density_chinese_pre:
                    density_chinese_largest = density_chinese
                    density_chinese_largest_div = i
                    density_chinese_pre = density_chinese_largest

            # # print('density_chinese_largest:', density_chinese_largest)
            # # print(density_chinese_largest_div)

            img_url_all_list = density_chinese_largest_div.find_all('img')
            if img_url_all_list:
                for ii in img_url_all_list:
                    try:
                        ii_tag1 = ii['src']
                        if 'http' not in ii_tag1:
                            img_url = self.url_pinjie(detail_url, ii_tag1)
                            if img_url.endswith("/"):
                                img_url = img_url[:-1]
                            else:
                                img_url = img_url
                        else:
                            img_url = ii_tag1
                    except Exception as e:
                        self.logger.info('img_url ERROR:', e)
                        continue
                    img_url_list.append(img_url)

            try:
                g = Goose({'stopwords_class': StopWordsChinese, 'browser_user_agent': 'Version/5.1.2 Safari/534.52.7'})
                article = g.extract(url=detail_url)

                content2 = article.cleaned_text            # 描述和站点
                opengraph = article.opengraph
                # # print('opengraph:', opengraph)
                if opengraph:
                    try:
                        description_list.append(opengraph['description'])
                        # site_name = opengraph['site_name']
                    except Exception as e2:
                        self.logger.info(e2)
                else:
                    pass
                # 标题



                title_judge2 = article.title
                # # print('title_judge2:', title_judge2)
            except Exception as rr:
                self.logger.info(rr)

            # if description_list:
            #     pass
            # else:
                # description_list = self.content_description_extraction(content, record)
            if content == '':
                content = content2
            if title_judge2 == '':
                title_judge2 = title_judge1

            record["description"] = description_list
            # record["site_name"] = site_name
            record["img_url"] = img_url_list
            record["content"] = content
            record["title_auto"] = title_judge2
            record["time_detail_page_list"] = time_detail_page_linshi_list2
            record["crawl_time"] = self.start_down_time
            record["html"] = htmlstr
            # # print('content:', content)
            return record
        else:
            self.logger.info('详情页未响应:%s' % (detail_url))
            return None


    # 定义删除除字母,数字，汉字以外的所有符号的函数
    def remove_punctuation(self, line):
        line = str(line)
        if line.strip() == '':
            return ''
        r1 = u'[a-zA-Z0-9’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~]+'
        r2 = u"[^a-zA-Z0-9\u4E00-\u9FA5]"
        rule = re.compile(u"[^<>]")
        line = rule.sub('', line)
        return line

    # TODO 拼接新闻详情页的链接
    def url_pinjie(self, url, the_shortest_url):
        url = url.strip().replace(' ', '')
        if url.endswith("/"):
            url = url
        else:
            url = url + '/'

        the_shortest_url = the_shortest_url.replace('./', '')
        url_join = urljoin(url, the_shortest_url)
        # # print('url_join:', url_join)
        arr = parse.urlparse(url_join)
        # # print('arr:', arr)
        # Host = arr[1]
        # # print(Host)
        path = normpath(arr[2])
        # # print('path:', path)
        result = urlunparse((arr.scheme, arr.netloc, path, arr.params, arr.query, arr.fragment))

        result = result.strip()
        if result.endswith(".html"):
            result = result
        else:
            if result.endswith("/"):
                result = result[:-1]
            else:
                result = result
        result = result.strip().replace(' ', '')
        return result

    def data_match(self, test_str):
        # 格式匹配. 如2016-12-24与2016/12/24的日期格式.
        publish_date = ''
        date_reg_exp_list = []
        date_reg_exp = re.compile('\d{4}[-/._年 ]\d{2}[-/._月 ]\d{2}')
        date_reg_exp2 = re.compile('\d{4}[-/._年 ]\d{2}')
        date_reg_exp3 = re.compile('\d{4}[-/._年 ]\d{1}[-/._月 ]\d{2}')
        date_reg_exp4 = re.compile('\d{4}[-/._年 ]\d{2}[-/._月 ]\d{1}')
        date_reg_exp_list.append(date_reg_exp)
        date_reg_exp_list.append(date_reg_exp2)
        date_reg_exp_list.append(date_reg_exp3)
        date_reg_exp_list.append(date_reg_exp4)
        # test_str= """
        #      平安夜圣诞节2016-12-24的日子与去年2015/12/24的是有不同哦.
        #      """
        matches_list = []
        matches_list_clean = []
        # 根据正则查找所有日期并返回
        for num, date_reg_exp_list_each in enumerate(date_reg_exp_list):
            # # print('num:', num)
                matches_list = date_reg_exp_list_each.findall(test_str)
                if matches_list:
                    # 列出并打印匹配的日期
                    for match in matches_list:
                        if (str(match).startswith('20')):
                            # if ('2020' in str(match)):
                            # # print(match)
                            publish_date = ''
                            publish_date_linshi = ''
                            publish_date_linshi_list = []
                            if num == 0:
                                publish_date = match.replace('年', '-').replace('月', '-').replace(' ', '-').replace('_', '-').replace('/',
                                                                                                                          '-').replace(
                                        '.', '-').strip()
                                # # print('match:',publish_date)
                                matches_list_clean.append(publish_date)
                                # # print(matches_list_clean)
                            elif num == 1:
                                publish_date = match.replace('年', '-').replace('月', '-').replace(' ', '-').replace('_',
                                                                                                                   '-').replace(
                                    '/',
                                    '-').replace(
                                    '.', '-').strip() + '-01'
                                # # print('match:', publish_date)
                                matches_list_clean.append(publish_date)
                                # # print(matches_list_clean)
                            elif num == 2:
                                publish_date_linshi = match.replace('年', '-').replace('月', '-').replace(' ', '-').replace('_',
                                                                                                                   '-').replace(
                                    '/',
                                    '-').replace(
                                    '.', '-').strip()
                                publish_date_linshi_list = publish_date_linshi.split('-')
                                for iii_num, iii in enumerate(publish_date_linshi_list):
                                    if iii_num == 1:
                                        publish_date += '-0' + iii
                                    elif iii_num == 2:
                                        publish_date += '-' + iii
                                    elif iii_num == 0:
                                        publish_date += iii
                                # # print('match:', publish_date)
                                matches_list_clean.append(publish_date)
                                # # print(matches_list_clean)
                            elif num == 3:
                                publish_date_linshi = match.replace('年', '-').replace('月', '-').replace(' ', '-').replace(
                                    '_',
                                    '-').replace(
                                    '/',
                                    '-').replace(
                                    '.', '-').strip()
                                publish_date_linshi_list = publish_date_linshi.split('-')
                                for iii_num, iii in enumerate(publish_date_linshi_list):
                                    if iii_num == 2:
                                        publish_date += '-0' + iii
                                    elif iii_num == 1:
                                        publish_date += '-' + iii
                                    elif iii_num == 0:
                                        publish_date += iii
                                # # print('match:', publish_date)
                                matches_list_clean.append(publish_date)
                                # # print(matches_list_clean)
        if matches_list:
            return matches_list, matches_list_clean
            # 2016-12-24
            # 2015/12/24
        else:
            return None, None

    def find_detial_page_href(self, i):
        detail_url_linshi = ''
        detail_title_linshi = ''

        # # print('i_tag:', i.tag)
        ancestor_list = i.xpath('./ancestor-or-self::*')
        ancestor_list_clean = []
        for ii in ancestor_list:
            tag_name = ii.tag
            ancestor_list_clean.append(tag_name + '/')


        i_children_list = i.xpath('./*')  # 获取i的子标签 list类型
        # # print('子标签:', i.xpath('./*'))
        if i_children_list:
            for num, i_children_list_each in enumerate(i_children_list):  # 获取i的子标签的兄弟标签
                # print('第' + str(num + 1) + '次url标签搜索, 遍历i的子级兄弟标签')
                detail_url_linshi = i_children_list_each.xpath('.//a/@href')
                detail_title_linshi = i_children_list_each.xpath('.//a/text()')
                if detail_url_linshi:
                    # print('找到detail_url_linshi')
                    # print('i_children_list_each.tag', i_children_list_each.tag)
                    # print(i_children_list_each.xpath('.//a/text()'))
                    # print(i_children_list_each.xpath('.//a/@href'))
                    # print(ancestor_list_clean, '\n')
                    return detail_url_linshi, detail_title_linshi
                else:
                    # print('第' + str(num + 1) + '次url标签搜索, 遍历i的子级兄弟标签'+'没有找到链接', i_children_list_each.tag)
                    pass
        i_brother_list_pre = i.xpath('./preceding-sibling::*')  # 获取i的兄弟标签 list类型
        # print('兄弟标签前:', i.xpath('./preceding-sibling::*'))
        if i_brother_list_pre:
            for num, i_brother_list_each in enumerate(i_brother_list_pre):  # 获取i的子标签的兄弟标签
                # print('第' + str(num + 1) + '次url标签搜索, 遍历i的前兄弟标签')
                detail_url_linshi = i_brother_list_each.xpath('.//a/@href')
                detail_url_linshi2 = i_brother_list_each.xpath('.//@href')
                detail_title_linshi = i_brother_list_each.xpath('.//a/text()')
                detail_title_linshi2 = i_brother_list_each.xpath('.//text()')
                if detail_url_linshi:
                    # print('找到detail_url_linshi')
                    # print('i_brother_list_each.tag', i_brother_list_each.tag)
                    # print(i_brother_list_each.xpath('.//a/text()'))
                    # print(i_brother_list_each.xpath('.//a/@href'))
                    # print(ancestor_list_clean, '\n')
                    return detail_url_linshi, detail_title_linshi
                if detail_url_linshi2:
                    # print('找到detail_url_linshi2')
                    # print('i_brother_list_each.tag', i_brother_list_each.tag)
                    # print(i_brother_list_each.xpath('.//text()'))
                    # print(i_brother_list_each.xpath('.//@href'))
                    # print(ancestor_list_clean, '\n')
                    return detail_url_linshi2, detail_title_linshi2
        i_brother_list_fol = i.xpath('./following-sibling::*')  # 获取i的兄弟标签 list类型
        # print('兄弟标签后:', i.xpath('./following-sibling::*'))
        if i_brother_list_fol:
            for num, i_brother_list_each in enumerate(i_brother_list_fol):  # 获取i的子标签的兄弟标签
                # print('第' + str(num + 1) + '次url标签搜索, 遍历i的后兄弟标签')
                detail_url_linshi = i_brother_list_each.xpath('.//a/@href')
                detail_url_linshi2 = i_brother_list_each.xpath('.//@href')
                detail_title_linshi = i_brother_list_each.xpath('.//a/text()')
                detail_title_linshi2 = i_brother_list_each.xpath('.//text()')
                if detail_url_linshi:
                    # print('找到detail_url_linshi')
                    # print('i_brother_list_each.tag', i_brother_list_each.tag)
                    # print(i_brother_list_each.xpath('.//a/text()'))
                    # print(i_brother_list_each.xpath('.//a/@href'))
                    # print(ancestor_list_clean, '\n')
                    return detail_url_linshi, detail_title_linshi
                if detail_url_linshi2:
                    # print('找到detail_url_linshi2')
                    # print('i_brother_list_each.tag', i_brother_list_each.tag)
                    # print(i_brother_list_each.xpath('.//text()'))
                    # print(i_brother_list_each.xpath('.//@href'))
                    # print(ancestor_list_clean, '\n')
                    return detail_url_linshi2, detail_title_linshi2
        if (not i_brother_list_pre) and (not i_brother_list_fol):
            # print('遍历i的兄弟标签'+'没有找到链接')
            pass
        i_father1_list = i.xpath('./..')  # 获取i的父标签
        # print('父标签:', i_father1_list)
        # # print(type(i_father1_list))
        if i_father1_list:
            i_father1 = i_father1_list[0]
            i_father1_brother_list_self = i_father1.xpath('./self::*')  # 获取i的父标签
            if i_father1_brother_list_self:
                for num, i_father1_brother_list_each in enumerate(i_father1_brother_list_self):
                    # print('第' + str(num + 1) + '次url标签搜索, 遍历i的父级自身标签')
                    detail_url_linshi = i_father1_brother_list_each.xpath('.//@href')
                    detail_title_linshi = i_father1_brother_list_each.xpath('.//text()')
                    if detail_url_linshi:
                        # print('找到detail_url_linshi')
                        # print('i_father1_brother_list_each.tag', i_father1_brother_list_each.tag)
                        # print(i_father1_brother_list_each.xpath('.//text()'))
                        # print(i_father1_brother_list_each.xpath('.//@href'))
                        # print(ancestor_list_clean, '\n')
                        return detail_url_linshi, detail_title_linshi
            i_father1_brother_list_pre = i_father1.xpath('./preceding-sibling::*')  # 获取i的2父标签
            if i_father1_brother_list_pre:
                for num, i_father1_brother_list_each in enumerate(i_father1_brother_list_pre):
                    # print('第'+str(num+1)+'次url标签搜索, 遍历i的父级前兄弟标签')
                    # # print(each_i_father1_brother_list)
                    # # print(type(each_i_father1_brother_list))
                    detail_url_linshi = i_father1_brother_list_each.xpath('.//a/@href')
                    detail_title_linshi = i_father1_brother_list_each.xpath('.//a/text()')
                    if detail_url_linshi:
                        # print('找到detail_url_linshi')
                        # print('i_father1_brother_list_each.tag', i_father1_brother_list_each.tag)
                        # print(i_father1_brother_list_each.xpath('.//a/text()'))
                        # print(i_father1_brother_list_each.xpath('.//a/@href'))
                        # print(ancestor_list_clean, '\n')
                        return detail_url_linshi, detail_title_linshi
            i_father1_brother_list_fol = i_father1.xpath('./following-sibling::*')  # 获取i的父标签的兄弟标签
            if i_father1_brother_list_fol:
                for num, i_father1_brother_list_each in enumerate(i_father1_brother_list_fol):
                    # print('第' + str(num + 1) + '次url标签搜索, 遍历i的父级后兄弟标签')
                    # # print(each_i_father1_brother_list)
                    # # print(type(each_i_father1_brother_list))
                    detail_url_linshi = i_father1_brother_list_each.xpath('.//a/@href')
                    detail_title_linshi = i_father1_brother_list_each.xpath('.//a/text()')
                    if detail_url_linshi:
                        # print('找到detail_url_linshi')
                        # print('i_father1_brother_list_each.tag', i_father1_brother_list_each.tag)
                        # print(i_father1_brother_list_each.xpath('.//a/text()'))
                        # print(i_father1_brother_list_each.xpath('.//a/@href'))
                        # print(ancestor_list_clean, '\n')
                        return detail_url_linshi, detail_title_linshi
            if (not i_father1_brother_list_pre) and (not i_father1_brother_list_fol):
                # print('遍历i的父级兄弟标签' + '没有找到链接')
                pass
        # # print(detail_url_linshi)
        if (detail_url_linshi == '') or (detail_url_linshi == []):
            i_father1_list = i.xpath('./..')  # 获取i的父标签
            i_father2_list = i_father1_list[0].xpath('./..')  # 获取i的2级父标签
            # print('2级父标签:', i_father2_list)
            # # print(type(i_father2_list))
            if i_father2_list:
                i_father2 = i_father2_list[0]
                i_father2_brother_list_self = i_father2.xpath('./self::*')  # 获取i的父标签的兄弟标签
                # # print(i_father2_brother_list_self[0].tag)
                if i_father2_brother_list_self:
                    for num, i_father2_brother_list_each in enumerate(i_father2_brother_list_self):
                        # print('第' + str(num + 1) + '次url标签搜索, 遍历i的2级父级自身标签')
                        detail_url_linshi = i_father2_brother_list_each.xpath('.//@href')
                        detail_title_linshi = i_father2_brother_list_each.xpath('.//text()')
                        if detail_url_linshi:
                            # print('找到detail_url_linshi')
                            # print('i_father2_brother_list_each.tag', i_father2_brother_list_each.tag)
                            # print(i_father2_brother_list_each.xpath('.//text()'))
                            # print(i_father2_brother_list_each.xpath('.//@href'))
                            # print(ancestor_list_clean, '\n')
                            return detail_url_linshi, detail_title_linshi
                i_father2_brother_list_pre = i_father2.xpath('./preceding-sibling::*')  # 获取i的父标签的兄弟标签
                if i_father2_brother_list_pre:
                    for num, i_father2_brother_list_each in enumerate(i_father2_brother_list_pre):
                        # print('第' + str(num + 1) + '次url标签搜索, 遍历i的2级父级前兄弟标签')
                        detail_url_linshi = i_father2_brother_list_each.xpath('.//a/@href')
                        detail_title_linshi = i_father2_brother_list_each.xpath('.//a/text()')
                        if detail_url_linshi:
                            # print('找到detail_url_linshi')
                            # print('i_father2_brother_list_each.tag', i_father2_brother_list_each.tag)
                            # print(i_father2_brother_list_each.xpath('.//a/text()'))
                            # print(i_father2_brother_list_each.xpath('.//a/@href'))
                            # print(ancestor_list_clean, '\n')
                            return detail_url_linshi, detail_title_linshi
                i_father2_brother_list_fol = i_father2.xpath('./following-sibling::*')  # 获取i的父标签的兄弟标签
                if i_father2_brother_list_fol:
                    for num, i_father2_brother_list_each in enumerate(i_father2_brother_list_fol):
                        # print('第' + str(num + 1) + '次url标签搜索, 遍历i的2级父级后兄弟标签')
                        detail_url_linshi = i_father2_brother_list_each.xpath('.//a/@href')
                        detail_title_linshi = i_father2_brother_list_each.xpath('.//a/text()')
                        if detail_url_linshi:
                            # print('找到detail_url_linshi')
                            # print('i_father1_brother_list_each.tag', i_father2_brother_list_each.tag)
                            # print(i_father2_brother_list_each.xpath('.//a/text()'))
                            # print(i_father2_brother_list_each.xpath('.//a/@href'))
                            # print(ancestor_list_clean, '\n')
                            return detail_url_linshi, detail_title_linshi
                if (not i_father2_brother_list_pre) and (not i_father2_brother_list_fol):
                    # print('遍历i的2级父级兄弟标签' + '没有找到链接')
                    pass

        # print(ancestor_list_clean, '\n')
        return detail_url_linshi, detail_title_linshi

    def judge_whether_news_list_page(self, url):
        resp = self.downloader.crawl_data(url, None, self.headers, "get")
        if resp:
            # print('resp', resp)
            resp.encoding = self.bianma
            content = resp.text
            # soup = BeautifulSoup(content, 'lxml')

            list_a1, list_b1 = self.data_match(content)
            if list_b1:
                if len(list_b1) >= 5:
                    # print('list_b1', list_b1)
                    self.logger.info('这是新闻列表页：%s' % (url))
                    return True, resp
                else:
                    self.logger.info('不是新闻列表页, pass ：%s' % (url))
                    return False, resp
            else:
                self.logger.info('不是新闻列表页, pass ：%s' % (url))
                return False, resp
        else:
            self.logger.info('列表页未响应:%s' % (url))
            return False, resp

    def judge_whether_news_detail_page(self, url, detail_title_linshi, records):
        # title的余弦相似度
        # title的TF词频相似度计算
        detail_title_judge = records['title_auto']

        title_tf_similarity = self.tf_similarity(detail_title_judge, detail_title_linshi)
        # print('title_tf_similarity:', title_tf_similarity)
        if title_tf_similarity > 0.65:
            # 如果title相似，取比较长的title
            # if len(detail_title_linshi) > len(detail_title_judge):
            #     title = detail_title_linshi
            #     if title:
            #         records["title"] = title
            # else:
            #     title = detail_title_judge
            #     if title:
            #         records["title"] = title
            return True, records
        else:
            # # print('title不匹配, url：', url)
            content = records['content']
            if len(content) > 300:
                # 如果title相似，取比较长的title
                # if len(detail_title_linshi) > len(detail_title_judge):
                #     title = detail_title_linshi
                #     if title:
                #         records["title"] = title
                # else:
                #     title = detail_title_judge
                #     if title:
                #         records["title"] = title
                # print('title不匹配, 但正文长度符合要求, url：', url)
                return True, records
            detail_time_list = records['time_detail_page_list']
            news_list_page_time = records['publish_date']
            for each_detail_page_time in detail_time_list:
                if each_detail_page_time == news_list_page_time:
                    # 如果title相似，取比较长的title
                    # if len(detail_title_linshi) > len(detail_title_judge):
                    #     title = detail_title_linshi
                    #     if title:
                    #         records["title"] = title
                    # else:
                    #     title = detail_title_judge
                    #     if title:
                    #         records["title"] = title
                    # print('title不匹配, 但文章日期相同, 符合要求, url：', url)
                    return True, records
            # print('title不匹配, 正文长度不符合要求, 文章日期不符合要求, url：', url)

        # 正文长度
        content = records['content']
        if len(content) > 250:
            # print("正文长度符合要求")
            return True, records

        # time是否一致
        detail_time_list = records['time_detail_page_list']
        news_list_page_time = records['publish_date']
        for each_detail_page_time in detail_time_list:
            if each_detail_page_time == news_list_page_time:
                # print("详情页文章日期符合要求")
                return True, records
        # print('title不匹配, 正文长度不符合要求, 文章日期不符合要求, url：', url)
        return False, records



    # TF文本词频相似度计算
    def tf_similarity(self, s1, s2):
        def add_space(s):
            return ' '.join(list(s))

        # print('s1:', s1)
        # print('s2:', s2)
        # 将字中间加入空格
        s1, s2 = add_space(s1), add_space(s2)
        try:
            # 转化为TF矩阵
            cv = CountVectorizer(tokenizer=lambda s: s.split())
            corpus = [s1, s2]
            vectors = cv.fit_transform(corpus).toarray()
            # 计算TF系数

            TF = np.dot(vectors[0], vectors[1]) / (norm(vectors[0]) * norm(vectors[1]))
            # print('type TF:', type(TF))
        except Exception as e:
            self.logger.info(e)
            TF = 0
            return TF

        return TF
    # 中文关键句抽取
    # def content_description_extraction(self, content, record):
    #     Suggester = JClass("com.hankcs.hanlp.suggest.Suggester")
    #     suggester = Suggester()
    #     title_array = re.split('[。；？！\n]', content)
    #     title_array_pre_10 = title_array[:10]
    #     for title in title_array_pre_10:
    #         # # print(title)
    #         suggester.addSentence(title)
    #
    #     title = record["title"]
    #     description_list = suggester.suggest(title, 3)
    #     # print(suggester.suggest(title, 3))  # 语义
    #     return description_list

    def updateFile(self, file, old_str, new_str):
        """
        替换文件中的字符串
        :param file:文件名
        :param old_str:就字符串
        :param new_str:新字符串
        :return:
        """
        file_data = ""
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                if old_str in line:
                    line = line.replace(old_str, new_str)
                file_data += line
        with open(file, "w", encoding="utf-8") as f:
            f.write(file_data)


    # # TODO 抽取和清洗部分
    # def run_cleaning(self):
    #     c = 0
    #     save = 0
    #     for i in self.mongo_read_col2.find():
    #         c += 1
    #
    #         if c < 0:
    #             continue
    #
    #         if 'title_final' not in i:
    #             print('c:', c)
    #             title_triple = ''
    #             save += 1
    #             print('save:', save)
    #             # TODO 抽取关键句部分
    #             content, title_list = key_sentence_e(i)
    #             if content:
    #                 # TODO 抽取三元组部分
    #                 title_triple = ltp_e(i, title_list)
    #
    #             else:
    #                 print('没有content')
    #
    #             # TODO title清洗
    #             title1_original, title1_clean = title_cleaning(i, title_triple)
    #
    #             # TODO title_auto清洗
    #             title_auto_clean = title_auto_cleaning(i, c, title1_clean)
    #
    #             # TODO title_final抽取
    #             title_final = title_final_e(i, title_triple, title_auto_clean)
    #
    #             print('清洗成功, url:', i['url'])
    #
    #
    #         else:
    #             pass
    #
    #     return save


if __name__ == '__main__':
    bp1 = ListDetailSpider(SAVE_MONGO_CONFIG)
    bp2 = ListDetailSpider(SAVE_MONGO_CONFIG)
    bp3 = ListDetailSpider(SAVE_MONGO_CONFIG)
    bp4 = ListDetailSpider(SAVE_MONGO_CONFIG)
    bp5 = ListDetailSpider(SAVE_MONGO_CONFIG)


    th1 = MultipleThreading(bp1.run, (5, 1, "2020-06-10"))
    th2 = MultipleThreading(bp2.run, (5, 2, "2020-06-10"))
    th3 = MultipleThreading(bp3.run, (5, 3, "2020-06-10"))
    th4 = MultipleThreading(bp4.run, (5, 4, "2020-06-10"))
    th5 = MultipleThreading(bp5.run, (5, 5, "2020-06-10"))

    # th1.start()
    # th2.start()
    # th3.start()
    # th4.start()
    th5.start()
    print(th5.isAlive())
