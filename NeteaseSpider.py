#!/usr/bin/python
# -*- coding: utf-8 -*-

import calendar
import datetime
import gc
import json
import multiprocessing
import random
import motor
import aiohttp
import asyncio

import regex as re
import requests
from bs4 import BeautifulSoup
from pymongo import *


def get_site_url(sitename=0):
    site_list = [[u'新闻', 'http://news.163.com/special/0001220O/news_json.js',
                  'http://snapshot.news.163.com/wgethtml/http+!!news.163.com!special!0001220O!news_json.js/'],
                 [u'娱乐', 'http://ent.163.com/special/00032IAD/ent_json.js',
                  'http://snapshot.news.163.com/wgethtml/http+!!ent.163.com!special!00032IAD!ent_json.js/'],
                 [u'体育', 'http://sports.163.com/special/0005rt/news_json.js',
                  'http://snapshot.news.163.com/wgethtml/http+!!sports.163.com!special!0005rt!news_json.js/'],
                 [u'财经', 'http://money.163.com/special/00251G8F/news_json.js',
                  'http://snapshot.news.163.com/wgethtml/http+!!money.163.com!special!00251G8F!news_json.js/'],
                 [u'科技', 'http://tech.163.com/special/00094IHV/news_json.js',
                  'http://snapshot.news.163.com/wgethtml/http+!!tech.163.com!special!00094IHV!news_json.js/'],
                 [u'手机', 'http://mobile.163.com/special/00112GHS/phone_json.js',
                  'http://snapshot.news.163.com/wgethtml/http+!!tech.163.com!mobile!special!00112GHS!phone_json.js/'],
                 [u'女人', 'http://lady.163.com/special/00264IIC/lady_json.js',
                  'http://snapshot.news.163.com/wgethtml/http+!!lady.163.com!special!00264IIC!lady_json.js/']]
    return site_list[sitename]


## 由于程序经常因为网络原因崩溃，所以把所有网络处理的部分集中到一个函数。
## 如果这个方法依旧不行就考虑按照TODO列表里的Save&Load大法
def get_network_content(url):
    while True:
        try:
            ## TODO:添加超时判定
            r = requests.get(url + '?' + str(random.random()), headers={
                'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0',
                'referer': '163.com'})
        except:
            continue
        break
    return r.text


def get_child_classification(category):
    return_value = list()
    for elem in category:
        return_value.append(elem[u'n'])
    return return_value


def get_json(year=2014, month=1, day=1, newsType=0):
    if year == datetime.datetime.now().year and \
                    month == datetime.datetime.now().month and \
                    day == datetime.datetime.now().day:
        return get_network_content(get_site_url(newsType)[2] + date_format(
            year, month, day) + '/0.js')
    else:
        return get_network_content(
            get_site_url(newsType)[2] + date_format(year, month, day) + '/0.js')


def date_format(year=2014, month=1, day=1):
    date_string = datetime.date(year, month, day).isoformat().split('-')
    return date_string[0] + '-' + date_string[1] + '/' + date_string[2]


def json_format(year=2014, month=1, day=1, newsType=0):
    text = get_json(year, month, day, newsType)
    return_value = list()
    if text.startswith('var data=') is True:
        tmp = re.sub(',*,', ',', text.lstrip('var data=').rstrip(';').replace('\n', '').replace(',[]', ''))
        if newsType is not 0:
            tmp = re.sub(r'(,|\{)([a-z]*?)(:)', r'\1"\2"\3', tmp)
            tmp = re.sub(r'(\[),(\{)', r'\1\2', tmp.replace('\\', '/'))
        try:
            tmp_value = json.loads(tmp, strict=False)
        except:
            return list()
        child_classification = get_child_classification(tmp_value[u'category'])
        if newsType is 1:
            value_list = tmp_value[u'ent']
        else:
            value_list = tmp_value[u'news']
        for list0 in value_list:
            for list1 in list0:
                if list1 is not None:
                    if list1[u'l'].find('photoview') is -1 and list1[u'l'].find('blog') is -1:
                        return_value.append(
                            [list1[u'p'].split()[0], list1[u'p'].split()[1], get_site_url(newsType)[0],
                             child_classification[list1[u'c']], list1[u'l'],
                             list1[u't']])
        del tmp
        del text
        del tmp_value
        del child_classification
        del value_list
        gc.collect()
    return return_value


def send_to_mongodb(insert_data):
    db = MongoClient()
    collections = db.client['neteasenews']['news']
    collections.insert_one(insert_data)
    db.close()


def get_news(url):
    date = str()
    html = get_network_content(url)
    html = re.sub(r'<script.*?</script>', '', html)
    html = re.sub(r'(<div id="endText">)(.*?)(<p>)', r'\1\3', html)
    soup = BeautifulSoup(html, 'lxml')
    alls = soup.select('#endText')
    for div in alls:
        p_in_div = div.find_all('p')
        for p_tag in p_in_div:
            if len(p_tag.text) is not 0:
                date += p_tag.text + u'\n'
    del html
    del soup
    del alls
    return date


##TODO:重构此部分代码，使得每一条新闻使用一个单独的进程来处理，以减少因网络I/O带来的延迟
def childProcess(year=2014, month=1, day=1, newsType=0):
    jsonlist = json_format(year=year, month=month, day=day, newsType=newsType)
    if len(jsonlist) is not 0:
        for items in jsonlist:
            send_to_mongodb(
                {'date': items[0], 'time': items[1], 'class': items[2], 'childclass': items[3],
                 'url': items[4], 'title': items[5], 'content': get_news(str(items[4]))})
        del jsonlist
    gc.collect()


# 网易的接口最多只能获取到2014年3月22日的新闻。再往前也有对应的接口，不过已经无法工作
# 如果改用腾讯的接口，虽然能获取到2009年1月1日的新闻，但网页处理方面比较麻烦（主要是腾讯的网页改过版），放弃
def main(start_year=2014, start_month=1, start_day=1, start_news_type=0):
    for year in range(2014, datetime.datetime.now().year + 1):
        if year < start_year:
            continue
        for month in range(1, 13):
            if month < start_month:
                continue
            for day in range(1, calendar.monthrange(year, month)[1] + 1):
                if day < start_day:
                    continue
                pool = multiprocessing.Pool()
                for newsType in range(0, 7):
                    if newsType < start_news_type:
                        continue
                    pool.apply_async(childProcess, args=(year, month, day, newsType))
                pool.close()
                pool.join()
                gc.collect()


## TODO:程序崩溃时保留状态，以便恢复
# def restoreProcess():


## TODO:从上次的状态中恢复
# def recoveryFromCrash():


if __name__ == '__main__':
    main()
