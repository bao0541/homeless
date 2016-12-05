# -*- coding: utf-8 -*-
# @Author: tongbo.bao
# @Date:   2016-12-02 17:27:41


import pickle
import os.path
import datetime
import time
import random
import socket
import http.client
import requests
import csv
from bs4 import BeautifulSoup
from model import TradedHouse

grabedPool = {}
grabedPool["data"] = set([])

def get_content(url , data = None):
    header={
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Connection': 'keep-alive',
        'Cookie': 'lianjia_uuid=0aa7a33a-93b4-4a9e-b3c1-af9cbc633883; Hm_lvt_efa595b768cc9dc7d7f9823368e795f1=1480658089; Hm_lpvt_efa595b768cc9dc7d7f9823368e795f1=1480658089; _jzqckmp=1; all-lj=59dc31ee6d382c2bb143f566d268070e; _gat=1; _gat_global=1; _gat_new_global=1; _gat_dianpu_agent=1; select_city=440100; _smt_uid=581aeeac.32e50a81; CNZZDATA1255849599=1678346186-1478158648-%7C1480661072; CNZZDATA1254525948=1419405425-1478154863-%7C1480660477; CNZZDATA1255633284=77773979-1478156487-%7C1480661741; CNZZDATA1255604082=723472224-1478156842-%7C1480663373; _qzja=1.1259462903.1478160044802.1478160044803.1480662871167.1480664485833.1480664491229.0.0.0.10.2; _qzjb=1.1480662871167.8.0.0.0; _qzjc=1; _qzjto=8.1.0; _jzqa=1.1923535260492789200.1478160045.1478160045.1480662359.2; _jzqc=1; _jzqb=1.11.10.1480662359.1; _ga=GA1.2.1959356750.1477991005; lianjia_ssid=1ab87500-df7e-42e5-9167-d8e04c9cc03a',
        'Host': 'gz.lianjia.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.235'
    }
    timeout = random.choice(range(80, 180))
    while True:
        try:
            rep = requests.get(url,headers = header,timeout = timeout)
            rep.encoding = 'utf-8'
            break
        except socket.timeout as e:
            print( '3:', e)
            time.sleep(random.choice(range(8,15)))

        except socket.error as e:
            print( '4:', e)
            time.sleep(random.choice(range(20, 60)))

        except http.client.BadStatusLine as e:
            print( '5:', e)
            time.sleep(random.choice(range(30, 80)))

        except http.client.IncompleteRead as e:
            print( '6:', e)
            time.sleep(random.choice(range(5, 15)))

    return rep.text


# def before_grab(func):
#     def wapper(*args, **kwargs):
#         if os.path.exists("grabedPool.set"):
#             with open("grabedPool.set", "rb") as f:
#                 grabedPool["data"] = pickle.load(f)
#         else:
#             grabedPool["data"] = set([])

#         func(*args, **kwargs)
#     return wapper

# def after_grab(func):
#     def wapper(*args, **kwargs):
#         try:
#             func(*args, **kwargs)
#         except Exception as e:
#             raise
#         finally:
#             with open("grabedPool.set" , "wb") as f:
#                 pickle.dump(grabedPool["data"], f)
#     return wapper

# @before_grab
def start():
    print(grabedPool["data"])
    a = []
    i = 1
    while i <= 100:
        page = "http://gz.lianjia.com/chengjiao/pg{0}/".format(str(i))
        i += 1
        r = grab(page)
        # print(r)
        a.extend(r)
    print(a)
    write_data(a, 'fang.csv')
    print('已经存放到fang.csv')
        
# @after_grab
def grab(url):
    print("try to grab page ", url)
    r = get_content(url)

    soup = BeautifulSoup(r, "lxml")

    tradedHoustList = soup.find("ul", class_="listContent").find_all('li')
    temp = []

    if not tradedHoustList:
        return 

    for item in tradedHoustList:
        # 房屋详情链接，唯一标识符
        houseUrl = item.find('div', class_="title").a["href"] or ''

        if houseUrl in grabedPool["data"]:
            print(houseUrl, " 已经存在，跳过，开始抓取下一个")
            continue

        print('开始抓取' , houseUrl)

        # 抓取 小区，户型，面积
        title = item.find('div', class_="title").a
        if title:
            xiaoqu, houseType, square = (item.find('div', class_="title").a.string.split(" "))
        else:
            xiaoqu, houseType, square = ('Nav', 'Nav', 'Nav')

        # 成交时间，朝向，楼层
        orientation = item.find('div', class_='houseInfo').get_text()
        floor = item.find('div', class_='positionInfo').get_text()
        dealHouseInfo = item.find('div', class_="dealHouseInfo").find('span', class_='dealHouseTxt')
        if dealHouseInfo is None:
            buildInfo = ''
        else:
            buildInfo = dealHouseInfo.find('span').string

        tradeData = item.find("div", class_='dealDate').string
        unitPrice = item.find("div", class_='unitPrice').find("span", class_='number')
        if unitPrice is None:
            perSquarePrice = ''
        else:
            perSquarePrice = unitPrice.string
        totalPriceDiv = item.find("div", class_='totalPrice').find("span", class_='number')
        if totalPriceDiv is None:
            totalPrice = ''
        else:
            totalPrice = totalPriceDiv.string
        
        temp.append([
            xiaoqu,
            houseType,
            square,
            houseUrl,
            orientation,
            floor,
            buildInfo,
            tradeData,
            perSquarePrice,
            totalPrice,
        ])
        # print(temp)
        # # 通过 ORM 存储到 sqlite
        # tradeItem = TradedHouse(
        #                         xiaoqu = xiaoqu,
        #                         houseType = houseType,
        #                         square = square,
        #                         houseUrl = houseUrl,
        #                         orientation = orientation,
        #                         floor = floor,
        #                         buildInfo = buildInfo,
        #                         tradeDate = tradeData,
        #                         perSquarePrice = perSquarePrice,
        #                         totalPrice = totalPrice,
        #                         )

        # tradeItem.save()

        # 添加到已经抓取的池
        grabedPool["data"].add(houseUrl)
        
    # 抓取完成后，休息几秒钟，避免给对方服务器造成大负担
    # time.sleep(random.randint(10,30))
    # print(temp)
    return temp

def write_data(data, name):
    file_name = name
    with open(file_name, 'a', errors='ignore', newline='') as f:
        f_csv = csv.writer(f)
        f_csv.writerows(data)

if __name__== "__main__":
    start()
