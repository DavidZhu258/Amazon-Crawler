import json
import re
import sys
import time
import pymysql
import requests
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import html  # 导入html模块用于转义HTML实体
import pandas as pd
import os
import aiomysql
from pymysql.constants import CLIENT
import logging
from datetime import datetime, timedelta
import random
import colorlog
import uuid
from tqdm import tqdm
from urllib.parse import urlparse, parse_qs

import asyncio
import random
import re
import json
from lxml import etree
import ddddocr
import curl_cffi
import time
from scrapy.http import HtmlResponse
from curl_cffi.requests.session import AsyncSession
from curl_cffi.requests import Cookies
from curl_cffi.const import CurlHttpVersion

from redis.asyncio import Redis
import json
from typing import Optional
from asyncio import Queue

url_first = 'https://www.amazon.com'
ocr = ddddocr.DdddOcr()


if sys.platform != 'win32':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 将当前目录添加到系统路径
    sys.path.append(current_dir)
else:
    current_dir = os.getcwd()
    

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "device-memory": "8",
    "dnt": "1",
    "downlink": "4.85",
    "dpr": "1",
    "ect": "4g",
    "pragma": "no-cache",
    "priority": "u=0, i",
    "referer": "https://www.amazon.com/",
    "rtt": "250",
    "sec-ch-device-memory": "8",
    "sec-ch-dpr": "1",
    "sec-ch-ua": "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\", \"Not?A_Brand\";v=\"99\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-ch-ua-platform-version": "\"10.0.0\"",
    "sec-ch-viewport-width": "1918",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "viewport-width": "1918"
}
CHANGE_LOCAL_HEADERS = {
    # 'anti-csrftoken-a2z': anti_csrftoken_a2z,
    'content-type': 'application/json',
    # 'Accept': 'text/html,application/xhtml xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'zh-CN,zh;q=0.9', 'Cache-Control': 'no-cache', 'Dnt': '1', 'Pragma': 'no-cache',
    'Priority': 'u=0, i',
    'Sec-Ch-Ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
    'Sec-Ch-Ua-Mobile': '?0', 'Sec-Ch-Ua-Platform': '"Windows"', 'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'none', 'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    # 'Accept-Encoding': 'gzip, deflate',
    'Referer': 'https://www.amazon.com/portal-migration/hz/glow/get-location-label?storeContext=generic&pageType=Gateway&actionSource=desktop-modal',
}
IMAGE_HEADERS = {
    'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
    'accept-language': 'zh-CN,zh;q=0.9',
    'cache-control': 'no-cache',
    'dnt': '1',
    'pragma': 'no-cache',
    'priority': 'u=2.json, i',
    # 'referer': url if url else response.url,
    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'image',
    'sec-fetch-mode': 'no-cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/538.36'
}
h = "0123456789"


def creat_csm_sid():
    o = ""
    for _ in range(18):
        p = random.randint(0, len(h) - 1)
        o += h[p]
    return o[:3] + "-" + o[3:10] + "-" + o[10:17]


async def get_ubid_main_url_cookie(headers_first, proxies=None):
    # if proxies is None:
    #     proxies = {}
    while True:
        session = AsyncSession()
        session.proxies.update(proxies or {})
        cookie = Cookies()
        cookie.set(name='csm_sid', value=creat_csm_sid())
        cookie.set(name='lc-main', value="en_US")
        session.cookies = cookie
        session.http_version = CurlHttpVersion.V1_1
        try:
            session.headers.clear()
            session.headers.update(headers_first)
            impersonate = [
                # Edge
                "edge99",
                "edge101",
                # Chrome
                "chrome99",
                "chrome100",
                "chrome101",
                "chrome104",
                "chrome107",
                "chrome110",
                "chrome116",
                "chrome119",
                "chrome120",
                "chrome123",
                "chrome124",
                "chrome99_android",
                # Safari
                "safari15_3",
                "safari15_5",
                "safari17_0",
                "safari17_2_ios",
                # alias
                "chrome",
                "edge",
                "safari",
                "safari_ios",
                "chrome_android",
            ]
            resp_home = await session.get(url_first, impersonate=random.choice(impersonate))
            if resp_home.status_code == 503:
                continue
            resp_home_html_resp = HtmlResponse(body=resp_home.content, url=url_first, encoding='utf-8')
            while "Click the button below to continue shopping" in resp_home_html_resp.text:
                amzn = resp_home_html_resp.xpath('//input[@name="amzn"]/@value').get()
                amzn_r = resp_home_html_resp.xpath('//input[@name="amzn-r"]/@value').get()
                field_keywords = resp_home_html_resp.xpath('//input[@name="field-keywords"]/@value').get()
                resp_home_html_resp = await session.get(f"http://www.amazon.com/errors/validateCaptcha?amzn={amzn}&amzn-r={amzn_r}&field-keywords={field_keywords}")
                resp_home_html_resp = HtmlResponse(body=resp_home_html_resp.content, url=url_first)
            while 'Enter the characters you see below' in resp_home_html_resp.text:
                resp_home = await ocr_image(
                    None, resp_home_html_resp, t_h=headers_first, session_t=session, proxies=proxies
                )
                resp_home_html_resp = HtmlResponse(body=resp_home.content, url=url_first, encoding='utf-8')
            if resp_home.status_code == 503:
                continue
            if rid := dict(resp_home.headers).get("x-amz-rid"):
                session.cookies.set('csm-hit',
                                    f'tb:{rid}+s-{rid}|{int(time.time() * 1000)}&t:{int(time.time() * 1000)}&adb:adblk_no')
            result = resp_home_html_resp.xpath(
                "//script[contains(text(), 'GwInstrumentation.markH1Af')]/text()"
            ).extract_first()
            path = re.search("\"(.*)\"", result).group(1)
            try:
                anti_csrftoken_a2z = json.loads(
                    resp_home_html_resp.xpath(
                        '//span[@id="nav-global-location-data-modal-action"]/@data-a-modal'
                    ).extract_first()
                )['ajaxHeaders']['anti-csrftoken-a2z']
                headers_first['anti-csrftoken-a2z'] = anti_csrftoken_a2z
            except Exception as e:
                continue
            break
        except Exception as e:
            print(f"Error in get_ubid_main_url_cookie: {e}")
            continue

    ubid_main_cookie_url = "https://www.amazon.com" + path
    # resp = await session.get(url='https://myip.ipip.net/')
    # print(resp.text)
    return ubid_main_cookie_url, session


async def ocr_image(request, response, h=None, url=None, t_h=None, session_t=None,
                    proxies=None) -> curl_cffi.requests.models.Response:
    # session = Session()
    try:
        html = etree.HTML(h if h else response.text)
        html = html.xpath("//input/@value")
    except Exception as e:
        print(e)
    headers_image = IMAGE_HEADERS
    headers_image['referer'] = url if url else response.url
    re_session = session_t
    image_url = [i for i in etree.HTML(h if h else response.text).xpath("//img/@src") if 'jpg' in i][0]
    image_response = await re_session.get(image_url, headers=headers_image, proxies=proxies)
    # no_img = etree.HTML(h if h else response.text).xpath("//img/@src")[1].replace("=0", "=1")
    # no_response = re_session.get(no_img, headers=headers_image, proxies=proxies)
    result = ocr.classification(image_response.content).upper()
    response = await re_session.get(
        f'https://www.amazon.com/errors/validateCaptcha?amzn={html[0]}&amzn-r={html[1]}&field-keywords={result.upper()}',
        headers=t_h if t_h else None, proxies=proxies)
    return response


async def run_sync_function(sessionQueue, proxies):
    # 将同步函数放到线程中执行
    # c = await asyncio.to_thread(session_To_Queue, proxies)
    try:
        c = await renew_session(proxies)
        if c:
            await sessionQueue.put(c)
    except Exception as e:
        print(e)


async def use_ubid_cookie_to_get_CSRF_TOKEN(ubid_main_cookie_url, session, headers_first, proxies):
    #      ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓     只需要发个请求获取ubid_main_cookie     ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓

    _response_405 = await session.post(ubid_main_cookie_url, headers=headers_first)
    while True:
        _response_405 = await session.get(
            f"https://www.amazon.com/portal-migration/hz/glow/get-rendered-toaster?pageType=Gateway&aisTransitionState=null&rancorLocationSource=REALM_DEFAULT&_={int(time.time() * 1000)}",
            headers=headers_first
        )
        if _response_405.status_code == 200:
            break

    get_CSRF_URL = 'https://www.amazon.com/portal-migration/hz/glow/get-rendered-address-selections?deviceType=desktop&pageType=Gateway&storeContext=NoStoreName&actionSource=desktop-modal'
    while True:
        CSRF_resp = await session.get(get_CSRF_URL, headers=headers_first)
        if CSRF_resp.status_code == 200:
            break

    CSRF_html_resp = HtmlResponse(body=CSRF_resp.content, url=get_CSRF_URL, encoding='utf-8')
    # if 'United States' in CSRF_html_resp.text:
    #     return session
    try:
        anti_csrftoken_a2z = re.search('CSRF_TOKEN : "(.*?)"', CSRF_html_resp.text).group(1)
    except Exception as e:
        return session
    change_local_url = "https://www.amazon.com/portal-migration/hz/glow/address-change?actionSource=glow"
    change_local_payload = json.dumps({
        "locationType": "LOCATION_INPUT",
        "zipCode": "10001",
        "deviceType": "web",
        "storeContext": "generic",
        "pageType": "Gateway",
        "actionSource": "glow"
    })
    change_local_headers = CHANGE_LOCAL_HEADERS.copy()
    change_local_headers['anti-csrftoken-a2z'] = anti_csrftoken_a2z
    ressult_response = await session.post(change_local_url, headers=change_local_headers, data=change_local_payload)
    return session


async def renew_session(proxies):
    headers_first = HEADERS.copy()
    while True:
        ubid_main_cookie_url, session = await get_ubid_main_url_cookie(headers_first, proxies)
        try:
            session = await use_ubid_cookie_to_get_CSRF_TOKEN(ubid_main_cookie_url, session, headers_first, proxies)
        except Exception as e:
            print(f"Renew session failed: {e}")
            continue
        if session != None:
            break
    session.use_num = 0
    return session


# redis配置 (从环境变量读取)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
MYSQL_URL = os.getenv("MYSQL_HOST", "localhost")
# MySQL配置 (从环境变量读取)
MYSQL_CONFIG = {
    'host': MYSQL_URL,
    'user': os.getenv("MYSQL_USER", "root"),
    'password': os.getenv("MYSQL_PASSWORD", ""),
    'db': os.getenv("MYSQL_DB", "amazon"),
    'port': int(os.getenv("MYSQL_PORT", "3306")),
    'autocommit': True
}

# 用于存储本次爬取的商品ID
current_session_asins = set()


# 设置日志
def setup_logger():
    # 创建logs目录
    log_dir = os.path.join(current_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # 创建日志文件名,包含时间戳
    log_file = os.path.join(log_dir, f'amazon_crawler_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

    # 创建彩色日志格式化器
    color_formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s - %(levelname)s - %(message)s%(reset)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        },
        secondary_log_colors={},
        style='%'
    )

    # 文件处理器使用普通格式
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # 文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(file_formatter)

    # 控制台处理器（带颜色）
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(color_formatter)

    # 获取logger
    logger = logging.getLogger('amazon_crawler')
    logger.setLevel(logging.INFO)

    # 清除现有的处理器
    logger.handlers.clear()

    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# 创建logger实例
logger = setup_logger()


async def save_to_mysql(pool, product_data):
    """保存单条数据到MySQL"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # 准备SQL语句
            sql = """INSERT INTO amazon_products (Handle, Title, Body_HTML, Vendor, Type, Tags, \
                                                  Option1_Name, Option1_Value, Option2_Name, Option2_Value, \
                                                  Variant_SKU, Variant_Price, Variant_Compare_At_Price, \
                                                  Image_Src, Image_Position, Status, SEO_Title, SEO_Description, \
                                                  Google_Shopping_Google_Product_Category, Google_Shopping_Gender, \
                                                  Google_Shopping_Age_Group, Google_Shopping_MPN, \
                                                  Google_Shopping_AdWords_Grouping, Google_Shopping_AdWords_Labels, \
                                                  Google_Shopping_Condition, Google_Shopping_Custom_Product, \
                                                  Google_Shopping_Custom_Label_0, Google_Shopping_Custom_Label_1, \
                                                  Google_Shopping_Custom_Label_2, Google_Shopping_Custom_Label_3, \
                                                  Google_Shopping_Custom_Label_4, Variant_Image, \
                                                  Variant_Weight_Unit, Variant_Tax_Code, Cost_per_item) \
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                             %s, %s, %s, %s, %s) ON DUPLICATE KEY \
                     UPDATE
                         Handle = \
                     VALUES (Handle), Title = \
                     VALUES (Title), Body_HTML = \
                     VALUES (Body_HTML), Vendor = \
                     VALUES (Vendor), Type = \
                     VALUES (Type), Tags = \
                     VALUES (Tags), Option1_Name = \
                     VALUES (Option1_Name), Option1_Value = \
                     VALUES (Option1_Value), Option2_Name = \
                     VALUES (Option2_Name), Option2_Value = \
                     VALUES (Option2_Value), Variant_Price = \
                     VALUES (Variant_Price), Variant_Compare_At_Price = \
                     VALUES (Variant_Compare_At_Price), Image_Src = \
                     VALUES (Image_Src), Image_Position = \
                     VALUES (Image_Position), Status = \
                     VALUES (Status), SEO_Title = \
                     VALUES (SEO_Title), SEO_Description = \
                     VALUES (SEO_Description), Google_Shopping_Google_Product_Category = \
                     VALUES (Google_Shopping_Google_Product_Category), Google_Shopping_Gender = \
                     VALUES (Google_Shopping_Gender), Google_Shopping_Age_Group = \
                     VALUES (Google_Shopping_Age_Group), Google_Shopping_MPN = \
                     VALUES (Google_Shopping_MPN), Google_Shopping_AdWords_Grouping = \
                     VALUES (Google_Shopping_AdWords_Grouping), Google_Shopping_AdWords_Labels = \
                     VALUES (Google_Shopping_AdWords_Labels), Google_Shopping_Condition = \
                     VALUES (Google_Shopping_Condition), Google_Shopping_Custom_Product = \
                     VALUES (Google_Shopping_Custom_Product), Google_Shopping_Custom_Label_0 = \
                     VALUES (Google_Shopping_Custom_Label_0), Google_Shopping_Custom_Label_1 = \
                     VALUES (Google_Shopping_Custom_Label_1), Google_Shopping_Custom_Label_2 = \
                     VALUES (Google_Shopping_Custom_Label_2), Google_Shopping_Custom_Label_3 = \
                     VALUES (Google_Shopping_Custom_Label_3), Google_Shopping_Custom_Label_4 = \
                     VALUES (Google_Shopping_Custom_Label_4), Variant_Image = \
                     VALUES (Variant_Image), Variant_Weight_Unit = \
                     VALUES (Variant_Weight_Unit), Variant_Tax_Code = \
                     VALUES (Variant_Tax_Code), Cost_per_item = \
                     VALUES (Cost_per_item), updated_at = CURRENT_TIMESTAMP \
                  """

            # 准备数据
            values = (
                product_data.get('Handle', ''),
                product_data.get('Title', ''),
                product_data.get('Body_HTML', ''),
                product_data.get('Vendor', ''),
                product_data.get('Type', ''),
                product_data.get('Tags', ''),
                product_data.get('Option1_Name', 'SIZE'),
                product_data.get('Option1_Value', ''),
                product_data.get('Option2_Name', 'COLOUR'),
                product_data.get('Option2_Value', ''),
                product_data.get('Variant_SKU', ''),
                product_data.get('Variant_Price', 0.00),
                product_data.get('Variant_Compare_At_Price', 0.00),
                product_data.get('Image_Src', ''),
                product_data.get('Image_Position', 0),
                product_data.get('Status', 'active'),
                product_data.get('SEO_Title', ''),
                product_data.get('SEO_Description', ''),
                product_data.get('Google_Shopping_Google_Product_Category', ''),
                product_data.get('Google_Shopping_Gender', ''),
                product_data.get('Google_Shopping_Age_Group', ''),
                product_data.get('Google_Shopping_MPN', ''),
                product_data.get('Google_Shopping_AdWords_Grouping', ''),
                product_data.get('Google_Shopping_AdWords_Labels', ''),
                product_data.get('Google_Shopping_Condition', ''),
                product_data.get('Google_Shopping_Custom_Product', ''),
                product_data.get('Google_Shopping_Custom_Label_0', ''),
                product_data.get('Google_Shopping_Custom_Label_1', ''),
                product_data.get('Google_Shopping_Custom_Label_2', ''),
                product_data.get('Google_Shopping_Custom_Label_3', ''),
                product_data.get('Google_Shopping_Custom_Label_4', ''),
                product_data.get('Variant_Image', ''),
                product_data.get('Variant_Weight_Unit', ''),
                product_data.get('Variant_Tax_Code', ''),
                product_data.get('Cost_per_item', '')
            )

            try:
                await cursor.execute(sql, values)
                # 记录本次插入的商品SKU
                current_session_asins.add(product_data.get('Variant_SKU', ''))
                # logger.info(f"Successfully saved/updated SKU: {product_data.get('Variant_SKU', '')}")
                return True
            except Exception as e:
                logger.error(f"Error saving to MySQL: {e} asin : {product_data.get('Variant_SKU', '')}")
                return False


async def export_mysql_to_excel(pool, excel_path, limit=None):
    """从MySQL导出数据到CSV,按Image_Position排序,只导出本次爬取的商品"""
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # 使用IN子句筛选本次爬取的商品,并按Image_Position排序
                if current_session_asins:
                    placeholders = ', '.join(['%s'] * len(current_session_asins))
                    query = f"""
                        SELECT 
                            Handle,
                            Title,
                            Body_HTML as 'Body (HTML)',
                            Vendor,
                            Type,
                            Tags,
                            'TRUE' as Published,
                            Option1_Name as 'Option1 Name',
                            Option1_Value as 'Option1 Value',
                            Option2_Name as 'Option2 Name',
                            Option2_Value as 'Option2 Value',
                            '' as 'Option3 Name',
                            '' as 'Option3 Value',
                            Variant_SKU as 'Variant SKU',
                            '0' as 'Variant Grams',
                            'shopify' as 'Variant Inventory Tracker',
                            '100' as 'Variant Inventory Qty',
                            'deny' as 'Variant Inventory Policy',
                            'manual' as 'Variant Fulfillment Service',
                            Variant_Price as 'Variant Price',
                            Variant_Compare_At_Price as 'Variant Compare At Price',
                            'TRUE' as 'Variant Requires Shipping',
                            'TRUE' as 'Variant Taxable',
                            '' as 'Variant Barcode',
                            Image_Src as 'Image Src',
                            Image_Position as 'Image Position',
                            '' as 'Image Alt Text',
                            'FALSE' as 'Gift Card',
                            SEO_Title as 'SEO Title',
                            SEO_Description as 'SEO Description',
                            Google_Shopping_Google_Product_Category as 'Google Shopping / Google Product Category',
                            Google_Shopping_Gender as 'Google Shopping / Gender',
                            Google_Shopping_Age_Group as 'Google Shopping / Age Group',
                            Google_Shopping_MPN as 'Google Shopping / MPN',
                            Google_Shopping_AdWords_Grouping as 'Google Shopping / AdWords Grouping',
                            Google_Shopping_AdWords_Labels as 'Google Shopping / AdWords Labels',
                            Google_Shopping_Condition as 'Google Shopping / Condition',
                            Google_Shopping_Custom_Product as 'Google Shopping / Custom Product',
                            Google_Shopping_Custom_Label_0 as 'Google Shopping / Custom Label 0',
                            Google_Shopping_Custom_Label_1 as 'Google Shopping / Custom Label 1',
                            Google_Shopping_Custom_Label_2 as 'Google Shopping / Custom Label 2',
                            Google_Shopping_Custom_Label_3 as 'Google Shopping / Custom Label 3',
                            Google_Shopping_Custom_Label_4 as 'Google Shopping / Custom Label 4',
                            Variant_Image as 'Variant Image',
                            Variant_Weight_Unit as 'Variant Weight Unit',
                            Variant_Tax_Code as 'Variant Tax Code',
                            Cost_per_item as 'Cost per item',
                            Status,
                            '' as Collection
                        FROM amazon_products 
                        WHERE Variant_SKU IN ({placeholders})
                        ORDER BY Handle, Image_Position ASC
                    """
                    await cursor.execute(query, tuple(current_session_asins))
                    rows = await cursor.fetchall()

                    if rows:
                        # 获取列名
                        columns = [i[0] for i in cursor.description]

                        # 创建DataFrame
                        df = pd.DataFrame(rows, columns=columns)

                        # 获取唯一的Handle列表
                        unique_handles = df['Handle'].unique()

                        # 如果有limit限制,只取前limit个商品的数据
                        if limit and len(unique_handles) > limit:
                            selected_handles = unique_handles[:limit+1]
                            df = df[df['Handle'].isin(selected_handles)]

                        # 将文件扩展名从.xlsx改为.csv
                        csv_path = excel_path.replace('.xlsx', '.csv')
                        
                        # 保存为CSV格式，使用utf-8-sig编码解决中文乱码问题
                        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                        
                        logger.info(f"数据已导出到CSV文件: {csv_path}")
                        logger.info(f"导出商品数量: {len(df['Handle'].unique())}")
                    else:
                        logger.info("没有找到本次爬取的商品数据")
                else:
                    logger.info("本次没有爬取到任何商品数据")
    except Exception as e:
        if isinstance(e, (aiomysql.Error, aiomysql.OperationalError)) or 'MySQL' in str(e):
            logger.error(f"MySQL Error occurred: {e}")
            try:
                # 如果存在旧的连接池,先关闭
                if 'pool' in locals():
                    pool.close()
                    await pool.wait_closed()

                # 创建新的MySQL连接池
                pool = await aiomysql.create_pool(
                    host=MYSQL_CONFIG['host'],
                    user=MYSQL_CONFIG['user'],
                    password=MYSQL_CONFIG['password'],
                    db=MYSQL_CONFIG['db'],
                    port=MYSQL_CONFIG['port'],
                    autocommit=True,
                    connect_timeout=7200,
                    pool_recycle=1800,
                    maxsize=15,
                    minsize=5,
                    client_flag=CLIENT.MULTI_STATEMENTS
                )
                logger.info("Successfully recreated MySQL connection pool")
                return pool
            except Exception as pool_error:
                logger.error(f"Failed to recreate MySQL pool: {pool_error}")
                raise
        else:
            # 非MySQL错误的处理
            logger.error(f"Non-MySQL error occurred: {e}")
            raise


def is_captcha_page(response_text):
    """检查是否是验证码页面"""
    captcha_indicators = [
        "Enter the characters you see below",
        "Type the characters you see in this image",
        "Sorry, we just need to make sure you're not a robot"
    ]
    return any(indicator in response_text for indicator in captcha_indicators)


async def get_product_urls(keyword, target_count, session, proxies=None):
    """获取指定数量的商品URL,使用aiohttp session,遇到反爬虫自动重置session"""
    headers = {
        'accept': 'text/html,image/webp,*/*',
        'accept-language': 'zh-CN,zh-TW;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'device-memory': '8',
        'downlink': '10',
        'dpr': '1',
        'ect': '4g',
        'origin': 'https://www.amazon.com',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.amazon.com/s?k=nike&page=4&qid=1750181679&xpid=6ksvMIOV1cRib&ref=sr_pg_4',
        'rtt': '250',
        'sec-ch-device-memory': '8',
        'sec-ch-dpr': '1',
        'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"10.0.0"',
        'sec-ch-viewport-width': '1920',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        'viewport-width': '1920',
        'x-amazon-rush-fingerprints': 'AmazonRushAssetLoader:1202F8AA9B9E3A62A246BF3FA42812770110C222|AmazonRushFramework:3D1FA039FF626AA6C7491C07020F748C1E108C55|AmazonRushRouter:935F5ECC39032E6CE0E9C33A0426BEBA1A06E5BA',
        'x-amazon-s-fallback-url': '',
        'x-amazon-s-mismatch-behavior': 'ABANDON',
        'x-amazon-s-swrs-version': '683AF8B7BF09B53ADC3105276C93F9D6,D41D8CD98F00B204E9800998ECF8427E',
        'x-requested-with': 'XMLHttpRequest',
    }

    collected_asins = set()
    page = 1
    logger.info(f"开始收集商品URL,目标数量: {target_count}")

    # 使用传入的session
    current_session = session
    try_again = 0

    while len(collected_asins) < target_count:
        logger.info(f"正在获取第 {page} 页商品列表,当前已收集: {len(collected_asins)}")
        try:
            # url = 'https://www.amazon.com/s?i=fashion&rh=n%3A7141123011%2Cp_n_feature_thirty-two_browse-bin%3A121075132011%2Cp_123%3A427346&dc&ds=v1%3AToSwXGnyJKOLK%2FIyrCE3W2uD1O%2BXFqsJPP0yZC4dGKo&crid=10707FETRGKHT&qid=1749706449&rnid=85457740011&sprefix=%2Caps%2C1363&ref=sr_nr_p_123_1'
            url = 'https://www.amazon.com/s/query'
            # url2 = 'https://www.amazon.com/s'
            json_data = {
                'page-content-type': 'atf',
                'prefetch-type': 'rq',
                'customer-action': 'pagination',
            }
            params = {'k': keyword, 'page': page, 'qid': '1750181694',  'ref': 'sr_pg_4',  'xpid': 'oEJeYFWcUVJTo',}

            params2 = {
                'k': keyword,
                'page': page,
                'crid': '1K5VVEYF6AN43',
                'qid': '1750215335',
                'sprefix': ',aps,684',
                'xpid': 'hJ_UAgzryP64t',
                'ref': 'sr_pg_4',
            }

            headers2 = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'zh-CN,zh-TW;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6',
                'cache-control': 'no-cache',
                'device-memory': '8',
                'downlink': '10',
                'dpr': '1',
                'ect': '4g',
                'pragma': 'no-cache',
                'priority': 'u=0, i',
                'referer': 'https://www.amazon.com/s?k=lacoste&crid=2VCQZIVW21HB9&sprefix=lacoste%2Caps%2C673&ref=nb_sb_noss_1',
                'rtt': '200',
                'sec-ch-device-memory': '8',
                'sec-ch-dpr': '1',
                'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-ch-ua-platform-version': '"10.0.0"',
                'sec-ch-viewport-width': '1920',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
                'viewport-width': '1920',
            }

            retry_count = 0
            while True:
                try:
                    
                    response = await current_session.post(url, params=params, headers=headers,json=json_data)
                    text = response.text
                    # asins_test = re.findall(r'data-asin="(.*?)"', text)
                    asins_test = re.findall(r'"asin" : "(.*?)"', text)
                    if is_captcha_page(text) or len(asins_test) == 0:
                        logger.error(f"遇到验证码页面,重置session并重试 page={page}")
                        current_session = await get_session()
                        if is_captcha_page(text) == False and len(asins_test) == 0:
                            logger.info(f"没有找到商品URL,当前页面返回：{text}")
                        elif is_captcha_page(text) == True:
                            logger.error(f"遇到验证码页面,重置session并重试 page={page}")

                        # 重置session - 关闭旧session并创建新session
                        await current_session.close()
                        current_session = await get_session()

                        retry_count += 1
                        if retry_count > 5:
                            logger.error("连续多次验证码,放弃本页")
                            # 返回收集到的商品和最新的session
                            return list(collected_asins)[:target_count], current_session
                        continue
                    # get请求
                    # response2 = await current_session.get(url2, params=params2, headers=headers2)
                    # text2 = response2.text
                    # asins_test = re.findall(r'data-asin="(.*?)"', text2)
                    # asins_test = re.findall(r'"asin" : "(.*?)"', text2)
                    # if is_captcha_page(text2) or len(asins_test) == 0:
                    #     logger.error(f"遇到验证码页面,重置session并重试 page={page}")
                    #     current_session = await get_session()
                    #     if is_captcha_page(text) == False and len(asins_test) == 0:
                    #         logger.info(f"没有找到商品URL,当前页面返回：{text}")
                    #     elif is_captcha_page(text) == True:
                    #         logger.error(f"遇到验证码页面,重置session并重试 page={page}")

                    #     # 重置session - 关闭旧session并创建新session
                    #     await current_session.close()
                    #     current_session = await get_session()

                    #     retry_count += 1
                    #     if retry_count > 5:
                    #         logger.error("连续多次验证码,放弃本页")
                    #         # 返回收集到的商品和最新的session
                    #         return list(collected_asins)[:target_count], current_session
                    #     continue
                    # 正常页面
                    break
                except Exception as e:
                    logger.error(f"aiohttp请求异常: {e}")
                    continue
            # 提取ASIN并直接添加到set中
            # asins = re.findall(r'data-asin="(.*?)"', text)
            asins = re.findall(r'"asin" : "(.*?)"', text)
            asins2 = re.findall(r'data-asin="(.*?)"', text2)
            current_size = len(collected_asins)
            collected_asins.update(filter(None, asins))
            collected_asins.update(filter(None, asins2))
            if len(collected_asins) == current_size:
                try_again += 1
                logger.info(f"现在的重试次数 {try_again}")
                current_session = await get_session()
                if try_again > 5:
                    logger.info(f"检测到没有新的商品URL,可能已到达最后一页")
                    break
            else:
                try_again = 0
            page += 1

        except Exception as e:
            logger.error(f"获取商品列表页面出错: {e}")
            continue
    if len(collected_asins) < target_count:
        logger.warning(f"\n警告：只收集到 {len(collected_asins)} 个商品URL,少于目标数量 {target_count}")
    # 返回收集到的商品和最新的session
    return list(collected_asins)[:target_count], current_session


async def get_detail_async(asin, session, proxies=None):
    """异步获取产品详情,遇到反爬虫自动重置当前session并返回新session和结果"""

    params = {
        'th': '1',
        'psc': '1',
    }

    max_retries = 3

    # 使用传入的session
    current_session = session

    for attempt in range(max_retries):
        try:
            response = await current_session.get(
                f'https://www.amazon.com/dp/{asin}',
                params=params,
            )
            res = response.text
            if is_captcha_page(res):
                logger.error(f"获取商品 {asin} 详情时遇到验证码页面,重置当前session并重试")
                current_session = await get_session()
                response = await current_session.get(
                    f'https://www.amazon.com/dp/{asin}',
                    params=params,
                )
                res = response.text
                if is_captcha_page(res):
                    logger.error(f"再次尝试获取 {asin} 详情时遇到验证码页面,依然报错")
                    current_session = await get_session()
                    continue
                result_list = await process_detail_data(asin, res, current_session)
                # 返回处理结果和当前使用的session
                return result_list, current_session
            result_list = await process_detail_data(asin, res, current_session)
            if not result_list:
                logger.error(f"获取商品 {asin} 详情时没有找到数据 却没有遇到验证码")
                logger.info(f"res : {res}")
                current_session = await get_session()
                continue
            # 返回处理结果和当前使用的session
            return result_list, current_session
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to get details for ASIN {asin} after {max_retries} attempts: {str(e)}")
            logger.info(f"asin : {asin} 正在重试 第{attempt}次 attempts: {str(e)}")
    return [], current_session


async def get_price(asin, current_session):
    for retry in range(3):  # 最多重试3次
        try:
            response = await current_session.get(
                f'https://www.amazon.com/dp/{asin}',
            )
            res = response.text
            await asyncio.sleep(1)
            if is_captcha_page(res):
                logger.error(f"获取商品 {asin} 价格时遇到验证码页面,重置当前session并重试")
                current_session = await get_session()
                continue

            match = re.findall(r'"priceAmount":(.*?),', res)
            if match:
                return True, match[0], current_session
            else:
                logger.error(f"获取商品 {asin} 价格时没有找到价格")
            return False, 0.00, current_session
        except Exception as e:
            logger.error(f"获取价格失败 {asin}: {e}")
    return False, 0.00, current_session


async def get_prices_concurrent(ordered_asins, current_session):
    """并发获取价格"""
    # 创建任务队列
    queue = Queue()
    sku_prices = {}
    # 将所有ASIN加入队列
    for asin in ordered_asins:
        await queue.put(asin)

    async def price_worker(current_session):
        """价格获取工作协程"""

        try:
            while True:
                try:
                    # 从队列获取任务
                    asin = await queue.get()
                    if asin is None:  # 结束信号
                        break
                    await asyncio.sleep(1)
                    # 获取价格
                    have_price, price, current_session = await get_price(asin, current_session=current_session)
                    if have_price:
                        sku_prices[asin] = price

                except Exception as e:
                    logger.error(f"获取价格出错 {asin}: {str(e)}")
                finally:
                    queue.task_done()
        finally:
            pass

    # 创建10个工作协程
    workers = []
    for _ in range(10):
        workers.append(asyncio.create_task(price_worker(current_session)))

    # 等待所有任务完成
    await queue.join()

    # 发送结束信号
    for _ in workers:
        await queue.put(None)

    # 等待所有工作协程结束
    await asyncio.gather(*workers)

    return sku_prices


async def process_detail_data(asin, res, current_session):
    """处理产品详情数据"""
    try:
        res = res.replace("\n", "").replace("\r", "")  # 去除所有换行符和回车符

        # 提取sku顺序
        dimensions_match = re.findall(r'"dimensions" : \[([^\]]+)\]', res)
        dimensions = ""
        dimensions_flag = 0
        try:
            dimensions = dimensions_match[0]
        except Exception as e:
            dimensions_flag = 1

        if dimensions_flag == 0:
            dimensions = dimensions.replace('"', '')
            dimensions = dimensions.split(',')

        dimension_values_match = re.findall(r'"dimensionValuesDisplayData" : {(.*?)},', res, re.S)
        ordered_asins = []
        sku_list = []
        if dimension_values_match:
            raw_data = dimension_values_match[0]
            asin_data = {}

            # Extract all ASIN entries using regex
            asin_entries = re.findall(r'"([^"]+)"\s*:\s*\[([^\]]+)\]', raw_data)
            # 获取所有ASIN作为ordered_asins
            dimensionToAsinMap = re.findall(r'"dimensionToAsinMap" : {(.*?)},', res, re.S)
            ordered_asins = []
            # 这一部分的asin一定有价格
            if dimensionToAsinMap:
                raw_map = dimensionToAsinMap[0]
                # Extract ASINs in order
                asin_matches = re.findall(r'".*?"\s*:\s*"([^"]+)"', raw_map)
                ordered_asins = asin_matches
            # 这一部分的asin可能没价格
            else:
                logger.info(f"没有找到dimensionToAsinMap , 使用asin_entries内容代替")
                ordered_asins = [entry[0] for entry in asin_entries]

            color_size = [entry[1] for entry in asin_entries]
            # logger.info(f"color_size : {color_size}")
            if len(ordered_asins) > 0:
                # # 多线程获取每个SKU的价格
                # sku_prices = await get_prices_concurrent(ordered_asins, current_session)

                # # 如果没有获取到任何价格,返回空列表
                # if not sku_prices:
                #     logger.error(f"没有获取到任何价格信息: {asin}")
                #     return []

                # 单sku价格替代所有
                try:
                    sku_prices = {}
                    match = re.findall(r'"priceAmount":(.*?),', res)
                    if match:
                        Variant_Compare_At_Price = match[0]
                        for sku_asin in ordered_asins:
                            sku_prices[sku_asin] = Variant_Compare_At_Price
                    else:
                        # 第二种匹配
                        match_2 = re.findall(r'<input type="hidden" name="priceValue" value="(.*?)" id="priceValue"/>', res)
                        if match_2:
                            Variant_Compare_At_Price = match_2[0]
                            for sku_asin in ordered_asins:
                                sku_prices[sku_asin] = Variant_Compare_At_Price
                        else:
                            return []
                except Exception as e:
                    # 没有价格
                    logger.info(f"Error processing data: {e}")
                    return []
            else:
                logger.error(f"asin列表为空: {asin}")

            # 处理SKU数据
            for asin, values_str in asin_entries:
                values = values_str.split(',')
                values = [i_v.replace('"', '') for i_v in values]
                if not sku_prices.get(asin):
                    continue
                asin_data[asin] = {
                    'Variant_SKU': asin,
                    'Variant_Price': sku_prices.get(asin, 0.00),
                    'Variant_Compare_At_Price': sku_prices.get(asin, 0.00)
                }
                if dimensions_flag == 0:
                    for idx, d_name in enumerate(dimensions):
                        # logger.info(f"asin : {asin} idx : {idx} d_name : {d_name}")
                        if d_name == "color_name":
                            asin_data[asin]['Color'] = values[idx].strip()
                        elif d_name == "size_name":
                            asin_data[asin]['Size'] = values[idx].strip()

            # Create sku_list in the correct order
            for asin in ordered_asins:
                if asin in sku_prices:  # 只添加有价格的SKU
                    sku_list.append(asin_data[asin])
        else:
            ordered_asins.append(asin)
            logger.info(f"只有单个sku asin : {asin}")
            asin_data = {}
            try:
                sku_prices = {}
                match = re.findall(r'"priceAmount":(.*?),', res)
                if match:
                    Variant_Compare_At_Price = match[0]
                    for sku_asin in ordered_asins:
                        sku_prices[sku_asin] = Variant_Compare_At_Price
                else:
                    # 第二种匹配
                    match_2 = re.findall(r'<input type="hidden" name="priceValue" value="(.*?)" id="priceValue"/>', res)
                    if match_2:
                        Variant_Compare_At_Price = match_2[0]
                        for sku_asin in ordered_asins:
                            sku_prices[sku_asin] = Variant_Compare_At_Price
                    else:
                        return []
            except Exception as e:
                # 没有价格
                logger.info(f"Error processing data: {e}")
                return []
            
            asin_data[asin] = {
                'Variant_SKU': asin,
                'Variant_Price': sku_prices.get(asin, 0.00),
                'Variant_Compare_At_Price': sku_prices.get(asin, 0.00)
            }
            
            sku_list.append(asin_data[asin])
            
        title_match = re.search(r'<span id="productTitle" .*?>(.*?)</span>', res)
        Title = title_match.group(1).strip() if title_match else ""
        # 转义HTML实体字符
        Title = html.unescape(Title)

        # 提取handle
        handle_match = re.findall(r'<link rel="canonical" href="https://www.amazon.com/(.*?)/dp/(.*?)"/>', res)
        handle = '-'.join(handle_match[0]) if handle_match else ""
        # Add Title and Handle to all sku_list items
        for sku in sku_list:
            sku['Title'] = Title
            sku['Handle'] = f"{handle}"
            
        # 使用多种选择器尝试获取商品描述
        soup = BeautifulSoup(res, 'html.parser')
        Body = ""
        
        # 尝试多种选择器匹配产品描述
        selectors = [
            # 原始选择器
            'div.a-expander-collapsed-height.a-row.a-expander-container.a-spacing-medium.a-expander-partial-collapse-container',
            # 新页面结构中的选择器
            'div#productFactsDesktopExpander',
            # 更通用的选择器
            'div[data-a-expander-name="productFactsDesktopExpander"]',
            # 尝试直接获取产品详情部分
            'div.a-section.a-spacing-medium.a-spacing-top-small',
            # 尝试获取产品描述部分
            'div#productDescription',
            # 尝试获取特性列表
            'div#feature-bullets'
        ]
        
        # 依次尝试不同的选择器
        for selector in selectors:
            body_div = soup.select_one(selector)
            if body_div:
                Body = str(body_div)
                # logger.info(f"成功使用选择器 '{selector}' 获取商品描述")
                break
                
        if not Body:
            # 如果上面的选择器都失败了，尝试直接查找包含"About this item"的部分
            about_headers = soup.find_all('h3', string=lambda s: s and 'About this item' in s)
            if about_headers:
                # 找到"About this item"标题后的列表
                for header in about_headers:
                    parent = header.parent
                    if parent:
                        # 查找父元素中的列表
                        item_list = parent.find('ul')
                        if item_list:
                            Body = str(parent)
                            logger.info(f"通过'About this item'标题找到商品描述")
                            break
        
        if not Body:
            logger.warning(f"没有找到商品描述 asin: {asin}")
            
        # Add Body only to first sku_list item
        if sku_list:
            sku_list[0]['Body'] = Body

        color_images_matches = re.findall(r'"hiRes":\s*"(https://m\.media-amazon\.com.*?\.jpg)"', res)
        Image_Src = color_images_matches if color_images_matches else []

        # Add Image_Src to sku_list items in order
        for i, sku in enumerate(sku_list):
            if i < len(Image_Src):
                sku['Image_Src'] = Image_Src[i]

        # # 获取所有价格并找出最大值作为Variant Compare At Price
        # try:
        #     match = re.findall(r'"priceAmount":(.*?),', res)
        #     if match:
        #         Variant_Compare_At_Price = match[0]
        #     else:
        #         logger.info(f"Error processing data: {e}")
        #         return []
        # except Exception as e:
        #     # 没有价格
        #     logger.info(f"Error processing data: {e}")
        #     return []

        # 构建数据行
        result_list = []

        # 为每个SKU创建一行数据
        for i, sku in enumerate(sku_list, 1):
            row = {
                'Handle': sku.get('Handle', ''),
                'Title': sku.get('Title', ''),
                'Body_HTML': sku.get('Body', '') if i == 1 else '',  # 只在第一个变体显示Body
                'Vendor': '',
                'Type': '',
                'Tags': '',
                'Option1_Name': 'SIZE',
                'Option1_Value': sku.get('Size', 'default'),
                'Option2_Name': 'COLOUR',
                'Option2_Value': sku.get('Color', 'default'),
                'Variant_SKU': sku.get('Variant_SKU', ''),
                'Variant_Price': sku.get('Variant_Price', 0.00),
                'Variant_Compare_At_Price': sku.get('Variant_Compare_At_Price', 0.00),
                'Image_Src': sku.get('Image_Src', ''),
                'Image_Position': i,
                'Status': 'active'
            }
            result_list.append(row)

        if not result_list:
            logger.info(f"没有找到商品详情 asin : {asin} 标题 : {Title}")

        return result_list

    except Exception as e:
        logger.info(f"Error processing data: {e}")
        return []


def save_to_excel(data_list, excel_path):
    """将数据列表保存到Excel文件"""
    try:
        # 检查数据列表是否为空
        if not data_list:
            logger.info("没有数据需要保存")
            return

        # 创建DataFrame
        df = pd.DataFrame(data_list)

        # 处理重复的Handle名称
        handle_counts = {}
        for i, row in df.iterrows():
            handle = row['Handle']
            if handle in handle_counts:
                handle_counts[handle] += 1
                # 添加后缀以区分重复的Handle
                df.at[i, 'Handle'] = f"{handle}-{handle_counts[handle]}"
            else:
                handle_counts[handle] = 0

        # 确保目录存在
        os.makedirs(os.path.dirname(excel_path), exist_ok=True)

        # 检查文件是否存在
        if os.path.exists(excel_path):
            # 读取现有Excel文件
            existing_df = pd.read_excel(excel_path)

            # 检查与现有数据中的Handle重复情况
            existing_handles = existing_df['Handle'].tolist()
            for i, row in df.iterrows():
                handle = row['Handle']
                suffix = 1
                original_handle = handle
                # 当Handle在现有数据中存在时,添加递增后缀
                while handle in existing_handles:
                    handle = f"{original_handle}-{suffix}"
                    suffix += 1
                if handle != original_handle:
                    df.at[i, 'Handle'] = handle

            # 合并新旧数据
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            # 将合并后的DataFrame写入Excel文件
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                combined_df.to_excel(writer, index=False)
                # 获取xlsxwriter工作簿和工作表对象
                workbook = writer.book
                # 设置默认字体为支持UTF-8的字体
                workbook.set_properties({
                    'title': 'Amazon Products',
                    'author': 'Amazon Crawler',
                    'comments': 'Created with Python and XlsxWriter'
                })
        else:
            # 如果文件不存在,直接写入
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
                # 获取xlsxwriter工作簿和工作表对象
                workbook = writer.book
                # 设置默认字体为支持UTF-8的字体
                workbook.set_properties({
                    'title': 'Amazon Products',
                    'author': 'Amazon Crawler',
                    'comments': 'Created with Python and XlsxWriter'
                })

        logger.info(f"Excel文件已保存至: {excel_path}")
    except Exception as e:
        logger.info(f"保存Excel文件时出错: {str(e)}")



async def get_session():
    if sys.platform == 'win32':
        proxies = {
            "http": 'http://127.0.0.1:7890',
            "https": 'http://127.0.0.1:7890',
        }
    else:
        # 生产环境代理配置 (从环境变量读取)
        proxyAddr = os.getenv("PROXY_ADDR", "")
        authKey = os.getenv("PROXY_AUTH_KEY", "")
        password = os.getenv("PROXY_PASSWORD", "")
        save_time = os.getenv("PROXY_SAVE_TIME", "")
        proxyUrl = "http://%(user)s:%(password)s:T%(save_time)s@%(server)s" % {
            "user": authKey,
            "password": password,
            "save_time": save_time,
            "server": proxyAddr,
        }
        proxies = {
            "http": proxyUrl,
            "https": proxyUrl,
        }
    return await renew_session(proxies)


async def process_asin_task(asin, session, pool):
    """处理单个ASIN的任务,包括获取详情、保存数据和session管理"""
    result, new_session = await get_detail_async(asin, session)
    flag = 0
    if result:
        # 保存到MySQL
        # for product_data in result:
        #     get_res = await save_to_mysql(pool, product_data)
        #     if not get_res:
        #         flag = 1
        # if flag == 1:
        #     logger.error(f"保存到MySQL有失败的情况 asin : {asin}")
        # else:
        #     logger.info(f"保存到MySQL成功 asin : {asin}")
        return True, new_session
    return False, new_session


class AmazonProcessor:
    def __init__(self, redis_url: str, mysql_config: dict):
        self.redis_url = redis_url
        self.mysql_config = mysql_config
        self.redis: Optional[Redis] = None
        self.mysql_pool: Optional[aiomysql.Pool] = None
        self.stop_processing = False
        self.task_id = str(uuid.uuid4())  # 生成唯一任务ID
        self.total_items = 0  # 总任务数
        self.completed_items = 0  # 已完成任务数
        self.update_progress = None  # 进度更新回调函数
        
    async def initialize(self):
        """初始化Redis和MySQL连接"""
        # self.redis = Redis.from_url(self.redis_url)
        # self.mysql_pool = await aiomysql.create_pool(**self.mysql_config)
        pass

    async def process_single_asin(self):
        """处理单个ASIN的协程"""
        session = await get_session()
        try:
            while not self.stop_processing:
                try:
                    # 从Redis中获取并删除一个ASIN,设置超时时间
                    asin = await asyncio.wait_for(self.redis.spop("amazon"), timeout=5.0)
                    if not asin:
                        logger.info("没有更多的ASIN需要处理")
                        break

                    # 将bytes转换为UTF-8字符串
                    if isinstance(asin, bytes):
                        asin = asin.decode('utf-8')

                    # 处理ASIN,设置超时时间
                    try:
                        success, new_session = await asyncio.wait_for(
                            process_asin_task(asin, session, self.mysql_pool),
                            timeout=420
                        )

                        # 更新进度
                        self.completed_items += 1
                        if self.update_progress:
                            await self.update_progress(self.completed_items)

                        # 如果session发生变化,关闭旧session并使用新session
                        if new_session != session:
                            await session.close()
                            session = new_session

                    except asyncio.TimeoutError:
                        logger.error(f"处理ASIN超时: {asin}")
                        # 将ASIN放回Redis
                        await self.redis.sadd("amazon", asin)
                        continue

                except asyncio.TimeoutError:
                    logger.error("从Redis获取ASIN超时")
                    continue

                except Exception as e:
                    logger.error(f"处理ASIN时发生错误: {str(e)}")
                    try:
                        session = await get_session()
                    except Exception as se:
                        logger.error(f"创建新session失败: {str(se)}")
                        continue

        except Exception as e:
            logger.error(f"协程发生未预期的错误: {str(e)}")
        finally:
            if session:
                await session.close()

    async def process_all_asins(self, concurrency: int = 10):
        """使用多个协程并发处理所有ASIN"""
        try:
            # 初始化连接
            await self.initialize()

            # 检查是否有数据需要处理
            if not await self.redis.exists("amazon"):
                logger.warning("Redis中没有找到amazon key")
                return

            # 创建多个协程
            tasks = []
            for _ in range(concurrency):
                task = asyncio.create_task(self.process_single_asin())
                tasks.append(task)

            # 等待所有协程完成
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=3600)
            except asyncio.TimeoutError:
                logger.error("处理超时,准备停止所有任务")
                self.stop_processing = True
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"处理过程中发生错误: {str(e)}")
        finally:
            # await self.cleanup()
            pass

    async def export_data(self, excel_path: str):
        """导出数据到CSV文件"""
        try:
            await export_mysql_to_excel(self.mysql_pool, excel_path, None)
            logger.info(f"数据已成功导出到: {excel_path}")
            return True
        except Exception as e:
            logger.error(f"导出数据失败: {str(e)}")
            return False


async def export_with_retry(pool, excel_path, limit, current_session_asins):
    """尝试导出数据,最多重试3次"""
    for attempt in range(3):
        try:
            await export_mysql_to_excel(pool, excel_path, limit)
            logger.info(f"成功导出数据到: {excel_path}")
            return True
        except Exception as e:
            logger.error(f"导出失败 (尝试 {attempt + 1}/3): {str(e)}")
            if attempt == 2:  # 最后一次尝试也失败
                # 保存current_session_asins到文件
                backup_file = os.path.join(current_dir+"/fail/", f'failed_asins_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
                try:
                    with open(backup_file, 'w') as f:
                        f.write('\n'.join(current_session_asins))
                    logger.info(f"已保存未导出的ASIN到: {backup_file}")
                except Exception as save_err:
                    logger.error(f"保存ASIN失败: {str(save_err)}")
            await asyncio.sleep(2)  # 等待2秒后重试
    return False


async def main():
    try:
        # # 创建MySQL连接池 (暂时注释)
        # pool = await aiomysql.create_pool(
        #     host=MYSQL_CONFIG['host'],
        #     user=MYSQL_CONFIG['user'],
        #     password=MYSQL_CONFIG['password'],
        #     db=MYSQL_CONFIG['db'],
        #     port=MYSQL_CONFIG['port'],
        #     autocommit=True,
        #     connect_timeout=7200,
        #     pool_recycle=1800,
        #     maxsize=15,
        #     minsize=5,
        #     client_flag=CLIENT.MULTI_STATEMENTS
        # )

        try:
            # # 创建Redis连接 (暂时注释)
            # redis = Redis.from_url(REDIS_URL)
            # # 清空Redis中的amazon键
            # await redis.delete("amazon")
            # logger.info("已清空Redis中的amazon键")
            total_start_time = time.time()
            base_target_count = 20  # 实际需要的数据量

            crawl_multiplier = 1.3  # 爬取倍数
            if base_target_count <= 30:
                crawl_multiplier = 2.5
            logger.info(f"实际需要的数据量: {base_target_count} 爬取倍数: {crawl_multiplier}")
            target_product_count = int(base_target_count * crawl_multiplier)
            # keyword = "licuadora"
            url = "https://www.amazon.com/s?k=lacoste&crid=2VCQZIVW21HB9&sprefix=lacoste%2Caps%2C673&ref=nb_sb_noss_1"
            # 解析URL并提取查询参数
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            # 提取k参数（关键词）
            keyword = query_params.get('k', [None])[0]
            print(f"关键词: {keyword}")

            logger.info("第一阶段：收集商品URL")
            # 为商品列表页创建单独的session
            list_session = await get_session()
            # 修改get_product_urls函数调用,传递session并获取更新后的session
            collected_asins, list_session = await get_product_urls(keyword, target_product_count, list_session)
            # 关闭列表页session
            # await list_session.close()

            # # 将collected_asins写入redis (暂时注释)
            # if collected_asins:
            #     # 确保ASIN是字符串格式
            #     asins_to_add = [str(asin) for asin in collected_asins]
            #     # 使用sadd添加多个ASIN
            #     await redis.sadd("amazon", *asins_to_add)
            #     logger.info(f"成功将 {len(collected_asins)} 个ASIN写入Redis")
            # else:
            #     logger.warning("没有收集到ASIN")

            logger.info(f"已收集到 {len(collected_asins)} 个商品URL")

            logger.info("\n第二阶段：并发获取商品详情")

            # # 创建处理器实例 (暂时注释Redis/MySQL处理)
            # processor = AmazonProcessor(REDIS_URL, MYSQL_CONFIG)
            # # 设置并发数
            # concurrency = 5
            # # 开始处理
            # logger.info(f"开始处理Amazon数据,并发数: {concurrency}")
            # await processor.process_all_asins(concurrency)
            # logger.info("处理完成")

            # 直接处理收集到的ASIN (不使用Redis队列)
            concurrency = 5
            logger.info(f"开始处理Amazon数据,并发数: {concurrency}, 共 {len(collected_asins)} 个ASIN")
            session = await get_session()
            for asin in collected_asins:
                try:
                    success, session = await process_asin_task(asin, session, None)
                    if success:
                        logger.info(f"处理ASIN成功: {asin}")
                    else:
                        logger.warning(f"处理ASIN失败: {asin}")
                except Exception as e:
                    logger.error(f"处理ASIN出错: {asin}, 错误: {e}")
            logger.info("处理完成")

            logger.info(f"\n第三阶段：导出数据")
            if sys.platform == 'win32':
                excel_path = os.path.join(current_dir, f'amazon_products_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
            else:

                excel_path = os.path.join(current_dir+"/xlsx/", f'amazon_products_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
            # # 尝试导出数据 (暂时注释MySQL导出)
            # export_success = await processor.export_data(excel_path)
            # if export_success:
            #     logger.info("数据导出成功")
            # else:
            #     logger.error("数据导出失败,已保存ASIN列表作为备份")
            logger.info(f"数据导出功能已禁用 (MySQL未连接), 文件路径: {excel_path}")

            total_end_time = time.time()
            total_elapsed_time = total_end_time - total_start_time
            logger.info(f"\n程序总耗时: {total_elapsed_time:.2f} 秒 ({timedelta(seconds=int(total_elapsed_time))})")
            logger.info(f"实际获取商品数: {len(current_session_asins)}")

        finally:
            pass
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

