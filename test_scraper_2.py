from riko.modules import fetch
from time import clock_getres, sleep
from datetime import datetime, timedelta
import traceback
import feedparser
import hashlib

seven_hours = timedelta(hours=7)

sources = [
    {'url': 'https://www.thairath.co.th/rss/news', 'name': 'ไทยรัฐ'},
    # {'url': 'http://rssfeeds.sanook.com/rss/feeds/sanook/news.index.xml', 'name': 'สนุกดอทคอม'},
    # {'url': 'https://news.thaipbs.or.th/rss/news', 'name': 'thaipbs'},
    # {'url': 'https://www.prachachat.net/feed', 'name': 'ประชาชาติ'},
    # {'url': 'http://www.lokwannee.com/web2013/?cat=69&feed=rss2', 'name': 'โลกวันนี้'},
    # {'url': 'https://www.matichon.co.th/feed', 'name': 'มติชน'},
    # {'url': 'https://voicetv.co.th/rss', 'name': 'Voice TV'}
]

import time

whitelist_keys = ['title', 'summary', 'pubDate']

def fetch_source(source):
    name = source['name']
    # kafka_producer = connect_kafka_producer()
    url = source['url']
    feed = feedparser.parse(url)
    news_updated = feed.entries
    hash_object = hashlib.sha256(str(news_updated).encode('utf-8'))
    last_dig = hash_object.hexdigest()
    for item in feed.entries:
        print(item)

    # while(True):
    #     try:
    #         feed = feedparser.parse(url)
    #         news_updated = feed.entries
    #         hash_object = hashlib.sha256(str(news_updated).encode('utf-8'))
    #         hex_dig = hash_object.hexdigest()
    #         is_updated = last_dig is not hex_dig
    #         if(is_updated):
    #             item = next(stream, None)
    #             if(item is None):
    #                 # print('pass')
    #                 sleep(5)
    #                 continue
    #             print(item['title'])
    #             to_json = {key: item[key] for key in whitelist_keys}
    #             if item['pubDate'] is not None:
    #                 to_json['pubDate'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', item['pubDate'])
    #             else:
    #                 to_json['pubDate'] = (datetime.now() - seven_hours).strftime('%Y-%m-%dT%H:%M:%SZ')
    #             to_json['source'] = source
    #             to_json['url'] = item['link']
    #             # print(to_json['title'])
    #             # publish_message(kafka_producer, to_json)
    #     except:
    #         traceback.print_exc()
    #         sleep(5)
    #         pass

from multiprocessing import Pool

with Pool(5) as p:
    p.map(fetch_source, sources)