from time import sleep
from datetime import datetime, timedelta
import traceback
import feedparser
import time

SEVEN_HOURS = timedelta(hours=7)
KAFKA_TOPIC = 'rss-raw-1'

sources = [
    {'url': 'https://www.thairath.co.th/rss/news', 'name': 'ไทยรัฐ'},
    {'url': 'http://rssfeeds.sanook.com/rss/feeds/sanook/news.index.xml', 'name': 'สนุกดอทคอม'},
    {'url': 'https://news.thaipbs.or.th/rss/news', 'name': 'thaipbs'},
    {'url': 'https://www.prachachat.net/feed', 'name': 'ประชาชาติ'},
    {'url': 'http://www.lokwannee.com/web2013/?cat=69&feed=rss2', 'name': 'โลกวันนี้'},
    {'url': 'https://www.matichon.co.th/feed', 'name': 'มติชน'},
    {'url': 'https://voicetv.co.th/rss', 'name': 'Voice TV'}
]

whitelist_keys = ['title', 'summary', 'published_parsed', 'source']

from kafka import KafkaProducer
import json
import pykafka

def publish_message(producer, json_send_data):
    try:
        producer.produce(bytes(json.dumps(json_send_data),'ascii'))
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    client = pykafka.KafkaClient("localhost:9092")
    producer = client.topics[bytes(KAFKA_TOPIC,'ascii')].get_producer()
    return producer

def send(producer, item):
    if(item is None):
        return
    to_json = {key: item[key] for key in whitelist_keys}
    if item['published_parsed'] is not None:
        to_json['pubDate'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', item['published_parsed'])
    else:
        to_json['pubDate'] = (datetime.now() - SEVEN_HOURS).strftime('%Y-%m-%dT%H:%M:%SZ')
    print(to_json['source'], to_json['title'], to_json['pubDate'])
    to_json['url'] = item['link']
    publish_message(producer, to_json)

def fetch(url):
    feed = feedparser.parse(url)
    newsList = feed.entries
    return newsList, newsList[0]['link']

def fetch_source(source):
    name = source['name']
    url = source['url']
    newsList, last_url = fetch(url)
    producer = connect_kafka_producer()
    # initial
    for item in newsList:
        item['source'] = name
        send(producer, item)

    while(True):
        try:
            sleep(5)
            newsList, new_url = fetch(url)
            is_updated = new_url != last_url
            if(is_updated):
                last_url = new_url
                item = newsList[0]
                item['source'] = name
                send(producer, item)
        except:
            traceback.print_exc()
            pass

from multiprocessing import Pool

with Pool(7) as p:
    p.map(fetch_source, sources)