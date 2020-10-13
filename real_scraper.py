from riko.modules import fetch, fetchsitefeed
from time import sleep

sources = [
    {'url': 'https://www.thairath.co.th/rss/news', 'name': 'ไทยรัฐ'},
    {'url': 'http://rssfeeds.sanook.com/rss/feeds/sanook/news.index.xml', 'name': 'สนุกดอทคอม'},
    {'url': 'https://news.thaipbs.or.th/rss/news', 'name': 'thaipbs'},
    {'url': 'https://www.prachachat.net/feed', 'name': 'ประชาชาติ'},
    {'url': 'http://www.lokwannee.com/web2013/?cat=69&feed=rss2', 'name': 'โลกวันนี้'},
    {'url': 'https://www.matichon.co.th/feed', 'name': 'มติชน'},
    {'url': 'https://voicetv.co.th/rss', 'name': 'Voice TV'}
]

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
    producer = client.topics[bytes('rss-test-1','ascii')].get_producer()
    return producer

import time

whitelist_keys = ['title', 'summary', 'pubDate']

def fetch_source(source):
    stream = fetch.pipe(conf=source)
    source = source['name']
    kafka_producer = connect_kafka_producer()
    
    while(True):
        print('fetching')
        try:
            item = next(stream)
            to_json = {key: item[key] for key in whitelist_keys}
            to_json['pubDate'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', item['pubDate'])
            to_json['source'] = source
            to_json['url'] = item['link']
            print(to_json['title'])
            publish_message(kafka_producer, to_json)
        except:
            sleep(5)
            pass

from multiprocessing import Pool

with Pool(5) as p:
    p.map(fetch_source, sources)