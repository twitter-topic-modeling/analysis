# Text Classification on RSS News Feeds

![Architecture](https://raw.githubusercontent.com/twitter-topic-modeling/analysis/master/assets/architecture.png)

## Introduction

เนื่องจากปัจจุบันข้อมูลข่าวสารมีมากมาย แล้วถ้าเราต้องการจัดหมวดหมู่ข่าวสารเองอาจต้องใช้เวลานาน เราจึงได้ทำโปรเจคนี้ขึ้นมา เพื่อจัดหมวดหมู่ของข้อมูลข่าวสารให้โดยอัตโนมัติ เมื่อมีข่าวสารใหม่ ๆ เข้ามา

## Data Source

- ไทยรัฐ
- สนุกดอทคอม
- Thai PBS
- ประชาชาติ
- โลกวันนี้
- มติชน
- Voice TV

## Data

| key         | type     | description                                                                              |
| ----------- | -------- | ---------------------------------------------------------------------------------------- |
| url         | string   | ลิงค์ที่ระบุแหล่งในอินเตอร์เน็ต โดยให้ข้อมูลเกี่ยวกับตำแหน่งและที่อยู่ของเว็บไซต์หนึ่ง ๆ |
| source      | string   | สำนักข่าวที่ทำการเผยแพร่ข่าว                                                             |
| title       | string   | หัวข้อข่าว                                                                               |
| summary     | string   | เนื้อหาข่าว                                                                              |
| publishDate | datetime | วันที่เผยแพร่ข่าว                                                                        |

## Prerequisite

- Hadoop
- Kafka
- Spark, Mlib
- Logstash
- Elasticsearch
- Kibana

## Dataset

![TR-TPBS](https://github.com/nakhunchumpolsathien/TR-TPBS/)
