from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('svm-app').getOrCreate()

df = spark.read.csv('/project/dataset.csv', header = True, inferSchema = True)

df = df.select('title', 'summary', 'labels')

df = df.sample(withReplacement=True, fraction=0.1, seed=100)

map_label = {
    'E-Sport': 5,
    'การเมือง': 1,
    'กีฬา': 5,
    'กีฬาอื่นๆ': 5,
    'ข่าว': None,
    'ข่าวบันเทิง': 8,
    'คนดังนั่งเขียน': None,
    'คอลัมน์': None,
    'ตรวจหวย': 2,
    'ต่างประเทศ': 3,
    'ทองคำ': 2,
    'ทันข่าวเด่น': None,
    'ทั่วไทย': 9,
    'ท่องเที่ยว': 2,
    'นักข่าวพลเมือง': None,
    'บันเทิง': 8,
    'บันเทิงต่างประเทศ': 8,
    'บ้าน': None,
    'ผู้หญิง': None,
    'พระราชสำนัก': 9,
    'ฟุตซอล': 5,
    'ฟุตบอลยุโรป': 5,
    'ฟุตบอลโลก': 5,
    'ฟุตบอลไทย': 5,
    'ภัยพิบัติ': 10,
    'ภูมิภาค': 9,
    'มวย/MMA': 5,
    'ยานยนต์': 7,
    'วอลเลย์บอล': 5,
    'วิทยาศาสตร์เทคโนโลยี': 7,
    'ศิลปะ-บันเทิง': 8,
    'สกู๊ปไทยรัฐ': None,
    'สังคม': 1,
    'สิ่งแวดล้อม': 10,
    'หนัง': 8,
    'หนังสือพิมพ์': None,
    'หน้าแรก': None,
    'หวย': 2,
    'อาชญากรรม': 4,
    'อาหาร': 11,
    'เลือกตั้ง': 1,
    'เศรษฐกิจ': 2,
    'เอเชียนเกมส์': 5,
    'ไทยพีบีเอส อินไซส์': None,
    'ไทยรัฐเชียร์ไทยแลนด์': None,
    'ไทยลีก': 5,
    'ไลฟ์': 12,
    'ไลฟ์สไตล์': 12,
    'ไอที': 7,
    'ไอ้โหด': 4
}

label_enum = {
    1: 'การเมือง',
    2: 'เศรษฐกิจ',
    3: 'ต่างประเทศ',
    4: 'อาชญากรรม',
    5: 'กีฬา',
    # 6: 'การศึกษา'
    7: 'เทคโนโลยี',
    8: 'บันเทิง',
    9: 'ในประเทศ',
    10: 'สิ่งแวดล้อม',
    11: 'อาหาร',
    12: 'ไลฟ์สไตล์'
}


import re
import string
from pythainlp import word_tokenize
from pythainlp.corpus import thai_stopwords

clean = re.compile('<.*?>|\s+|“|”')
table = str.maketrans(dict.fromkeys(string.punctuation))
stopwords = thai_stopwords()

def pre_process(title, summary):
    if(title is None):
        title = ''
    if(summary is None):
        summary = ''
        
    x = title + summary
    if(x is None):
        return []
    x = x.translate(table)
    # remove html tag
    x = re.sub(clean, '', x)
    
    tokens = word_tokenize(x)
    tokens = [token for token in tokens if token is not stopwords]
    return tokens

from collections import Counter
import numpy as np

def clean_labels(labels):
    if(labels is None):
        return None
    
    labels = labels.split(',')
    labels = [map_label[label] if label in map_label else None for label in labels ]
    labels = [label_enum[label] for label in labels if label is not None]
    
    if(len(labels) <= 0):
        return None
    
    # pick max freq label
    counter = Counter(labels)
    keys = list(counter.keys())
    values = list(counter.values())
    max_index = np.argmax(values)
    label = keys[max_index]
    
    return label

# input_rdd = df.rdd.map(lambda x: pre_process(x))

udf_pre_process = udf(pre_process, ArrayType(StringType()))
udf_clean_labels = udf(clean_labels, StringType())

df = df.withColumn("tokens", udf_pre_process("title", "summary"))

df = df.withColumn("label_pre", udf_clean_labels("labels"))

df = df.na.drop()

df.show()

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, CountVectorizer

countVectors = CountVectorizer(inputCol="tokens", outputCol="features", vocabSize=10000, minDF=5)

label_stringIdx = StringIndexer(inputCol = "label_pre", outputCol = "label")
indexer = label_stringIdx.fit(df)
df = indexer.transform(df)
meta = [f.metadata for f in df.schema.fields if f.name == "label"]
labels = meta[0]
print(labels)
#pipeline = Pipeline(stages=[countVectors])
# Fit the pipeline to training documents.
#pipelineFit = pipeline.fit(df)

#pipelineFit.save('/project/pipeline')
# dataset = pipelineFit.transform(df)

# (trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)


