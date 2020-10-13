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
