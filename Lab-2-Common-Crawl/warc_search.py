import re, time, boto3, os
import requests
from warcio.archiveiterator import ArchiveIterator

def keyword_search(key):
    t1 = time.time()
    dynamo_tbl = boto3.resource('dynamodb').Table('pywren-workshop-common-crawl')
    search_array = os.environ.get('KEYWORDS').split(',');
    result = {}
    for search_str in search_array:
        result[search_str]={}
        result[search_str]['count']=0
        #result[search_str]['dates']=[]
    resp = requests.get('https://commoncrawl.s3.amazonaws.com/' + key, stream = True)
    for record in ArchiveIterator(resp.raw, arc2warc=True):
        if record.content_type == 'text/plain':
            webpage_text = record.content_stream().read()
            date = record.rec_headers.get_header('WARC-Date')
            for search_str in search_array:
                if re.search(search_str,webpage_text):
                    result[search_str]['count'] += 1
    for search_str in search_array:
        if result[search_str]['count'] > 0:
            record={}
            record['warc_file']=key
            record['search_str']=search_str
            record['occurrence']=result[search_str]['count']
            response=dynamo_tbl.put_item(Item=record)
    t2 = time.time()
    return t2-t1

def keyword_search_with_URL(key):
    search_array = os.environ.get('KEYWORDS').split(',');
    result = {}
    for search_str in search_array:
        result[search_str.strip()]=[]
    resp = requests.get('https://commoncrawl.s3.amazonaws.com/' + key, stream = True)
    for record in ArchiveIterator(resp.raw, arc2warc=True):
        if record.content_type == 'text/plain':
            webpage_text = record.content_stream().read()
            url = record.rec_headers.get_header('WARC-Target-URI')
            for search_str in search_array:
                if re.search(search_str,webpage_text):
                    result[search_str.strip()].append(url)
    return result
