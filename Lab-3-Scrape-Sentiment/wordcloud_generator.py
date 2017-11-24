from __future__ import print_function
import cPickle as pickle
import GDELT_scrape
import urllib, hashlib
import pywren
import sys
import boto3, botocore
from wordcloud import WordCloud
import click
import GDELT_scrape
import os

S3BUCKET = ''

def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts]
             for i in range(wanted_parts) ]

def wordcloud(links):
    s3 = boto3.resource('s3')
    s3bucket = os.environ.get('S3BUCKET');
    try:
        for link in links:
            print(link)
            text = GDELT_scrape.scrape_content(link)
            wordcloud = WordCloud(width = 500, height = 300).generate(text)
            cloud = wordcloud.to_file('/tmp/cloud.jpg')
            s3.Object(s3bucket, 'tagclouds/' + hashlib.md5(link).hexdigest() + '.jpg').put(ACL='public-read',ContentType='image/jpeg',Body=open('/tmp/cloud.jpg', 'rb'))
        return 'Ok'
    except Exception as e:
        return e


@click.group()
def cli():
    pass

@cli.command('write')
@click.option('--bucket_name', help='bucket to save files in')
def create_wordcloud_pywren(bucket_name):
    S3BUCKET = bucket_name
    links = pickle.load(open('links.pickle', 'rb'))
    wrenexec = pywren.default_executor()
    futures = wrenexec.map(wordcloud, split_list(links, wanted_parts=100), invoke_pool_threads = 128, extra_env = {'S3BUCKET' : S3BUCKET})
    pywren.get_all_results(futures)

if __name__ == '__main__':
    cli()
