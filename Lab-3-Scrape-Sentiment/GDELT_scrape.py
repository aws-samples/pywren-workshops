from __future__ import print_function
from decimal import Decimal
import csv
import urllib, hashlib
from bs4 import BeautifulSoup
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
import zipfile
import boto3, botocore
import StringIO
import pywren
import os

S3BUCKET = 'pywren-workshop'

def news_analyzer(links):
    try:
        download_nltk_data()
        dynamo_tbl = boto3.resource('dynamodb', 'us-west-2').Table('pywren-workshop-gdelt-table')
        for link in links:
            text = scrape_content(link)
            sid = SentimentIntensityAnalyzer()
            sentiment = sid.polarity_scores(text)
            record = {}
            record['link'] = link
            record['sentiment'] = str(sentiment['compound'])
            words = []
            for word, frequency in get_frequent_words(text):
                words.append(word + ':' + str(frequency))
            record['words'] = words
            response = dynamo_tbl.put_item(Item=record)
        return 'Ok'
    except Exception as e:
        return e

def download_nltk_data():
    s3 = boto3.resource('s3')
    # insert IF check if NLTK data is available to speed up (container re-use)
    # FIX
    try:
        s3.Bucket('pywren-workshop').download_file('nltk_data.zip',
                                                   '/tmp/nltk_data.zip')
        zip_ref = zipfile.ZipFile('/tmp/nltk_data.zip', 'r')
        zip_ref.extractall('/tmp/condaruntime/')
        zip_ref.close()
    except botocore.exceptions.ClientError as e:
        return e

def scrape_content(link):
    try:
        html = urllib.urlopen(link).read()
        soup = BeautifulSoup(html, "html.parser")
        # kill all script and style elements
        for script in soup(["script", "style"]):
            script.extract()    # rip it out
        # get text
        text = soup.get_text()
        return text
    except IOError:
        return ''

def get_frequent_words(text):
    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)
    words = nltk.word_tokenize(text)
    # Remove single and double-character tokens (mostly punctuation)
    words = [word for word in words if len(word) > 2]
    # Remove numbers
    words = [word for word in words if not word.isnumeric()]
    # Lowercase all words (default_stopwords are lowercase too)
    words = [word.lower() for word in words]
    # Remove stopwords
    default_stopwords = set(nltk.corpus.stopwords.words('english'))
    words = [word for word in words if word not in default_stopwords]
    # Calculate frequency distribution
    fdist = nltk.FreqDist(words)
    return fdist.most_common(50)

def get_urls_from_gdelt_data(file):
    s3 = boto3.client('s3', 'us-east-1')
    try:
        s3_object = s3.get_object(Bucket='gdelt-open-data', Key='events/' + file)
        f = StringIO.StringIO(s3_object['Body'].read().decode('utf-8','replace').encode('ascii','replace'))
        fieldnames = ['GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate', 'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode', 'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code', 'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code', 'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode', 'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent', 'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass',
               'GoldsteinScale', 'NumMentions', 'NumSources', 'NumArticles', 'AvgTone', 'Actor1Geo_Type', 'Actor1Geo_FullName', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code', 'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor1Geo_FeatureID', 'Actor2Geo_Type', 'Actor2Geo_FullName', 'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code', 'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID', 'ActionGeo_Type', 'ActionGeo_FullName', 'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL']
        items = csv.DictReader(f, fieldnames, delimiter='\t')
        links = []
        for i, item in enumerate(items):
            links.append(item['SOURCEURL'])
        # remove duplicates in list of URLS
        links_without_duplicates = []
        for link in links:
            if link not in links_without_duplicates:
                links_without_duplicates.append(link)
        # limit ourselves to 1000 articles
        return links_without_duplicates[:1000]
    except botocore.exceptions.ClientError as e:
        return e
