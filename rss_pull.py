import os
import glob
import html
import json
import requests
from bs4 import BeautifulSoup
import feedparser
import re
import pandas as pd
from datetime import datetime , timedelta

# Define Feeds in the form of a dict entry: media name, news category, rss url
feeds = [{'source':'la_nacion','category':'todas','url':'https://www.lanacion.com.ar/arcio/rss/'},\
         \
         {'source':'infobae','category':'todas','url':'https://www.infobae.com/argentina-rss.xml'},\
         \
         {'source':'pagina_12','category':'politica','url':'https://www.pagina12.com.ar/rss/secciones/el-pais/notas'},\
         {'source':'pagina_12','category':'mundo','url':'https://www.pagina12.com.ar/rss/secciones/el-mundo/notas'},\
         {'source':'pagina_12','category':'economia','url':'https://www.pagina12.com.ar/rss/secciones/economia/notas'},\
         {'source':'pagina_12','category':'sociedad','url':'https://www.pagina12.com.ar/rss/secciones/sociedad/notas'},\
         {'source':'pagina_12','category':'portada','url':'https://www.pagina12.com.ar/rss/portada'},\
         \
         {'source':'clarin','category':'politica','url':'https://www.clarin.com/rss/politica/'},\
         {'source':'clarin','category':'mundo','url':'https://www.clarin.com/rss/mundo/'},\
         {'source':'clarin','category':'economia','url':'https://www.clarin.com/rss/economia/'},\
         {'source':'clarin','category':'sociedad','url':'https://www.clarin.com/rss/sociedad/'},\
         {'source':'clarin','category':'opinion','url':'	https://www.clarin.com/rss/opinion/'}
         ]

# Initialize destination DataFrame
#df = pd.DataFrame(columns=['source','category','date','title','text','link'])
df = pd.read_csv('/home/ec2-user/news-base/news.csv',usecols=['source','category','date','title','text','link'])
print("CSV Loaded")

# Define Functions
def remove_html_tags(text):
    """Remove html tags from a string"""
    clean = re.compile('<.*?>')
    clean_text = re.sub(clean, '', text)
    unescaped_text = html.unescape(clean_text)
    unescaped_text = unescaped_text.replace("\n", " ")
    unescaped_text = unescaped_text.replace("\'", " ")
    return unescaped_text

def get_articlebody(link):
  """ Receives an html link containing a news article and returns
  a plain text string with the article body"""
  soup = BeautifulSoup(requests.get(link).content, "html.parser")
  json_data = json.loads(soup.find(type="application/ld+json").string)
  return(json_data['description'])

def get_news_tag(link):
  """ Receives an html link containing a news article and finds the "news-content" tag
  to retrieve a plain text string with the article body"""
  try:
    r = requests.get(link, headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')
    container = soup.find('div', class_='news-content')
    if container is None:
      return float('Nan')
    else:
      for para in container.find_all('p', recursive=False):
        return para.text
  except ConnectionResetError:
    return float('Nan')

def get_news(rss):
  """ Receives a dict entry containing media name, news category, rss url 
  and parses de RSS feed extracting news date, title and content 
  and append it to a destination pandas dataframe named df """
  global df
  data = feedparser.parse(rss['url'])
  if rss['source'] in ['clarin','ambito']:
    for entry in data.entries:
      new_row = {'source':rss['source'],'category':rss['category'],'date':entry.published, 'title':entry.title, 'text':remove_html_tags(get_articlebody(entry.link)),'link':entry.link}
      df = df.append(new_row, ignore_index=True)
  elif rss['source'] == 'perfil':
    for entry in data.entries:
      new_row = {'source':rss['source'],'category':rss['category'],'date':entry.published, 'title':entry.title, 'text':get_news_tag(entry.link),'link':entry.link}
      df = df.append(new_row, ignore_index=True)
  elif rss['source'] == 'la_nacion':
    for entry in data.entries:
      if "politica" in entry.link:
        new_row = {'source':rss['source'],'category':'politica','date':entry.published, 'title':entry.title, 'text':remove_html_tags(entry.content[0].value),'link':entry.link}
        df = df.append(new_row, ignore_index=True)
      elif "mundo" in entry.link:
        new_row = {'source':rss['source'],'category':'mundo','date':entry.published, 'title':entry.title, 'text':remove_html_tags(entry.content[0].value),'link':entry.link}
        df = df.append(new_row, ignore_index=True)
      elif "economia" in entry.link:
        new_row = {'source':rss['source'],'category':'economia','date':entry.published, 'title':entry.title, 'text':remove_html_tags(entry.content[0].value),'link':entry.link}
        df = df.append(new_row, ignore_index=True)
      elif "sociedad" in entry.link:
        new_row = {'source':rss['source'],'category':'sociedad','date':entry.published, 'title':entry.title, 'text':remove_html_tags(entry.content[0].value),'link':entry.link}
        df = df.append(new_row, ignore_index=True)
      elif "opinion" in entry.link:
        new_row = {'source':rss['source'],'category':'opinion','date':entry.published, 'title':entry.title, 'text':remove_html_tags(entry.content[0].value),'link':entry.link}
        df = df.append(new_row, ignore_index=True)
  elif rss['source'] == 'infobae':
    for entry in data.entries:
      if "politica" in entry.link:
        new_row = {'source':rss['source'],'category':'politica','date':entry.published, 'title':entry.title, 'text':remove_html_tags(entry.content[0].value),'link':entry.link}
        df = df.append(new_row, ignore_index=True)
      elif "america" in entry.link:
        new_row = {'source':rss['source'],'category':'mundo','date':entry.published, 'title':entry.title, 'text':remove_html_tags(entry.content[0].value),'link':entry.link}
        df = df.append(new_row, ignore_index=True)
      elif "economia" in entry.link:
        new_row = {'source':rss['source'],'category':'economia','date':entry.published, 'title':entry.title, 'text':remove_html_tags(entry.content[0].value),'link':entry.link}
        df = df.append(new_row, ignore_index=True)
      elif "sociedad" in entry.link:
        new_row = {'source':rss['source'],'category':'sociedad','date':entry.published, 'title':entry.title, 'text':remove_html_tags(entry.content[0].value),'link':entry.link}
        df = df.append(new_row, ignore_index=True)
  else:
    for entry in data.entries:
      new_row = {'source':rss['source'],'category':rss['category'],'date':entry.published, 'title':entry.title, 'text':remove_html_tags(entry.content[0].value),'link':entry.link}
      df = df.append(new_row, ignore_index=True)

# Iterate RSS feeds list and append destination DF
print("Pulling from every RSS")
for feed in feeds:
  get_news(feed)

# Retrieve previous dataset and append new results
news_path = '/home/ec2-user/news-base/news.csv'
ant = pd.read_csv(news_path,usecols=['source','category','date','title','text','link'])
compl = ant.append(df, ignore_index=True)

# Sanitize duplicate rows taking url as key
compl.drop_duplicates(subset='link', keep="first",inplace=True)
compl.text = compl.text.replace("\n", " ") 
compl['text'] = compl['text'].replace(r'\n',' ', regex=True) #temporary fix incomplete unescaping
compl['text'] = compl['text'].replace(r'\'',' ', regex=True) #temporary fix incomplete unescaping

# Make sure max lenght of articles is 40.000 characters
compl['text'] = compl.text.str[:40000]

# Store previous 'news_' + datetime.today().strftime('%Y-%m-%d'+'.csv')
#bk_filename = '/home/ec2-user/news-base/backups/news_' + (datetime.today() - timedelta(hours=3, minutes=00)).strftime('%Y-%m-%d %H:%M:%S'+'.csv')

# Check if backup already exists
#files_present = glob.glob(bk_filename)

# If not , backup previous CSV
#if not files_present:
#    print("Write backup")
#    df.to_csv(bk_filename,index=False)
#else:
#    print("Backup already exists")

# Write new consolidated CSV
print("Write final CSV")
compl.to_csv(news_path,index=False)
