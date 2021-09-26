#!pip install feedparser
#!pip install --upgrade gupload
import os
import glob
import html
import json
import requests
from bs4 import BeautifulSoup
import feedparser
import re
import pandas as pd
from datetime import datetime

# Define Feeds in the form of a dict entry: media name, news category, rss url
feeds = [{'source':'la_nacion','category':'politica','url':'http://contenidos.lanacion.com.ar/herramientas/rss/categoria_id=30'},\
         {'source':'la_nacion','category':'mundo','url':' http://contenidos.lanacion.com.ar/herramientas/rss/categoria_id=7'},\
         {'source':'la_nacion','category':'economia','url':'http://contenidos.lanacion.com.ar/herramientas/rss/categoria_id=272'},\
         {'source':'la_nacion','category':'sociedad','url':'http://contenidos.lanacion.com.ar/herramientas/rss/categoria_id=7773'},\
         {'source':'la_nacion','category':'opinion','url':'http://contenidos.lanacion.com.ar/herramientas/rss/categoria_id=28'},\
         {'source':'la_nacion','category':'portada','url':' http://contenidos.lanacion.com.ar/herramientas/rss/origen=1'},\
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
         {'source':'clarin','category':'opinion','url':'	https://www.clarin.com/rss/opinion/'},\
        # \
         #{'source':'ambito','category':'politica','url':'https://www.ambito.com/rss/politica.xml'},\
         #{'source':'ambito','category':'mundo','url':'https://www.ambito.com/rss/mundo.xml'},\
         #{'source':'ambito','category':'economia','url':'https://www.ambito.com/rss/economia.xml'},\
         #{'source':'ambito','category':'sociedad','url':'https://www.ambito.com/rss/informacion-general.xml'},\
         #{'source':'ambito','category':'opinion','url':'	https://www.ambito.com/rss/opinion.xml'}\
        # \
         #{'source':'perfil','category':'politica','url':'https://www.perfil.com/feed/politica'},\
         #{'source':'perfil','category':'mundo','url':'https://www.perfil.com/feed/internacionales'},\
         #{'source':'perfil','category':'economia','url':'https://www.perfil.com/feed/economia'},\
         #{'source':'perfil','category':'sociedad','url':'https://www.perfil.com/feed/sociedad'},\
         #{'source':'perfil','category':'opinion','url':'https://www.perfil.com/feed/opinion'}
         ]

# Initialize destination DataFrame
df = pd.DataFrame(columns=['source','category','date','title','text','link'])
df = pd.read_csv('/home/ec2-user/news-base/news.csv',usecols=['source','category','date','title','text','link'])
print("CSV Loaded")

# Define Functions
def remove_html_tags(text):
    """Remove html tags from a string"""
    clean = re.compile('<.*?>')
    clean_text = re.sub(clean, '', text)
    unescaped_text = html.unescape(clean_text)
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

  else:
    for entry in data.entries:
      new_row = {'source':rss['source'],'category':rss['category'],'date':entry.published, 'title':entry.title, 'text':remove_html_tags(entry.content[0].value),'link':entry.link}
      df = df.append(new_row, ignore_index=True)

# Iterate RSS feeds list and append destination DF
print("Pulling from every RSS")
for feed in feeds:
  get_news(feed)

# Retrieve previous dataset and append new results
ant = pd.read_csv('/home/ec2-user/news-base/news.csv',usecols=['source','category','date','title','text','link'])
compl = ant.append(df, ignore_index=True)
# Sanitize duplicate rows taking url as key
compl.drop_duplicates(subset='link', keep="first",inplace=True)

# Store previous 'news_' + datetime.today().strftime('%Y-%m-%d'+'.csv')
bk_filename = '/home/ec2-user/news-base/news_' + datetime.today().strftime('%Y-%m-%d'+'.csv')
filename = '/home/ec2-user/news-base/news.csv'
# Check if backup already exists
files_present = glob.glob(bk_filename)
# If not , backup previous CSV
if not files_present:
    print("Write daily backup")
    df.to_csv(bk_filename,index=False)
else:
    print("Backup already exists")
# Write new consolidated CSV
print("Write final CSV")
compl.to_csv(filename,index=False)
