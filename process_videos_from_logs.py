import os
import sqlite3
import urllib2
import string
import nltk
import re
from datetime import date
from BeautifulSoup import BeautifulSoup
from stemming.porter2 import stem

proxy_log_vid_list = []
video_log_vid_list = []
new_video_list = []
exclude = set(string.punctuation) #set of punctuations
 
def is_ascii(s):
    try:
        s.decode('ascii')
        return True
    except UnicodeDecodeError:
        return False


def process_videos_from_logs():
 global proxy_log_vid_list
 global video_log_vid_list
 global new_video_list
 global exclude
    
 conn = sqlite3.connect('C:\\ProxyDB\\ip_url_database.db')
 c = conn.cursor()

 c.execute('select vid from proxy_log')

 for row in c:
    proxy_log_vid_list.append(row[0])

 proxy_log_vid_list = list(set(proxy_log_vid_list))

 c.execute('select vid from video_log')

 for row in c:
    video_log_vid_list.append(row[0])

 video_log_vid_list = list(set(video_log_vid_list))

 for vid in proxy_log_vid_list:
    if vid not in video_log_vid_list:
        new_video_list.append(vid)

 for vid in new_video_list:
  tags = []
  user_tags = []
  raw_user_tags = []

  try:
   doc = urllib2.urlopen('http://www.youtube.com/watch?v='+vid).read()
  except:
   print 'FATAL ERROR urlopen failed for vid - ' + str(vid)
   continue
  
     
  soup = BeautifulSoup(''.join(doc))

  if soup == None:
     print 'FATAL ERROR soup failed for vid - ' + str(vid)
     exit()

  try:
     video_title_meta = soup.find('meta', attrs={'name':'title'})['content'] #video title
  except:
     print vid
     continue
 
  try:
   video_title_meta = str(BeautifulSoup(video_title_meta, convertEntities=BeautifulSoup.HTML_ENTITIES))
  except:
   video_title = ''      

#Remove punctuations
  video_title = ''
  for ch in video_title_meta:
        if ch not in exclude:
                video_title = video_title + ch
        else:
                video_title= video_title + ' '
                
 #print 'Title:' + video_title


#extract nouns
  text = nltk.word_tokenize(video_title)
  for (token,token_type) in nltk.pos_tag(text):
        if token_type == 'NN' or token_type == 'NNS' or token_type == 'NNP':
                if is_ascii(token.lower()):
                   tags.append(token.lower())

#print tags


#view count
  try:
      watch_count = soup.find('span', attrs={'class':'watch-view-count'}).find('strong').contents[0].replace(',','')
  except:
      watch_count = 0
 #print 'Views:' + watch_count


#description
  try:
      video_description_meta = soup.find('p', attrs={'id':'eow-description'}).contents[0]
  except:
      video_description = ''
#print video_description_meta

  try:
   video_description_meta = str(BeautifulSoup(video_description_meta, convertEntities=BeautifulSoup.HTML_ENTITIES))
  except:
   video_description = ''    

#print video_description_meta
#print video_description_meta

#Remove punctuations
  video_description = ''
  for ch in video_description_meta:
        if ch not in exclude:
                video_description = video_description + ch
        else:
                video_description = video_description + ' '

#print video_description

  r = re.compile(r"(https?://[^ ]+)")    #remove hyperlinks from string
  video_description = r.sub('', video_description)


#video_description = ''.join(ch for ch in video_description if ch not in exclude)  


  if video_description == 'no description available':
    video_description = ''

 #print 'Description:'+video_description

  text = nltk.word_tokenize(video_description)
  for (token,token_type) in nltk.pos_tag(text):
        if token_type == 'NN' or token_type == 'NNS' or token_type == 'NNP':
                if is_ascii(token.lower()):
                   tags.append(token.lower())


#print tags


  try:
      video_category = soup.find('p', attrs={'id':'eow-category'}).find('a').contents[0]
      video_category = str(BeautifulSoup(video_category, convertEntities=BeautifulSoup.HTML_ENTITIES))
  except:
      try:
          video_category = soup.find('ul', attrs={'class':'watch-info-tag-list'}).find('a').contents[0]
          video_category = str(BeautifulSoup(video_category, convertEntities=BeautifulSoup.HTML_ENTITIES))
      except:
          video_category = ''
#video_category = video_category.replace('&amp;','&')
 #print 'Category:'+video_category


  try:
   for video_tags in soup.find('ul', attrs={'id':'eow-tags'}).findAll('a'):
    raw_user_tags.append(str(BeautifulSoup(video_tags.contents[0].lower(), convertEntities=BeautifulSoup.HTML_ENTITIES)))
  except:
   #print 'No tags found'       
   raw_user_tags = []
 
  ascii_user_tags = []
#print 'Raw tags:' + str(raw_user_tags)
  for tag in raw_user_tags:
     if is_ascii(tag):
             ascii_user_tags.append(tag)

#print 'Ascii tags:' + str(ascii_user_tags)

  user_tags = []
  for tag in ascii_user_tags:
        for punct in exclude:
                tag = tag.replace(punct,' ')
        for string in tag.split():        
            user_tags.append(string)

#print 'User tags:' + str(user_tags)

#print tags
  stemmed_tags = []
  for tag in tags:
        stemmed_tags.append(stem(tag))
#print stemmed_tags
 
  tags = list(set(stemmed_tags))
 #print 'Strong tags: ' + str(tags)

  stemmed_tags = []
  for tag in user_tags:
        stemmed_tags.append(stem(tag))

  user_tags = list(set(stemmed_tags))
 #print 'Weak tags: ' + str(user_tags)

  common_tags = list(set(user_tags) & set(tags))
 #print 'Intersection:' + str(common_tags)

  tag_string = ''
  i = 0
  for tag in common_tags:
     if i != len(common_tags)-1:
      tag_string = tag_string + tag + ';'
     else:
      tag_string = tag_string + tag    
     i = i + 1
    
 #print tag_string

  insert_str = 'insert into video_log values (\''+vid+'\','+str(watch_count)+',\''+video_category+'\',\''+tag_string+'\')'
 #print insert_str
  try:
    c.execute(insert_str)
  except:
    print 'FATAL error failed ' + insert_str  
    continue  
      

 conn.commit()

 c.close()
 conn.close()


