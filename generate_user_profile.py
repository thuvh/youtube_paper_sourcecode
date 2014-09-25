import os
import sqlite3
import urllib2
import string
import nltk
import re
from datetime import date
from BeautifulSoup import BeautifulSoup
from stemming.porter2 import stem


def update_user_profile():
 proxy_log_cip_list = []
 cip_vid_dict = {}

 conn = sqlite3.connect('C:\\ProxyDB\\ip_url_database.db')
 c = conn.cursor()

 c.execute('select distinct cip from proxy_log')

 for row in c:
    proxy_log_cip_list.append(row[0])

#print proxy_log_cip_list

 for cip in proxy_log_cip_list:
    c.execute('select vid from proxy_log where cip=\'' + str(cip)+'\' and dov=\''+(date.today()-timedelta(1)).strftime('%d/%m/%y')+'\'')
    cip_vid_list = []
    for row in c:
        cip_vid_list.append(row[0])
    cip_vid_dict[cip] = cip_vid_list

#in future we must take the most recent videos watched by a user so
#select vid from proxy_log where cip='cip' and dov > some_date
#print cip_vid_dict

 for cip in proxy_log_cip_list:
    cip_vid_list = cip_vid_dict[cip]
    #print cip
    #print cip_vid_list
    keyword_list = []

    for vid in cip_vid_list:
        #print 'select keywords from video_log where vid=\'' + str(vid)+'\''
        c.execute('select keywords from video_log where vid=\'' + str(vid)+'\'')
        for row in c:
            #print row
            for keywd in row[0].split(';'):
                if keywd != '':
                   keyword_list.append(keywd)

    c.execute('select keywords from user_log where cip=\'' + str(cip)+'\'')
    cip_in_table = False
    for row in c:
       #print row
       cip_in_table = True 
       for keywd in row[0].split(';'):
           if keywd != '':
              keyword_list.append(keywd)

       
    keyword_list = list(set(keyword_list))

    #print keyword_list

    tag_string = ''
    i = 0
    for tag in keyword_list:
     if len(tag) + len(tag_string) < 4000:
      if i != len(keyword_list)-1:
        tag_string = tag_string + tag + ';'
      else:
        tag_string = tag_string + tag    
      i = i + 1
     else:
        #print tag_string
        tag_string = tag_string[:len(tag_string) - 1]
        break

    #print tag_string
    #print 'update user_log set keywords=\''+tag_string+'\' where cip=\'' + str(cip) + '\''

    if cip_in_table:
         c.execute('update user_log set keywords=\''+tag_string+'\' where cip=\'' + str(cip) + '\'')
    else:
         #print 'insert into user_log values(\'' + str(cip) + '\',\''+tag_string+'\')'
         c.execute('insert into user_log values(\'' + str(cip) + '\',\''+tag_string+'\')')


 conn.commit()
 c.close()
 conn.close()


