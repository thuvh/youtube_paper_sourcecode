import os
import sqlite3
import urllib2
import string
import nltk
import re
import random
from datetime import date
from BeautifulSoup import BeautifulSoup
from stemming.porter2 import stem

proxy_log_cip_list = []
video_log_vid_list = []
cip_vid_dict_unseen = {}
cip_vid_dict_seen = {}
cip_vid_recommend_dict = {}
cip_category_dict = {}
cip_keyword_dict = {}
cip_cip_dict = {}
vid_vc_dict = {}

def get_recommend_vid(cip):
    recommendations = []
    count = 0
    max_score = 0
    max_score_vid = ''
    second_max_score_vid = ''
    if cip not in proxy_log_cip_list:
        return []
    for (vid,score) in cip_vid_recommend_dict[cip]:
        if score > max_score:
            second_max_score_vid = max_score_vid
            max_score = score
            max_score_vid = vid

    #print 'Max score - ' + str(max_score)

    if max_score_vid != '' and max_score_vid not in recommendations:
        recommendations.append(max_score_vid)
        print 'Max score vid :' + max_score_vid
        count = count + 1

    if second_max_score_vid != '' and second_max_score_vid not in recommendations:
        print 'Second max score vid :' + max_score_vid        
        recommendations.append(second_max_score_vid)
        count = count + 1
        
    uu_recommendations = [] 
    for ((cmpcip,score)) in cip_cip_dict[cip]:
        if score > 0.5:
            for vid in cip_vid_dict_seen[cmpcip]:
                if vid in cip_vid_dict_unseen[cip] and vid not in recommendations:
                    uu_recommendations.append(vid)

    if count < 5:
     if len(uu_recommendations) == 1:
        if uu_recommendations[0] != '' and uu_recommendations[0] not in recommendations:
           recommendations.append(uu_recommendations[0])
           count = count + 1

    if count < 5:
     if len(uu_recommendations) >= 2:
      for vid in random.sample(uu_recommendations, 2):
        if vid != '' and vid not in recommendations:
           recommendations.append(vid)
           count = count + 1

#    uu_recommendations.sort(cmp=lambda x,y: cmp(vid_vc_dict[x],vid_vc_dict[y]),reverse=True)    

#    for i in range(len(uu_recommendations)):
#        if len(recommendations) < 5:
#           recommendations.append(uu_recommendations[i])
#           count = count + 1
#        else:
#           break


    category_recommendations = []

    if count < 5:
        max_ratio = 0.0
        fav_category = ''
        b = cip_category_dict[cip]
        for category in b.keys():
            if b[category] > max_ratio:
                max_ratio = b[category]
                fav_category = category

        conn = sqlite3.connect('C:\\ProxyDB\\ip_url_database.db')
        c = conn.cursor()

        print 'Fav category: ' + str(fav_category)
        
        for vid in cip_vid_dict_unseen[cip]:
            c.execute('select category from video_log where vid=\''+str(vid)+'\'')
            for row in c:
                vid_category = row[0]
           
            if vid_category == fav_category and vid not in recommendations:
                category_recommendations.append(vid)

        c.close()
        conn.close()

#    print category_recommendations 
    
#        category_recommendations.sort(cmp=lambda x,y: cmp(vid_vc_dict[x],vid_vc_dict[y]),reverse=True)    

#        for i in range(len(category_recommendations)):
#            if len(recommendations) < 5:
#               recommendations.append(category_recommendations[i])
#               count = count + 1
#            else:
#               break
        if len(category_recommendations) < 5:
            for i in range(len(category_recommendations)):
              if category_recommendations[i] not in recommendations:  
                recommendations.append(category_recommendations[i])
                count = count + 1
                if count == 5:
                    break

        if count < 5 and len(category_recommendations) >= 5: 
         for vid in random.sample(category_recommendations, 5):
          if vid != '' and vid not in recommendations:
             recommendations.append(vid)
             count = count + 1
             if count == 5:
                 break


                
    if count < 5:
        unseen_videos = cip_vid_dict_unseen[cip]
        unseen_videos.sort(cmp=lambda x,y: cmp(vid_vc_dict[x],vid_vc_dict[y]),reverse=True)
        for j in range(len(unseen_videos)):
            if unseen_videos[j] not in recommendations:
                print 'Random recommendation :' + str(vid)
                recommendations.append(unseen_videos[j])
                count = count + 1
                if count == 5:
                    break

    return recommendations
             
         

def recommend_setup():
 conn = sqlite3.connect('C:\\ProxyDB\\ip_url_database.db')
 c = conn.cursor()

 c.execute('select vid,views from video_log')

 for row in c:
    vid_vc_dict[row[0]] = row[1]


 c.execute('select distinct cip from proxy_log')

 for row in c:
    proxy_log_cip_list.append(row[0])

 c.execute('select distinct vid from video_log')

 for row in c:
    video_log_vid_list.append(row[0])


#print proxy_log_cip_list

 for cip in proxy_log_cip_list:
    c.execute('select distinct vid from proxy_log where cip=\'' + str(cip)+'\'')
    cip_vid_list = []
    for row in c:
        cip_vid_list.append(row[0])

    cip_vid_dict_seen[cip] = cip_vid_list

    videos_not_seen = []
    
    for vid in video_log_vid_list:
        if vid not in cip_vid_list:
            videos_not_seen.append(vid)
    
    cip_vid_dict_unseen[cip] = videos_not_seen


 for cip in proxy_log_cip_list:
    cip_vid_list = cip_vid_dict_unseen[cip]
    cip_keywd_list = []
    c.execute('select keywords from user_log where cip=\'' + str(cip)+'\'')
    for row in c:
        for keywd in row[0].split(';'):
            cip_keywd_list.append(keywd)
                
    cip_keyword_dict[cip] = cip_keywd_list



#User-User comparison
 for cip in proxy_log_cip_list:
    #print cip
    cip_cip_dict[cip]= []
    cip_keywd_list = []
    cip_keywd_list = cip_keyword_dict[cip]
    for cmpcip in proxy_log_cip_list:
        cmpcip_keywd_list = []
        if cmpcip != cip:
            #print 'Comparing with:'+str(cmpcip)
            cmpcip_keywd_list = cip_keyword_dict[cmpcip]
            score = 0
            count = 0.0
            if len(cmpcip_keywd_list) > 0 and len(cip_keywd_list) > 0:
              for keywd in cip_keywd_list:
                if keywd in cmpcip_keywd_list:
                     count = count + 1
              score = count / len(cmpcip_keywd_list)
            cip_cip_dict[cip].append((cmpcip,score))

 #print cip_cip_dict


#Category analaysis

 for cip in proxy_log_cip_list:
    cip_vid_list = cip_vid_dict_seen[cip]
    #print cip
    cip_category_list = []
    for vid in cip_vid_list:
      c.execute('select category from video_log where vid=\'' + str(vid)+'\'')
      for row in c:
        for category in row[0].split(';'):
            cip_category_list.append(category)

    b = {}
    for item in cip_category_list:
       b[item] = b.get(item, 0) + 1
       
    for category in b.keys():
        b[category] = b.get(category,0)/float(len(cip_category_list))
    
    cip_category_dict[cip] = b

#print cip_category_dict

#Keyword analysis
#User-video comparison

 for cip in proxy_log_cip_list:
    cip_vid_recommend_dict[cip] = []
    cip_vid_list = cip_vid_dict_unseen[cip]
    #print cip
    cip_keywd_list = cip_keyword_dict[cip]
    vid_keyword_list = []
    
    for vid in cip_vid_list:
        #print 'select keywords from video_log where vid=\'' + str(vid)+'\''
        c.execute('select keywords from video_log where vid=\'' + str(vid)+'\'')
        for row in c:
            #print row
            for keywd in row[0].split(';'):
                if keywd != '':
                   vid_keyword_list.append(keywd)

        count = 0.0
        score = 0
        if len(vid_keyword_list) > 0:
          for keywd in vid_keyword_list:
            if keywd in cip_keywd_list:
                 count = count + 1
          score = count / len(vid_keyword_list)

        if score > 0.5:           
           cip_vid_recommend_dict[cip].append((vid,score))

 #print cip_vid_recommend_dict


 c.close()
 conn.close()


