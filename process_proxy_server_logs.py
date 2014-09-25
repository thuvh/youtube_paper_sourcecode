import os
import sqlite3
from datetime import date, timedelta
from urlparse import urlparse,parse_qs


def get_video_of_day():
    yesterday = (date.today()-timedelta(1)).strftime('%d/%m/%y') 
    conn = sqlite3.connect('C:\\ProxyDB\\ip_url_database.db')
    c = conn.cursor()
    query_str = 'select vid from proxy_log where dov=\''+yesterday+'\' and freq=(select max(freq) from proxy_log where dov=\''+yesterday+'\')'
    c.execute('select vid from proxy_log where dov=\''+yesterday+'\' and freq=(select max(freq) from proxy_log where dov=\''+yesterday+'\')')
#    return query_str
    for row in c:
        return row[0]
          

#fp = open('proxy_server_stats'+date.today().strftime('_%d_%b_%Y.txt'),'r')
def process_proxy_logs():
 #fp = open('C:\PS_Stats\proxy_server_stats_04_Dec_2011.txt','r')
 fp = open('proxy_server_stats'+(date.today()-timedelta(1)).strftime('_%d_%b_%Y.txt'),'r')
# fp = open('proxy_server_stats'+date.today().strftime('_%d_%b_%Y.txt'),'r')
 conn = sqlite3.connect('C:\\ProxyDB\\ip_url_database.db')
 c = conn.cursor()

#c.execute('''create table stocks(date text, trans text, symbol text, qty real, price real)''')

# Save (commit) the changes
#conn.commit()
# We can also close the cursor if we are done with it
#t = (symbol,)
#c.execute('select * from stocks where symbol=?', t)
#for row in c:
#    print row

 ip_url = {}

 for line in fp:
    fields = line.split('>')
    dov = fields[0].strip()
    tov = fields[1].strip()
    
    for values in fields[2:]:
        tag = values.split(':')[0].strip()

        if tag == 'CIP':
            client_ip = values.split(':')[1].strip()
            if client_ip == '128.112.93.49':
                client_ip = '128.112.139.238'            


        if tag == 'URI':
            uri = values.split(':')[1].strip()
            #print client_ip + ' ' + uri
            if len(uri.split('youtube')) > 1:
                #print uri
                o = urlparse(uri)
                parameters = parse_qs(o.query)
                if parameters.has_key('v'):
                   uri = parameters['v'][0]
                
                if ip_url.has_key(client_ip):
                    entry_exists = False
                    for entry in ip_url[client_ip]:
                        if entry[0] == dov and entry[1] == uri:
                            entry[2] = entry[2] + 1
                            entry_exists = True
                            break 
                    if not entry_exists:
                       ip_url[client_ip].append([dov,uri,1])    

                else:
                    ip_url[client_ip]=[]
                    ip_url[client_ip].append([dov,uri,1])
                #print dov + ' ' + tov + ' ' + client_ip + ' ' + uri 

#print ip_url


 for key in ip_url.keys():
    client_info = ip_url[key]
    for record in client_info:
        insert_str = 'insert into proxy_log values (\''+record[0]+'\',\''+key+'\',\''+record[1]+'\','+str(record[2])+')'
        #print insert_str
        if len(record[1]) == 11:
          try:
#             print insert_str
            c.execute(insert_str)
          except:
            continue  

 conn.commit()

 fp.close()
 c.close()
 conn.close()
