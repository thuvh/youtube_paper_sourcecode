import time
import threading
import sys
import user_vid_recommendation
import process_proxy_server_logs
import process_videos_from_logs
import generate_user_profile
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

vod = open('video_of_the_day.txt','r')
video_of_day = ''

def housekeeping():
    global video_of_day
    global vod
    print 'Housekeeping operation started'
#    video_of_day = vod.readline().strip()
    process_proxy_server_logs.process_proxy_logs()
    process_videos_from_logs.process_videos_from_logs()
    generate_user_profile.update_user_profile()
    user_vid_recommendation.recommend_setup()
    video_of_day = process_proxy_server_logs.get_video_of_day()
    print 'Housekeeping operation finished'

def housekeeping_target():
  while True:
    housekeeping()
    time.sleep(86400)
    

class MyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global video_of_day
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write('<u>Video of the day</u> <br><br> <iframe width="400" height="250" src="http://www.youtube.com/embed/'+video_of_day+'?rel=0" frameborder="0" allowfullscreen></iframe> <br><br><br><br>')
        self.wfile.write('<u>Video recommendations for %s </u><br><br>' % self.client_address[0])
        vid_list = user_vid_recommendation.get_recommend_vid("128.112.92.72")
        if len(vid_list) == 0:
            self.wfile.write('Sorry no recommendations for you. Start using proxy server at 128.112.139.238:22 for watching YouTube videos to get recommendations<br><br>') 
            return
        i = 0
        for vid in vid_list:
            self.wfile.write('<iframe width="400" height="250" src="http://www.youtube.com/embed/'+str(vid)+'?rel=0" frameborder="0" allowfullscreen></iframe>')
            if i % 2 == 0:
                self.wfile.write('&nbsp;&nbsp;&nbsp;&nbsp;')
            else:    
                self.wfile.write('<br><br>')
            i = i + 1
        self.wfile.write('<br><br>Use proxy server at 128.112.139.238:22 for watching YouTube videos to get better recommendations <br><br>')
        return


def main():
    try:
        t = threading.Thread(target=housekeeping_target)
        t.daemon = True
        t.start()
        server = HTTPServer(('', 80), MyHandler)
        print 'started http server'
        server.serve_forever()
    except KeyboardInterrupt:
        print '^C received, shutting down server'
        server.socket.close()


if __name__ == '__main__':
    main()
