import user_vid_recommendation

user_vid_recommendation.recommend_setup()
cip_list = ['128.112.104.210','128.112.106.152','128.112.106.195','128.112.139.195','128.112.139.238','128.112.92.72','128.112.92.81','128.112.92.83','128.112.92.85','128.112.93.48','128.112.93.50','128.112.93.51','140.180.23.223','140.180.4.185','140.180.4.75','140.180.48.154','140.180.56.230','76.102.48.21','76.102.54.43','93.40.129.89','93.40.98.50']

for cip in cip_list:
    print cip
    vid_list = user_vid_recommendation.get_recommend_vid(cip)
    print
    print vid_list



        