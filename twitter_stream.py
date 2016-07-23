import sys
import time
#!/usr/bin/python
#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import json
from requests_oauthlib import OAuth1Session
import re
import twitter
from twitter import (__version__, _FileCache, json, DirectMessage, List,
                     Status, Trend, TwitterError, User, UserStatus, Category)

consumer_key='****'
consumer_secret='****'
access_token_key='****'
access_token_secret='****'

def multi_process_twitter():
    from multiprocessing import Process,Queue,current_process,Lock
    import time,queue

    def sender(block_q,lock):
        p = current_process()
        while True:
            lock.acquire()
            try:
                api = twitter.Api(consumer_key=consumer_key,consumer_secret=consumer_secret,
                                  access_token_key=access_token_key,access_token_secret=access_token_secret)
                cur_date = time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime())
                cur_date = cur_date.split('_')[0]
                block_q.put([api,cur_date],timeout=4)
            except queue.Full:
                print "block queue is full"
            except KeyboardInterrupt as e:
                print "break from keyboard!"
                break
            except:
                print "Unexpected error from sender :", sys.exc_info()[0]

            lock.release()
            time.sleep(0.5)
        block_q.close()

    def receiver(block_q,lock):
        p = current_process()
        while True:
            lock.acquire()
            try:
                api,cur_date = block_q.get(timeout=5)
                #open necessary files
                file_name = "sample_timeline_{}.txt".format(cur_date)
                json_file = "sample_json_file_{}.json".format(cur_date)
                f = open(file_name,'a')
                fjson = open(json_file,'a')

                #get public stream
                # small_sample is a gnerator object.
                small_sample = api.GetStreamSample() 
                count = 0
                for sample in small_sample:
#                    if count >= 10:
#                        print "want to break"
#                        break
                    try:
                        if type(sample) is not dict:
                            print "the data type is {}".format(type(sample))
                            sample = json.loads(sample)
                        #if the error message come back, it will be in str format, and we have to json.loads it.
                        #if the normal json message come back, json object can not be json.loads, without error raised.
                        #hopen it can store the information of TwitterError, and error from Twitter server such as "disconnect"
                        json.dump(sample,fjson)
                        fjson.write("\n")
                    except:
                        error_msg =  "the object returned can not be dumped."
                        print error_msg
                        f.write(error)
                        f.write("\n")
                    
                    if u"disconnect" in sample or "disconnect" in sample:
                        error_msg =  "find disconnect json response."
                        print error_msg
                        f.write(error)
                        f.write("\n")
                        time.sleep(50)
                    
                    if len(sample) > 20:
                        if sample[u'lang'] == "ja":
                            text = sample[u'text'].strip()
                            text = re.sub(r'\s','',text)
                            if text:
                                count += 1
                                text = "Text : {}\n".format(text)
                                name = "Name: {}\n".format(sample[u"user"][u"screen_name"])                 
                                print "{}\n:{}{}\n".format(count,text,name)
                                f.write(text)
                                f.write(name)

                f.close()
                fjson.close()
            except queue.Empty:
                print "block queue is empty!!!!"
            except TwitterError as e:
                print str(e)
                print "the connection is interruptted!!!"
            except KeyboardInterrupt as e:
                print "break from keyboard!!"
                break
            except:
                print "Unexpected error from receiver:", sys.exc_info()[0]
            lock.release()
            time.sleep(0.5)

    block_q = Queue(maxsize=1)
    all_process = []
    lock = Lock()
    p1 = Process(target=sender,args=(block_q,lock),name='Sender')
    p2 = Process(target=receiver,args=(block_q,lock),name='Receiver')
    all_process.append(p1)
    all_process.append(p2)
    p1.start()
    p2.start()
    for p in all_process:
        p.join()


if __name__ == "__main__":
    multi_process_twitter()

