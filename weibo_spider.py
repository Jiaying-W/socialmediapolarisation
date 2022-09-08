# -*-coding:utf-8-*-

import os.path
import time

from utils.spider_util import Spider

class Weibo_Spider(Spider):
    def __init__(self):
        super().__init__("spider")
        self.user_params = {}
        self.mblog_params = {'feature': '0'}
        self.comment_params = {'is_reload':'1',
                               'is_show_bulletin':'2',
                               'is_mix':'0',
                               'count':'20',
                               'type':'feed'}
        self.hottime_params = {"containerid": "100103type=1&q={}",
                               "page_type": "searchall",
                               "page": 1
                               }
        self.report_params = {'id':0,
                              'page':1,
                              'moduleID':'feed',
                              'count':10}
        self.user_id = 0
        self.mblog_ids = []
        self.comment_ids = []

    def spider_user(self, user_id=None):
        if user_id == None:
            user_id = self.user_id
        self.user_params['uid'] = user_id
        user_json = self.request('user',self.user_params)
        item = user_json.get('data').get('user')
        user = {}
        try:
            user['user_id'] = item.get('id')
            user['screen_name'] = item.get('screen_name')
            user['verified_reason'] = item.get('verified_reason')
            user['description'] = item.get('description')
            user['location'] = item.get('location')
            user['gender'] = item.get('gender')
            user['followers_count'] = item.get('followers_count')
            user['friends_count'] = item.get('friends_count')
            user['statuses_count'] = item.get('statuses_count')
        except Exception as e:
            self.logger.error("将微博用户的HTML解析数据转换成字典时，发生错误")
            self.logger.error(e)
        self.save('user',user)

    def spider_mblog(self, user_id=None, pages=10):
        if user_id == None:
            user_id = self.user_id
        self.mblog_params['uid'] = user_id
        for i in range(pages):
            self.mblog_params['page'] = i
            mblog_json = self.request('mblog',self.mblog_params)
            self.mblog_params['since_id'] = mblog_json.get('data').get('since_id')
            items = mblog_json.get('data').get('list')
            mblogs = []
            for i in range(len(items)):
                try:
                    mblog = {}
                    mblog['mblog_id'] = items[i].get('id')
                    mblog['user_id'] = items[i].get('user')['id']
                    mblog['screen_name'] = items[i].get('user')['screen_name']
                    mblog['text'] = items[i].get('text_raw')
                    mblog['source'] = items[i].get('source')
                    mblog['attitudes_count'] = items[i].get('attitudes_count')
                    mblog['comments_count'] = items[i].get('comments_count')
                    mblog['reposts_count'] = items[i].get('reposts_count')
                    mblog['region_name'] = items[i].get('region_name')
                    self.mblog_ids.append(mblog['mblog_id'])
                    mblogs.append(mblog)
                except Exception as e:
                    self.logger.error("将微博博客的HTML解析数据转换成字典时，发生错误")
                    self.logger.error(e)
            self.save_list('mblog',mblogs)
            time.sleep(10)

    def spider_comment(self, user_id=None, weibo_ids=None):
        if user_id == None:
            user_id = self.user_id
        if weibo_ids == None:
            weibo_ids = self.mblog_ids
        self.comment_params['uid'] = user_id
        for weibo_id in weibo_ids[110:]:
            self.comment_params['id'] = weibo_id
            while 'max_id' not in self.comment_params or self.comment_params['max_id'] != 0:
                comment_json = self.request('comment',self.comment_params)
                try:
                    self.comment_params['max_id'] = comment_json.get('max_id')
                    items = comment_json.get('data')
                    if items == []:
                        break
                except:
                    break
                comments = []
                for i in range(len(items)):
                    try:
                        comment = {}
                        comment['comment_id'] = items[i].get('id')
                        comment['comment_user_id'] = items[i].get('user')['id']
                        comment['mblog_id'] = weibo_id
                        comment['user_id'] = user_id
                        comment['screen_name'] = items[i].get('user')['screen_name']
                        comment['text'] = items[i].get('text_raw')
                        comment['source'] = items[i].get('source')
                        comment['like_counts'] = items[i].get('like_counts')
                        comment['floor_number'] = items[i].get('floor_number')
                        comment['created_at'] = items[i].get('created_at')
                        self.comment_ids.append(comment['comment_id'])
                        comments.append(comment)
                    except Exception as e:
                        self.logger.error("将微博评论的HTML解析数据转换成字典时，发生错误")
                        self.logger.error(e)
                self.save_list('comment', comments)
                time.sleep(3)
            if 'max_id' in self.comment_params:
                self.comment_params.pop('max_id')

    def spider_reports(self,mblog_id,report_count):
        self.report_params['id'] = mblog_id
        pages = report_count//10
        report_info = []
        for i in range(pages+1):
            self.report_params['page'] = i
            report_json = self.request('report',self.report_params)
            items = report_json.get('data')
            for item in items:
                report = {}
                report['mblog_id'] = mblog_id
                report['report_id']= item['id']
                report['user_id'] = item['user']['id']
                report['report_text'] = item['text_raw']
                report_info.append(report)
        return report_info

    '100103type=1&q=上海封城'
    def spider_hot(self,task,keyword,page):
        self.hottime_params['containerid'] = self.hottime_params['containerid'].format(keyword)
        self.hottime_params['page'] = page
        data = self.request(task, self.hottime_params, True)
        hottime_info = []
        for card in data["data"]["cards"]:
            if 'card_group' not in card:
                continue
            items = card['card_group']
            for item in items:
                if 'mblog' in item:
                    mblog = {}
                    mblog['mblog_id'] = item['mblog']['id']
                    mblog['mblog_text'] = item['mblog']['text']
                    mblog['user_id'] = item['mblog']['user']['id']
                    mblog['report_count'] = item['mblog']['reposts_count']
                    hottime_info.append(mblog)
        return hottime_info

    def save_jsonL(self,json_list,path):
        with open(path,'a+',encoding='utf-8') as w:
            for json in json_list:
                w.write(str(json) + '\n')
        print("已成功爬取并保存%d条信息至%s" %(len(json_list),path))


    def user_pipeline(self,user_id,pages=10):
        self.user_id = user_id
        self.spider_user()
        self.spider_mblog(pages=pages)
        self.spider_comment()

    def topic_pipeline(self,keyword,data_path,pages=100):
        self.hottime_path = os.path.join(data_path,'hottime.txt')
        self.report_path = os.path.join(data_path, 'report.txt')
        for page in range(pages):
            hottime_info = self.spider_hot('hottime',keyword,page+1)
            self.save_jsonL(hottime_info,self.hottime_path)
            time.sleep(10)
            for info in hottime_info:
                mblog_id = info['mblog_id']
                report_count = info['report_count']
                report_info = self.spider_reports(mblog_id,report_count)
                time.sleep(5)
                self.save_jsonL(report_info,self.report_path)

    def decoder_list(self,task:str,
                data_list:list):
        return [self.decoder(task,data) for data in data_list]


    def decoder(self,task,data):
        if task == 'comment':
            data = self._decode_comment(data)
        elif task == 'mblog':
            data = self._decode_mblog(data)
        elif task == 'user':
            data = self._decode_user(data)
        return data

    def _decode_comment(self,data):
        titles = ['comment_id','comment_user_id','mblog_id','user_id','screen_name','like_counts','floor_number','source','text','created_at']
        return {title:item for title,item in zip(titles,list(data))}

    def _decode_mblog(self,data):
        titles = ['mblog_id','user_id','screen_name','text','source','attitudes_count','comments_count','reposts_count','region_name']
        return {title:item for title,item in zip(titles,list(data))}

    def _decode_user(self,data):
        titles = ['id','screen_name','verified_reason','description','location','gender','followers_count','friends_count','statuses_count']
        return {title:item for title,item in zip(titles,list(data))}

if __name__ == "__main__":
    user_list = []
    for uer_id in user_list:
        weibo_spider = Weibo_Spider()
        weibo_spider.pipeline(uer_id)
