import pymongo
import praw
import random
import threading
import string
from datetime import datetime, timedelta
from config import PRAW_PASSWORD, PRAW_USERNAME, SUBREDDITS, CREDENTIALS
from analysis import KeywordExtractor
from Queue import Queue


db = pymongo.MongoClient()['reddit']

class RedditApiClient(object):
    CLOUD_QUERY = 'timestamp:{start}..{end}'
    CLOUD_SYNTAX = 'cloudsearch' 

    def __init__(self, username, password, start, end):
        self.reddit = praw.Reddit(self._random_name())
        self.reddit.login(username, password)
        self.start = start
        self.end = end
        return
    
    def crawl(self, subreddit, limit=None, sort='new'):
        query = self.CLOUD_QUERY.format(start=self.start, end=self.end)
        print query , subreddit, sort , self.CLOUD_SYNTAX
        return self.reddit.search('timestamp:1373932800..1474019200', subreddit=subreddit, sort=sort, limit=None,
                syntax=self.CLOUD_SYNTAX)
    
    def update(self, subreddit, limit=30):
        return self.reddit.get_subreddit(subreddit).get_new(limit=limit)

    def _random_name(self, length=10):
        return ''.join(random.choice(string.ascii_letters) for i in range(length))

    def serialize_post(self, submission, college, subreddit):
     return {
            'rid': submission.id,
            'title': submission.title,
            'text': submission.selftext,
            'url': submission.url, 
            'ups': submission.ups, 
            'downs': submission.downs,
            'subreddit': subreddit, 
            'college': college,     
            'created_utc': datetime.utcfromtimestamp(submission.created_utc),
            'comments': []
        }

    def serialize_comment(self, comment, subreddit, college):
        return {
            'rid': comment.id,
            'text': comment.body, 
            'ups' : comment.ups, 
            'downs': comment.downs,
            'college': college, 
            'subreddit' : subreddit,
            'created_utc': datetime.utcfromtimestamp(comment.created_utc)
        }

class RedditThread(threading.Thread):

    def __init__(self, threadID, reddit_client, mongodb):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.reddit_client = reddit_client
        self.mongodb = mongodb
        return

    def run(self, college_info):
        college, subreddit = college_info['name'] , college_info['subreddit']
        posts = self.reddit_client.crawl('gatech')
        for post in posts:
            print post.title
            post = self.reddit_client.serialize_post(post, college, subreddit)
            self.mongodb.posts.insert(post)
        return

    def start(self, q):
        while not q.empty():
            self.run(q.get_nowait())

    
class MultiThreadedCrawler(object):

    def __init__(self, credentials, colleges, start=datetime.now(), end=timedelta(weeks=156)):
        self.credentials = credentials
        self.colleges = colleges
        self.threads = []
        self.q = Queue(maxsize=len(credentials))
        self.start = start
        self.end = end

    def queue_up(self):
        for credential, college in zip(self.credentials, self.colleges):
            start = int(self.start.strftime("%s")) * 1000
            delta = (self.start - self.end)
            end = int(delta.strftime("%s")) * 1000
            print start, end
            username , password = credential
            self.q.put_nowait(college)
            client = RedditApiClient(username, password, start=start, end=end)
            mongodb = pymongo.MongoClient()['reddit']
            self.threads.append(RedditThread(username, client, mongodb))

    def begin(self):
        for thread in self.threads:
            thread.start(self.q)








def main():
    reddit = praw.Reddit('PRAW Gatech subreddit monitor')
    reddit.login(PRAW_USERNAME, PRAW_PASSWORD)
    print 'Logged In'
    for school, subreddit in SUBREDDITS.iteritems():
        posts = reddit.get_subreddit(subreddit)
        params = {'sort':'new', 't':'year'}
        crawl_subreddit(posts.get_new(params=params, limit=30), school, subreddit)
    return

def crawl_subreddit(posts, college , subreddit):
    c = 0
    p = 0
    records = []
    for submission in posts:
        p += 1
        record = serialize_post(submission, subreddit, college)
        try:
            submission.replace_more_comments(limit=None, threshold=0)
            comments = praw.helpers.flatten_tree(submission.comments)
            comment_records = []
            for comment in comments:
                c += 1
                document = serialize_comment(comment, subreddit, college)
                comment_records.append(document)

            # upsert by reddit id
            KeywordExtractor().extract(comment_records, get_text=lambda x: x['text'])
            for comment in comment_records:
                _id = db.comments.insert(comment)
                record['comments'].append(_id)
            records.append(record)
        except Exception as error:  
            print error
        print submission.title
    KeywordExtractor().extract(records, get_text=lambda x: x['text'])
    db.posts.insert(records)
    print '{} : {} posts {} comments'.format(college, p, c)
    return

def serialize_post(submission, subreddit, college):
     return {
            'rid': submission.id,
            'title': submission.title,
            'text': submission.selftext,
            'url': submission.url, 
            'ups': submission.ups, 
            'downs': submission.downs,
            'subreddit': subreddit, 
            'college': college,     
            'created_utc': datetime.utcfromtimestamp(submission.created_utc),
            'comments': []
        }
        
def serialize_comment(comment, subreddit, college):
    return {
            'rid': comment.id,
            'text': comment.body, 
            'ups' : comment.ups, 
            'downs': comment.downs,
            'college': college, 
            'subreddit' : subreddit,
            'created_utc': datetime.utcfromtimestamp(comment.created_utc)
        }

if __name__ == '__main__':
    multi = MultiThreadedCrawler([CREDENTIALS[0]], SUBREDDITS)
    multi.queue_up()
    multi.begin()
