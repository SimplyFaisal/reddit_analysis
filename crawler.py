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
    CLOUD_QUERY = 'timestamp:{end}..{start}'
    CLOUD_SYNTAX = 'cloudsearch' 

    def __init__(self, username, password, mongodb, start, end, interval=timedelta(weeks=12)):
        self.reddit = praw.Reddit(self._random_name())
        self.reddit.login(username, password)
        self.mongodb = mongodb
        self.start = start
        self.end = end
        self.interval = interval
        return
    
    def crawl(self, college_info, limit=None, sort='new'):
        college, subreddit = college_info['name'], college_info['subreddit']
        upper = self.start
        lower = upper - self.interval
        # trying to guard against the case where the chuncks overlap and
        # you wind up loosing a lot of posts, definately a better way to 
        # do this
        while lower > self.end:
            start_seconds = self._to_seconds(upper)
            end_seconds = self._to_seconds(lower)
            upper = lower
            lower = lower - self.interval
            if lower < self.end:
                lower = self.end
            query = self.CLOUD_QUERY.format(start=start_seconds, end=end_seconds)
            print college, str(lower.date()), str(upper.date())
            posts = self.reddit.search(query, subreddit=subreddit, sort=sort, limit=None,
                    syntax=self.CLOUD_SYNTAX)
            for post in posts:
                try:
                    comment_ids = []
                    post.replace_more_comments(limit=None, threshold=0)
                    comments = praw.helpers.flatten_tree(post.comments)
                    for comment in comments:
                        _id = self.mongodb.comments.insert(
                            self.serialize_comment(comment, subreddit, college))
                        comment_ids.append(_id)
                except Exception as e:
                    print e
                mongo_record = serialize_post(post, subreddit, college)
                mongo_record['comments'] = comment_ids
                self.mongodb.posts.insert(mongo_record)
        return
    
    def update(self, subreddit, limit=30):
        return self.reddit.get_subreddit(subreddit).get_new(limit=limit)

    def _random_name(self, length=10):
        return ''.join(random.choice(string.ascii_letters) for i in range(length))

    def _to_seconds(self, dt):
        return int(dt.strftime('%s'))

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

    def __init__(self, threadID, reddit_client, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.reddit_client = reddit_client
        self.q = q
        return

    def run(self):
        while not self.q.empty():
            college_info = self.q.get_nowait()
            self.reddit_client.crawl(college_info)
    
class MultiThreadedCrawler(object):

    def __init__(self, credentials, colleges, start=datetime.now(), end=datetime(2010, 1, 1)):
        self.credentials = credentials
        self.colleges = colleges
        self.threads = []
        self.start = start
        self.end = end

    def queue_up(self):
        q = Queue(maxsize=len(self.colleges))
        for college in self.colleges:
            q.put_nowait(college)

        for credential in self.credentials:
            username , password = credential
            mongodb = pymongo.MongoClient()['reddit']
            client = RedditApiClient(username, password, mongodb, start=self.start, end=self.end)
            self.threads.append(RedditThread(username, client, q))

    def begin(self):
        for thread in self.threads:
            thread.start()








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
    multi = MultiThreadedCrawler(CREDENTIALS, SUBREDDITS)
    multi.queue_up()
    multi.begin()
