import pymongo
import praw
import random
import threading
import string
from datetime import datetime, timedelta
from collections import deque
from config import PRAW_PASSWORD, PRAW_USERNAME, SUBREDDITS, CREDENTIALS
from analysis import KeywordExtractor
from Queue import Queue


db = pymongo.MongoClient()['reddit']

class RedditApiClient(object):
    """ Wrapper around praw """

    CLOUD_QUERY = 'timestamp:{end}..{start}'
    CLOUD_SYNTAX = 'cloudsearch' 

    def __init__(self, username, password, mongodb, start, end, interval=timedelta(weeks=12)):
        """
        Input:
            username <string> : reddit username
            password <string> : reddit password
            mongodb: mongodb db object
            start <datetime> : start time   
            end <datetime> : end time
            interval <timedelta> : amount the increment gets shifted each request
        """
        self.reddit = praw.Reddit(self._random_name())
        self.reddit.login(username, password)
        self.mongodb = mongodb
        self.start = start
        self.end = end
        self.interval = interval
        return
    
    def crawl(self, college_info, limit=None, sort='new'):
        """
        Crawl the specified subreddit in the range (self.start, self.end)

        Input:
            college_info <dict>: { 'name': name of the college, 'subreddit': name of the subreddit}
            limit <int> : max number of posts to return per call
            sort <string> : sort order of the returned posts
        """
        college, subreddit = college_info['name'], college_info['subreddit']
        upper = self.start
        lower = upper - self.interval
        while lower > self.end:
            start_seconds = self._to_seconds(upper)
            end_seconds = self._to_seconds(lower)
            upper = lower
            lower = lower - self.interval

            # if the  lower bound is past the beginning of our range just set 
            # the range equal to the beginning of the range

            if lower < self.end:
                lower = self.end
            query = self.CLOUD_QUERY.format(start=start_seconds, end=end_seconds)
            print college, str(lower.date()), str(upper.date())
            posts = self.reddit.search(query, subreddit=subreddit, sort=sort, limit=None,
                    syntax=self.CLOUD_SYNTAX)
            for post in posts:
                comment_ids = []
                try:
                    post.replace_more_comments(limit=None, threshold=0)
                    comment_stack = deque(post.comments)
                    while len(comment_stack):
                        comment = comment_stack.popleft()
                        _id = self.mongodb.comments.insert(
                            self.serialize_comment(comment, subreddit, college))
                        comment_ids.append(_id)
                        replies = comment.replies
                        if replies:
                            comment_stack.extend(replies)

                except Exception as e:
                    print e
                mongo_record = serialize_post(post, subreddit, college)
                mongo_record['comments'] = comment_ids
                self.mongodb.posts.insert(mongo_record)
        return
    
    def update(self, college_info, n=30):
        """
        Request the last n posts

        Input:
            subreddit <int>:
            limit:
        """
        return self.reddit.get_subreddit(college_info['subreddit']).get_new(limit=n)

    def _random_name(self, length=10):
        """
        Generate random alphabetical string

        Input:
            length <int> : length of the generated string
        """
        return ''.join(random.choice(string.ascii_letters) for i in range(length))

    def _to_seconds(self, dt):
        """
        Convert a datetime object to seconds

        Input:
            dt <datetime>:
        """
        return int(dt.strftime('%s'))

    def serialize_post(self, submission, college, subreddit):
        """
        Convert a praw post object to a dictionary

        Input:
            submission <praw.post>: praw post object
            college <string> : college name
            subreddit <submission> : subreddit
        """
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
        """
        Convert a praw  comment object to a dictionary
        Input:
            submission <praw.comment> : praw comment object
            college <string> : college name
            subreddit <string> : subreddit name
        """
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
    """ self contained thread object """

    def __init__(self, threadID, reddit_client, q):
        """
        Input:
            threadID: identifier for the thread
            reddit_client <praw.reddit> : praw.reddit object used for communicating with reddit api
            q <Queue> : queue containing college infos
        """
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.reddit_client = reddit_client
        self.q = q
        return

    def run(self):
        """
            Crawl the college retrieved form the queue
        """
        while not self.q.empty():
            college_info = self.q.get_nowait()
            self.reddit_client.crawl(college_info)
    
class MultiThreadedCrawler(object):
    
    def __init__(self, credentials, colleges, start=datetime.now(), end=datetime(2010, 1, 1)):
        """
        Input:
            credentials[] <dict>: array of {'username', 'password'}
            colleges[] <dict>: array of {'name', 'subreddit'}
            start <datetime>: time to start crawling from
            end <datetime>: time to crawl till
        """
        self.credentials = credentials
        self.colleges = colleges
        self.threads = []
        self.start = start
        self.end = end

    def queue_up(self):
        """
        Initialize the queue with the colleges
        """
        q = Queue(maxsize=len(self.colleges))
        for college in self.colleges:
            q.put_nowait(college)

        for credential in self.credentials:
            username , password = credential
            mongodb = pymongo.MongoClient()['reddit']
            client = RedditApiClient(username, password, mongodb, start=self.start, end=self.end)
            self.threads.append(RedditThread(username, client, q))

    def begin(self):
        """
        Activate the threads 
        """
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
