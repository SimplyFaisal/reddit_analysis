import pymongo
import praw
import random
import threading
import string
import binascii
import bson
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
            print college, str(lower.date()), str(upper.date())

            start_seconds = self._to_seconds(upper)
            end_seconds = self._to_seconds(lower)
            upper = lower
            lower = lower - self.interval

            # if the  lower bound is past the beginning of our range just set 
            # the lower bound equal to the beginning of the range

            if lower < self.end:
                lower = self.end
            query = self.CLOUD_QUERY.format(start=start_seconds, end=end_seconds)
            posts = self.reddit.search(query, subreddit=subreddit, 
                sort=sort, limit=None,syntax=self.CLOUD_SYNTAX)
            for post in posts:
                mongo_record = self.serialize_post(post, subreddit, college)
                try:
                    comments = self.process_comments(post, subreddit, college)
                    if comments:
                        # we could just batch insert them but the multithreading
                        # causes a infrequent duplicate key errors. An alternative
                        # would be to key the comments by their reddit id, but
                        # I don't know how unique they are
                        comment_ids = self.mongodb.comments.insert(comments)
                        mongo_record['comments'] = comment_ids
                except Exception as e:
                    print e
                finally:
                    self.mongodb.posts.insert(mongo_record)
       
        print 'Finished {}'.format(college)            
        return

    def process_comments(self, post, subreddit, college):
        """
        Retrieve the comments of a post

        Input:
            post <praw.post> : a praw post object 
            subreddit <string> : the subreddit to crawl
            college <string> : the college name
        """
        comments = []
        post.replace_more_comments(limit=None, threshold=0)
        comment_stack = deque(post.comments)
        while len(comment_stack):
            comment = comment_stack.popleft()
            replies = comment.replies
            comment = self.serialize_comment(comment, subreddit, college)
            comments.append(comment)
            if replies:
                comment_stack.extend(replies)
        return comments
    
    def update(self, college_info, n=30):
        """
        Request the last n posts

        Input:
            subreddit <int>:
            n <int>: limit
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
            '_id': self.create_object_id(submission.id),
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
            '_id': self.create_object_id(comment.id),
            'text': comment.body, 
            'ups' : comment.ups, 
            'downs': comment.downs,
            'college': college, 
            'subreddit' : subreddit,
            'created_utc': datetime.utcfromtimestamp(comment.created_utc)
        }

    def create_object_id(self, s):
        return bson.ObjectId(binascii.hexlify(s).zfill(24))
       

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
        print '{} started'.format(self.threadID)
        while not self.q.empty():
            college_info = self.q.get_nowait()
            print '{} aqcuired {}'.format(self.threadID, college_info['name'])
            self.reddit_client.crawl(college_info)
    
class MultiThreadedCrawler(object):
    
    def __init__(self, credentials, colleges, start=datetime.now(), end=datetime(2013, 1, 1)):
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
        self.mongodb_client = pymongo.MongoClient()['reddit']

    def _queue_up(self):
        """
        Initialize the queue with the colleges
        """
        q = Queue(maxsize=len(self.colleges))
        for college in self.colleges:
            q.put_nowait(college)

        for credential in self.credentials:
            username , password = credential
            mongodb = pymongo.MongoClient()['reddit']
            client = RedditApiClient(username, password, self.mongodb_client, start=self.start, end=self.end)
            self.threads.append(RedditThread(username, client, q))

    def begin(self):
        """
        Activate the threads 
        """
        self._queue_up()
        print 'Starting the threads'
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

if __name__ == '__main__':
    multi = MultiThreadedCrawler(CREDENTIALS, SUBREDDITS)
    multi.begin()
