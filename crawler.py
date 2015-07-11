import pymongo
import praw
import random
import threading
import string
import binascii
import bson
import sys
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
    
    def crawl(self, college_info, limit=None, start=None, end=None, sort='new'):
        """
        Crawl the specified subreddit in the range (self.start, self.end)

        Input:
            college_info <dict>: { 'name': name of the college, 'subreddit': name of the subreddit}
            limit <int> : max number of posts to return per call
            sort <string> : sort order of the returned posts
        """
        college, subreddit = college_info['name'], college_info['subreddit']
        upper = self.start or start
        lower = upper - self.interval
        end = end or self.end
        while lower > end:
            print college, str(lower.date()), str(upper.date())

            start_seconds = self._to_seconds(upper)
            end_seconds = self._to_seconds(lower)
            upper = lower
            lower = lower - self.interval

            # if the  lower bound is past the beginning of our range just set 
            # the lower bound equal to the beginning of the range

            if lower < end:
                lower = end
            query = self.CLOUD_QUERY.format(start=start_seconds, end=end_seconds)
            posts = self.reddit.search(query, subreddit=subreddit, 
                sort=sort, limit=None,syntax=self.CLOUD_SYNTAX)
            for post in posts:
                mongo_record = self.serialize_post(post, subreddit, college)
                try:
                    comments = self.process_comments(post, subreddit, college)
                    if comments:
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
        Crawls the requested subreddit between now and the date of the last post
        that appears in the database.

        Input:
            subreddit <int>:
            n <int>: limit
        """
        start = datetime.now()
        end = self.last_post_date(college_info)
        if end:
            self.crawl(college_info, start=start, end=end)
        else:
            self.crawl(college_info)

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

    def last_post_date(self, college_info):
        """
        Returns the date of the last post crawled for the request school

        Input:
            college_info <dict>: { 'name': name of the college, 'subreddit': name of the subreddit}

        Returns: A datetime object
        """
        college = college_info['name']
        print college
        last_post_query = self.mongodb.posts.find({'college': college}).sort('created_utc', 
                        pymongo.DESCENDING).limit(1)
        try:
            last_post_date = list(last_post_query)[0]['created_utc']
            # Add a ten second difference to insure that we don't crawl the post
            # again.
            return last_post_date + timedelta(seconds=10)
        except IndexError as e:
            return False

       

class RedditThread(threading.Thread):
    """ self contained thread object """

    def __init__(self, threadID, reddit_client, q, update_mode=True):
        """
        Input:
            threadID: identifier for the thread
            reddit_client <praw.reddit> : praw.reddit object used for
                communicating with reddit api
            q <Queue> : queue containing college infos
        """
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.reddit_client = reddit_client
        self.q = q
        self.update_mode = update_mode
        return

    def run(self):
        """
            Crawl the college retrieved form the queue
        """
        print '{} started'.format(self.threadID)
        while not self.q.empty():
            college_info = self.q.get_nowait()
            print '{} aqcuired {}'.format(self.threadID, college_info['name'])
            if self.update_mode:
                self.reddit_client.update(college_info)
            else:
                self.reddit_client.crawl(college_info)


    
class MultiThreadedCrawler(object):
    
    def __init__(self, credentials, colleges, start=datetime.now(), 
            end=datetime(2013, 1, 1), update_mode=True):
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
        self.update_mode = update_mode

    def initialize(self):
        """
        Initialize the queue with the colleges
        """
        q = Queue(maxsize=len(self.colleges))
        for college in self.colleges:
            q.put_nowait(college)

        for credential in self.credentials:
            username , password = credential
            mongodb = pymongo.MongoClient()['reddit']
            client = RedditApiClient(username, password, self.mongodb_client, 
                start=self.start, end=self.end)
            self.threads.append(RedditThread(username, client, q, 
                update_mode=self.update_mode))

    def begin(self):
        """
        Activate the threads 
        """
        self.initialize()
        print 'Starting the threads'
        for thread in self.threads:
            thread.start()


if __name__ == '__main__':
    update_mode = '-u' in sys.argv
    multi = MultiThreadedCrawler(CREDENTIALS, SUBREDDITS, update_mode=update_mode)
    multi.begin()
