import pymongo
import praw
from datetime import datetime
from config import PRAW_PASSWORD, PRAW_USERNAME, SUBREDDITS


db = pymongo.MongoClient()['reddit']

def main():
    reddit = praw.Reddit('PRAW Gatech subreddit monitor')
    reddit.login(PRAW_USERNAME, PRAW_PASSWORD)
    print 'Logged In'
    for school, subreddit in SUBREDDITS.iteritems():
        posts = reddit.get_subreddit(subreddit)
        params = {'sort':'new', 't':'year'}
        crawl_subreddit(posts.get_new(params=params, limit=300), school, subreddit)
    return

def crawl_subreddit(posts, college , subreddit):
    c = 0
    p = 0
    for submission in posts:
        p += 1
        record = serialize_post(submission, subreddit, college)
        try:
            submission.replace_more_comments(limit=None, threshold=0)
            comments = praw.helpers.flatten_tree(submission.comments)
            for comment in comments:
                c += 1
                document = serialize_comment(comment, subreddit, college)
                _id = db.comments.insert(document)
                print _id
                record['comments'].append(_id)
        except Exception as error:  
            print error
        db.posts.insert(record)
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
            'body': comment.body, 
            'ups' : comment.ups, 
            'downs': comment.downs,
            'college': college, 
            'subreddit' : subreddit,
            'created_utc': datetime.utcfromtimestamp(comment.created_utc)
        }
if __name__ == '__main__':
    main()