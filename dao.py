import pymongo
import random

mongodb = pymongo.MongoClient()['reddit']

def query(college, start, end):
    return mongodb.posts.find({
        'subreddit': college,
        'created_utc': {'$lte': end, '$gte': start}
        })

def distinct_colleges():
    return list(mongodb.posts.distinct('subreddit'))

def populate_comments(post):
    comments = []
    for comment in post['comments']:
        result = mongodb.comments.find_one({'_id': comment})
        result['color'] = 'blue'
        comments.append(result)

    return comments

def join_comments(posts):
    comments = []
    for post in posts:
        comments.extend(populate_comments(post))
    return posts + comments

def get_post(_id):
    return mongodb.posts.find_one({'_id': _id})

def get_comment(_id):
    return mongodb.comments.find_one({'_id': _id})

