import pymongo
import random

mongodb = pymongo.MongoClient()['reddit']

def query(college, start, end):
    return mongodb.posts.find({
        'subreddit': college,
        'created_utc': {'$lte': start, '$gte': end}
        })

def populate_comments(post):
    comments = []
    for comment in post['comments']:
        comments.append(mongodb.comments.find_one({'_id': comment}))
    return comments

def join_comments(posts):
    comments = []
    for post in posts:
        comments.extend(populate_comments(post))
    return posts + comments