import nltk
from collections import Counter
from crawler import db

def search(college, term):
    return db.posts.find({
            '$text': {'$search': term},
            'college': college
    })

def query_college(college): 
    return db.posts.find({'college': college})

class SankeyFormatter(object):

    def __init__(self, db, colleges, term):
        self.db = db
        self.colleges = colleges
        self.term = term
        return

    def search(self):
        post_pipe = [
            {'$match': {
                '$text': {'$search': self.term},
                'college': { '$in': self.colleges}
            }},

            {'$group': {
                '_id': '$college', 
                'count': {'$sum': 1},
                'posts': {'$push': '$text'}
            }},
        ]
        comment_pipe = [
            {'$match': {
                '$text': {'$search': self.term},
                'college': { '$in': self.colleges}
            }},

            {'$group': {
                '_id': '$college', 
                'count': {'$sum': 1},
                'posts': {'$push': '$body'}
            }},
        ]

        return (self.db.post.aggregate(pipeline=post_pipe)['result'] +
            self.db.comments.aggregate(pipeline=comment_pipe)['result'])

    def concordance(self, document):
        tokens = nltk.word_tokenize(document)
        token_searcher = nltk.TokenSearcher(token for token in tokens 
                                            if token.isalnum())
        regexp = self.build_regex(self.term)
        return token_searcher.findall(regexp)

    def build_regex(self, search_query):
        regex = r' '.join('<{word}>'.format(word=word) for word in search_query.split(' '))
        return regex + ' <.*>'

    def count(self, bigrams):
        counter = Counter()
        for word in bigrams:
            counter[word] += 1
        return counter
        