
import re
import nltk
import string
import pymongo
from nltk.stem.porter import PorterStemmer
from tree import SuffixTree
from sklearn import feature_extraction

db = pymongo.MongoClient()['reddit']

def search(college, term, limit=30):
    return db.posts.find({
            '$text': {'$search': term},
            'college': college
    }).limit(limit)

def recent(college, collection, limit=30):
    return db[collection].find({
        'college': college,
        }).sort('created_utc', pymongo.DESCENDING).limit(limit)

def query_college(college): 
    return db.posts.find({'college': college})

class SankeyFormatter(object):

    def __init__(self, db, colleges, term, tree=None):
        self.db = db
        self.colleges = colleges
        self.term = term
        self.tree = tree or SuffixTree(self.term)
        return

    def search(self):
        post_pipe = [
            {'$match': {
                '$text': {'$search': self.term},
                'college': { '$in': self.colleges}
            }},
            {'$limit': 4}
            ,

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
            {'$limit': 4},
            {'$group': {
                '_id': '$college', 
                'count': {'$sum': 1},
                'posts': {'$push': '$body'}
            }},
        ]

        return (self.db.post.aggregate(pipeline=post_pipe)['result'] +
            self.db.comments.aggregate(pipeline=comment_pipe)['result'])


    def _process(self, search_results):
        for college in search_results:
            relevent = []
            for post in college['posts']:
                exp = '%s (.*)' % format(self.term)
                relevent.extend(re.findall(exp, post))
            college['relevent'] = relevent
        return search_results  

    def json(self, processed):
        for college in processed:
            for sent in college['relevent']:
                self.tree.insert(sent.split(' '))
        return self.tree.json()
        

class KeywordExtractor(object):

    def __init__(self, stopwords=nltk.corpus.stopwords.words('english'), 
                get_text=lambda x: x):
        """
        Input:
            stopwords list<str>: list of terms to ignore
            get_text <function>: text accessor function to retrieve strings.

        """
        self.stopwords = set(stopwords)
        self.tfidf_transformer = feature_extraction.text.TfidfTransformer()
        self.cv = feature_extraction.text.CountVectorizer(stop_words=stopwords, ngram_range=(1,1))
        self.get_text = get_text
        self.vocabulary_keys = None
        self.vocabulary_values = None
        return

    def _count_vectorize(self, documents):
        """

        Input:
            documents list<str>: list of documents

        Returns:

        """
        return self.cv.fit_transform(documents)
      

    def _fit_transform(self, vectors):
        """

        Input:
            vectors:

        Returns:

        """
        return self.tfidf_transformer.fit_transform(vectors)
        

    def compute(self, documents):
        """

        Input: 
            documents list<T> : list of documents

        Returns:

        """
        word_counts = self._count_vectorize(
            [self.get_text(document) for document in documents])
        self.vocabulary_keys = self.cv.vocabulary_.keys()
        self.vocabulary_values = self.cv.vocabulary_.values()
        return self._fit_transform(word_counts).toarray()


    def extract(self, documents, threshold=0.25):
        """
        Input:
            documents list<T> : list of documents
            threshold <int> : value to filter out relevant terms
            post_process <function> : function to perform once the relevant
                terms have been identified. Expects (document, terms)
        """
        tfidf_vectors = self.compute(documents)
        for i, document in enumerate(tfidf_vectors):
            terms = []
            for j, score in enumerate(document):
                if score > threshold:
                    term = self.vocabulary_keys[self.vocabulary_values.index(j)]
                    terms.append(term)
            
