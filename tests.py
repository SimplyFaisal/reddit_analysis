import unittest
from analysis import recent, KeywordExtractor, db
from tree import SuffixTree
import nltk
from collections import Counter

# class SankeyTest(unittest.TestCase):

#     def setUp(self): 
#         self.formatter = SankeyFormatter(db, ['Georgia Tech', 'Purdue'], 'CS')
#         self.text = self.formatter.search()

#     def test_process(self):
#         for r in self.formatter._process(self.text):
#             print r['relevent']


# class SuffixTreeTest(unittest.TestCase):

#     def setUp(self):
#         self.docs = ['CS is hard', 'CS is easy', 'CS is fun']
#         self.tree = SuffixTree('CS')
#         self.tokens = [sent.split(' ') for sent in self.docs]

#     def test_insert(self):
#         for t in self.tokens:
#             self.tree.insert(t)

#     def test_json(self):
#         for t in self.tokens:
#             self.tree.insert(t)
#         graph = self.tree.json()
#         print graph

class KeywordTests(unittest.TestCase):

    def setUp(self):
        posts =  [doc for doc in recent('gatech','posts', limit=10)]
        comments = []
        for p in posts:
            for c in p['comments']:
                comment = db.comments.find_one({'_id': c})
                comments.append(comment)
        self.documents = posts + comments
        self.documents = [d for d in self.documents]
        print len(self.documents)
        self.keywords = KeywordExtractor(get_text=lambda x: x['text'])

    def test_extract(self):
        vectors = self.keywords.compute(self.documents)
        freqdist  = []
        for document, vector in zip(self.documents, vectors):
            terms = []
            for i, score in enumerate(vector):
                if score > 0.3:
                    term = self.keywords.vocabulary_keys[self.keywords.vocabulary_values.index(i)]
                    terms.append(term)
            freqdist.extend(terms)
            # print document['text']
            # print '--------'
            # print ','.join(terms)
        # cnt = Counter(freqdist)
        # for word, val in cnt.most_common(50):
        #     print word, val
        

if __name__ == '__main__': 
    unittest.main()