import unittest
from analysis import SankeyFormatter, db

class SankeyTest(unittest.TestCase):

    def setUp(self): 
        self.formatter = SankeyFormatter(db, ['Georgia Tech', 'Purdue'], 'CS')
        self.text = self.formatter.search()

    def test_build_regex(self):
        regex = self.formatter.build_regex('CS 1332')
        # self.assertEqual(regex, '<CS> <1332> <.*>')

    def test_concordance(self):
        concs = []
        for college in self.text:
            for post in college['posts']:
                bigrams = self.formatter.concordance(post)
                concs.extend(bigrams)
        print [w[1] for w in concs]

    def test_count(self):
        concs = []
        for college in self.text:
            for post in college['posts']:
                bigrams = self.formatter.concordance(post)
                concs.extend(bigrams)
        print self.formatter.count(w[1] for w in concs)

if __name__ == '__main__': 
    unittest.main()