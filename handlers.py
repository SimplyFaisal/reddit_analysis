import dao
import analysis
from datetime import datetime

def create_graph_handler(college, start, end, threshold):
    start = dt_from_timestamp(start)
    end = dt_from_timestamp(end)
    posts = list(dao.query(college, start, end))
    for post in posts:
        post['color'] = 'red'
    corpus = dao.join_comments(posts)
    print len(corpus)
    return cosine_graph(corpus, threshold)

def cosine_graph(corpus, threshold):
    keyword_extractor = analysis.KeywordExtractor(get_text=lambda x: x['text'])
    vectors = keyword_extractor.compute(corpus)
    nodes = [{'title': doc['text'], 'color': doc['color']} for doc in corpus]
    edges = []
    for i, v in enumerate(vectors):
        for j, k in enumerate(vectors):
            if i == j:
                continue
            else:
                angle = v.dot(k)
                if angle >= threshold:
                    edge = {'source': i, 'target': j}
                    edges.append(edge)
    return {'nodes': nodes, 'edges': edges}

def get_colleges_handler():
    return dao.distinct_colleges()

def get_post_handler(_id):
    return dao.get_post(_id)

def get_comment_handler(_id):
    return dao.get_comment(_id)

def get_comments_handler(_id):
    post = dao.get_post(_id)
    return dao.populate_comments(post)

def dt_from_timestamp(s):
    return datetime.strptime(s, '%b %d %Y')