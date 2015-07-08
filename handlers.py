import dao
import analysis
from datetime import datetime, timedelta

def create_graph_handler(college, day, threshold):
    start = dt_from_timestamp(day)
    end = start - timedelta(hours=24)
    posts = list(dao.query(college, start, end))
    corpus = dao.join_comments(posts)
    return cosine_graph(corpus, threshold)

def cosine_graph(corpus, threshold):
    keyword_extractor = analysis.KeywordExtractor(get_text=lambda x: x['text'])
    vectors = keyword_extractor.compute(corpus)
    nodes = [{'title': doc['text']} for doc in corpus]
    edges = []
    angles = []
    for i, v in enumerate(vectors):
        for j, k in enumerate(vectors):
            if i == j:
                continue
            else:
                angle = v.dot(k)
                angles.append(angle)
                if angle >= threshold:
                    edge = {'source': i, 'target': j}
                    edges.append(edge)
    _max = max(angles)
    return {'nodes': nodes, 'edges': edges}


def dt_from_timestamp(s):
    return datetime.strptime(s, '%a %b %d %Y')