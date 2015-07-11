from collections import deque
from random import random

class SuffixTree(object):

    def __init__(self, root_word):
        self.root_word = root_word
        self.root = Node(root_word)

    def insert(self, tokens):
        q = deque(tokens)
        self._traverse(self.root, q)

    def _traverse(self, node, q):
        if not q:
            return 
        term = q.popleft()
        if term == node.term:
            self._traverse(node, q)
            return
        if term not in node._set:
            new_node = Node(term, parent=node)
            node._set.add(term)
            node.nodes.append(new_node)
            self._traverse(new_node, q)
        else:
            for n in node.nodes:
                if term == n.term:
                    self._traverse(n, q)
        self._traverse(node, q)

    def json(self):
        nodes = []
        edges = []
        location = {}
        self._json_traverse(self.root, nodes, edges, location, 0)
        return {'nodes': nodes, 'edges' : edges}

    def _json_traverse(self, node, nodes, edges, location, depth):
        n = {'term': node.term,
            'depth': depth}
        nodes.append(n)
        location[node.term] = len(nodes) - 1
        depth += 1
        if node.parent:
            e = {'source': location[node.parent.term], 
                'target': location[node.term],
                'weight': len(node.nodes)
                }
            edges.append(e)
        for child in node.nodes:
            child.depth = depth
            self._json_traverse(child, nodes, edges, location, depth)
        return 


class Node(object):

    def __init__(self, term, parent=None):
        self._set = set()
        self.term = term
        self.nodes = []
        self.parent = parent
        self.depth = 0

    def __repr__(self):
        return '<Node %s>' % self.term
