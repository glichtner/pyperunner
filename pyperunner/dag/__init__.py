import networkx as nx
import hashlib


class Node:
    def __init__(self, name):
        self.name = name
        self.children = []
        self.parents = []

    def add_child(self, other):
        self.children.append(other)

    def add_parent(self, other):
        self.parents.append(other)

    def __call__(self, x):
        self.add_child(x)
        x.add_parent(self)

        return x

    def __str__(self):
        return self.name


class DAG:
    def __init__(self):
        self.root = Root()

    def __call__(self, x):
        self.root(x)
        return x

    def _add_node(self, G: nx.DiGraph, node: Node):
        G.add_node(node)

        for child in node.children:
            self._add_node(G, child)
            G.add_edge(node, child)

    def create_graph(self):
        G = nx.DiGraph()

        for child in self.root.children:
            self._add_node(G, child)
        return G

    def plot_graph(self):
        G = self.create_graph()
        gp = nx.drawing.nx_pydot.to_pydot(G)
        gp.set_simplify(True)
        img = gp.create_png()
        return img


class Root(Node):
    def __init__(self):
        super().__init__("root")

    def _hash(self):
        return [hashlib.md5("root".encode("utf-8")).hexdigest()]
