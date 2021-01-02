from typing import List
from collections import Counter
import networkx as nx


class Node:
    def __init__(self, name):
        self.name: str = name
        self.children: List[Node] = []
        self.parents: List[Node] = []
        self.dag: DAG = None

    def add_child(self, other):
        self.children.append(other)

    def add_parent(self, other):
        self.parents.append(other)

    def __call__(self, node: "Node"):
        if self.dag is not None and not self.dag.is_unique_node(node):
            raise ValueError(f"Node names must be unique, '{node.name}' already exists")
        self.add_child(node)
        node.add_parent(self)

        node.dag = self.dag

        return self

    def __str__(self):
        return self.name


class DAG:
    def __init__(self):
        self.root = Root(self)

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

    def is_unique_node(self, node):
        G = self.create_graph()

        for n in G.nodes:
            if n.name == node.name:
                if n is not node:
                    return False
        return True

    def assert_unique_nodes(self):
        g = self.create_graph()
        cnt = Counter([node.name for node in g.nodes])
        multiple = [k for k in cnt if cnt[k] > 1]

        if multiple:
            raise ValueError(
                f"Node names must be unique - multiple nodes with the same name found: {multiple}"
            )

    def assert_acyclic(self):
        g = self.create_graph()
        if not nx.is_directed_acyclic_graph(g):
            cycle = nx.find_cycle(g)
            cycle = [(n0.name, n1.name) for n0, n1 in cycle]
            raise ValueError(f"Graph is not acyclic, please remove cycle ({cycle})")


class Root(Node):
    def __init__(self, dag: DAG):
        super().__init__("root")
        self.dag = dag
