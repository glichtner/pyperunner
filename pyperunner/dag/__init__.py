from typing import List

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
        if not self.dag.is_unique_node(node):
            raise ValueError(f"Node names must be unique, '{node.name}' already exists")
        self.add_child(node)
        node.add_parent(self)

        node.dag = self.dag

        return node

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


class Root(Node):
    def __init__(self, dag: DAG):
        super().__init__("root")
        self.dag = dag
