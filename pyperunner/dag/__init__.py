from typing import List, Union, Optional, Callable
from collections import Counter
import networkx as nx
from .dagascii import draw


class Node:
    def __init__(self, name: str):
        self.name: str = name
        self.children: List[Node] = []
        self.parents: List[Node] = []
        self.dag: Optional[DAG] = None

    def _add_child(self, other: "Node") -> None:
        self.children.append(other)

    def _add_parent(self, other: "Node") -> None:
        self.parents.append(other)

    def __call__(self, node: Union["Node", List["Node"]]) -> "Node":
        if not type(node) == list:
            node = [node]  # type: ignore

        for n in node:  # type: ignore
            self.connect_parent(n)

        return self

    def connect_child(self, node: "Node") -> "Node":
        return self.connect(node, as_child=True)

    def connect_parent(self, node: "Node") -> "Node":
        return self.connect(node, as_child=False)

    def connect(self, node: "Node", as_child: bool) -> "Node":
        if self.dag is not None and not self.dag.is_unique_node(node):
            raise ValueError(f"Node names must be unique, '{node.name}' already exists")

        if as_child:
            parent, child = self, node
        else:
            parent, child = node, self

        child._add_parent(parent)
        parent._add_child(child)

        node.dag = self.dag

        return self

    def __str__(self) -> str:
        return self.name


class DAG:
    def __init__(self) -> None:
        self.root = Root(self)

    def __call__(self, x: "Node") -> "Node":
        self.root.connect_child(x)
        return x

    def _add_node(self, g: nx.DiGraph, node: Node) -> None:
        g.add_node(node)

        for child in node.children:
            self._add_node(g, child)
            g.add_edge(node, child)

    def create_graph(self) -> nx.DiGraph:
        g = nx.DiGraph()

        for child in self.root.children:
            self._add_node(g, child)

        return g

    def plot_graph(self) -> bytes:
        g = self.create_graph()
        gp = nx.drawing.nx_pydot.to_pydot(g)
        gp.set_simplify(True)
        img = gp.create_png()

        return img

    def is_unique_node(self, node: Node) -> bool:
        g = self.create_graph()

        for n in g.nodes:
            if n.name == node.name:
                if n is not node:
                    return False
        return True

    def assert_unique_nodes(self) -> None:
        g = self.create_graph()
        cnt = Counter([node.name for node in g.nodes])
        multiple = [k for k in cnt if cnt[k] > 1]

        if multiple:
            raise ValueError(
                f"Node names must be unique - multiple nodes with the same name found: {multiple}"
            )

    def assert_acyclic(self) -> None:
        g = self.create_graph()
        if not nx.is_directed_acyclic_graph(g):
            cycle = nx.find_cycle(g)
            cycle = [(n0.name, n1.name) for n0, n1 in cycle]
            raise ValueError(f"Graph is not acyclic, please remove cycle ({cycle})")

    def summary(self, print_fn: Callable = None) -> None:
        g = self.create_graph()
        s = draw([n.name for n in g.nodes], [(t.name, s.name) for s, t in g.edges])

        if print_fn is None:
            print_fn = print

        print_fn(s)


class Root(Node):
    def __init__(self, dag: DAG):
        super().__init__("root")
        self.dag = dag
