from typing import List, Union, Optional, Callable
import inspect
from collections import Counter
import networkx as nx
from pyperunner.dag.dagascii import draw


class Node:
    """
    Node for use in a direct acyclic graph (DAG)

    Args:
        name: Name of the node (must be unique within DAG).
    """

    def __init__(self, name: str):

        self.name: str = name
        self.children: List[Node] = []
        self.parents: List[Node] = []
        self.dag: Optional[DAG] = None

    def _add_child(self, other: "Node") -> None:
        """
        Add another node as child to this node

        Args:
            other: Other node
        """
        self.children.append(other)

    def _add_parent(self, other: "Node") -> None:
        """
        Add another node as parent to this node

        Args:
            other: Node
        """
        self.parents.append(other)

    def __call__(self, node: Union["Node", List["Node"]]) -> "Node":
        """
        When calling a node with another node, that other node is set as the parent node of this node.

        Args:
            node: Other node (will be parent)

        Returns:
            Self (to allow chaining)

        """
        if not type(node) == list:
            node = [node]  # type: ignore

        for n in node:  # type: ignore
            self.connect_parent(n)

        return self

    def connect_child(self, node: "Node") -> "Node":
        """
        Add another node as child to this node

        Equivalent to calling connect(node, as_child=True).

        Args:
            node: New child node

        Returns:
            Self (to allow chaining)

        """
        return self.connect(node, as_child=True)

    def connect_parent(self, node: "Node") -> "Node":
        """
        Add another node as parent to this node.

        Equivalent to calling connect(node, as_child=False).

        Args:
            node: New parent node

        Returns:
            Self (to allow chaining)

        """
        return self.connect(node, as_child=False)

    def connect(self, node: "Node", as_child: bool) -> "Node":
        """
        Connects another node to this node

        Args:
            node: Other node
            as_child: True if the other node will be a child of this node, False if the other node will be parent

        Returns:
            Self (to allow chaining)
        """
        if not isinstance(node, Node):
            if inspect.isclass(node) and issubclass(node, Node):
                raise ValueError(
                    'Supplied node is not instantiated - did you use "node" instead of "node()"?'
                )
            else:
                raise ValueError("Supplied node is not of type Node")

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
    """
    Directed acyclic graph implementation
    """

    def __init__(self) -> None:
        self.root = Root(self)

    def __call__(self, root_node: "Node") -> "DAG":
        """
        When calling the DAG with a node object, that node object will be added as a root node to the DAG.
        Args:
            root_node: Node to be added as root node

        Returns:
            Self (to allow chaining)

        """
        self.root.connect_child(root_node)

        return self

    def __len__(self) -> int:
        """
        Number of nodes in the DAG

        Returns: Number of nodes in the DAG

        """
        g = self.create_graph()
        return len(g)

    def _add_node(self, g: nx.DiGraph, node: Node) -> None:
        """
        Adds a node to the internal graph representation.

        .. warning:: Not to be used for adding nodes to the DAG, this is only used to build an internal
           representation of the graph.

        Args:
            g: Graph object
            node: Node to add

        """
        g.add_node(node)

        for child in node.children:
            self._add_node(g, child)
            g.add_edge(node, child)

    def create_graph(self) -> nx.DiGraph:
        """
        Creates the internal graph representation of the DAG.

        Returns:
            Graph representation of the DAG
        """
        g = nx.DiGraph()

        for child in self.root.children:
            self._add_node(g, child)

        return g

    def plot_graph(self) -> bytes:
        """
        Creates an image file of the DAG in PNG format.

        Returns:
            PNG image file of the graph
        """
        g = self.create_graph()
        gp = nx.drawing.nx_pydot.to_pydot(g)
        gp.set_simplify(True)
        img = gp.create_png()

        return img

    def is_unique_node(self, node: Node) -> bool:
        """
        Checks if the supplied node is unique within the graph (i.e. asserts that there is no other node in the graph
        with the same name).

        This function is used before adding a new node to assert that the new node can be added.

        Args:
            node: Node to check for uniqueness with respect to the current DAG.

        Returns:
            True if the node is unique w.r.t. the current DAG.
        """
        g = self.create_graph()

        for n in g.nodes:
            if n.name == node.name:
                if n is not node:
                    return False
        return True

    def assert_unique_nodes(self) -> None:
        """
        Assert that all nodes within the current graph are unique (i.e., each unique node must have a unique name;
        there must not be two (or more) different nodes with the same name.

        Raises a ValueError exception if non-unique nodes are encountered.
        This function is used on a completed graph to check its validity (as opposed to :func:`~DAG.is_unique_node()`,
        which is used for checking each new node. assert_unique_nodes() is required because during chaining operations
        (using the functional API to connect nodes), the .dag property of each node is not necessarily set to the
        enclosing DAG, thus it cannot be known whether the node is unique at that time.

        """
        g = self.create_graph()
        cnt = Counter([node.name for node in g.nodes])
        multiple = [k for k in cnt if cnt[k] > 1]

        if multiple:
            raise ValueError(
                f"Node names must be unique - multiple nodes with the same name found: {multiple}"
            )

    def assert_acyclic(self) -> None:
        """
        Asserts that the current DAG is indeed acyclic.
        """
        g = self.create_graph()
        if not nx.is_directed_acyclic_graph(g):
            cycle = nx.find_cycle(g)
            cycle = [(n0.name, n1.name) for n0, n1 in cycle]
            raise ValueError(f"Graph is not acyclic, please remove cycle ({cycle})")

    def summary(self, print_fn: Callable = None) -> None:
        """
        Prints an ASCII summary of the DAG.

        Args:
            print_fn: Print function used to output the ascii representation. Default: print()

        Returns:

        """
        g = self.create_graph()
        s = draw([n.name for n in g.nodes], [(t.name, s.name) for s, t in g.edges])

        if print_fn is None:
            print_fn = print

        print_fn(s)


class Root(Node):
    """
    Root node implementation for DAG.

    Args:
        dag: The DAG for which this node is the root node.
    """

    def __init__(self, dag: DAG):
        super().__init__("root")
        self.dag = dag
