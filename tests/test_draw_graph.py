import networkx as nx

from pyperunner.dag import draw


def test_draw_dag_ascii_sequential():
    g = nx.DiGraph()
    g.add_edges_from([("a", "b"), ("b", "c"), ("c", "d"), ("c", "d")])

    s = draw(g.nodes, g.edges)
    assert (
        s == "+---+  \n"
        "| d |  \n"
        "+---+  \n"
        "  *    \n"
        "  *    \n"
        "  *    \n"
        "+---+  \n"
        "| c |  \n"
        "+---+  \n"
        "  *    \n"
        "  *    \n"
        "  *    \n"
        "+---+  \n"
        "| b |  \n"
        "+---+  \n"
        "  *    \n"
        "  *    \n"
        "  *    \n"
        "+---+  \n"
        "| a |  \n"
        "+---+  "
    )


def test_draw_dag_ascii_branched():
    s = draw(["b", "d", "c", "a"], [("b", "a"), ("c", "b"), ("d", "c"), ("d", "b")])
    assert (
        s == "     +---+    \n"
        "     | a |    \n"
        "     +---+    \n"
        "        *     \n"
        "        *     \n"
        "        *     \n"
        "     +---+    \n"
        "     | b |    \n"
        "     +---+    \n"
        "     *    *   \n"
        "    *     *   \n"
        "   *       *  \n"
        "+---+       * \n"
        "| c |      *  \n"
        "+---+     *   \n"
        "     *    *   \n"
        "      *  *    \n"
        "       **     \n"
        "     +---+    \n"
        "     | d |    \n"
        "     +---+    "
    )
