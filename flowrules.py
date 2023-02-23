import argparse
import pydot

from enum import Enum


class PropertyVal(Enum):
    YES = 1
    NO = 2
    PRESERVE = 3
    CODE_BLOCK = 4


op_props_table = {
    'difference': {'monotone': PropertyVal.NO, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.YES},
    'sort': {'monotone': PropertyVal.NO, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.YES},
    'sort_by': {'monotone': PropertyVal.NO, 'deterministic': PropertyVal.CODE_BLOCK, 'stateful': PropertyVal.YES},
    'demux': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO},
    'for_each': {'monotone': PropertyVal.CODE_BLOCK, 'deterministic': PropertyVal.CODE_BLOCK, 'stateful': PropertyVal.NO},
    'handoff': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO},
    'identity': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO},
    'inspect': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO},
    'unzip': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO},
    'unique': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.YES},
    'merge': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO},
    'tee': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO},
    'join': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.YES},
    'cross_join': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.YES},
    'map': {'monotone': PropertyVal.CODE_BLOCK, 'deterministic': PropertyVal.CODE_BLOCK, 'stateful': PropertyVal.NO},
    'filter': {'monotone': PropertyVal.CODE_BLOCK, 'deterministic': PropertyVal.CODE_BLOCK, 'stateful': PropertyVal.NO},
    'flat_map': {'monotone': PropertyVal.CODE_BLOCK, 'deterministic': PropertyVal.CODE_BLOCK, 'stateful': PropertyVal.NO},
    'flatten': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO},
    'filter_map': {'monotone': PropertyVal.CODE_BLOCK, 'deterministic': PropertyVal.CODE_BLOCK, 'stateful': PropertyVal.NO},
    'next_tick': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO},
    'next_stratum': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO},
    'group_by': {'monotone': PropertyVal.CODE_BLOCK, 'deterministic': PropertyVal.CODE_BLOCK, 'stateful': PropertyVal.YES},
    'reduce': {'monotone': PropertyVal.CODE_BLOCK, 'deterministic': PropertyVal.CODE_BLOCK, 'stateful': PropertyVal.YES},
    'fold': {'monotone': PropertyVal.CODE_BLOCK, 'deterministic': PropertyVal.CODE_BLOCK, 'stateful': PropertyVal.YES},
    'null': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO},
    'source_stream': {'monotone': PropertyVal.YES, 'deterministic': PropertyVal.NO, 'stateful': PropertyVal.NO},
    'source_stream_serde': {'monotone': PropertyVal.YES, 'deterministic': PropertyVal.NO, 'stateful': PropertyVal.NO},
    'source_iter': {'monotone': PropertyVal.YES, 'deterministic': PropertyVal.YES, 'stateful': PropertyVal.NO},
    'source_stdin': {'monotone': PropertyVal.YES, 'deterministic': PropertyVal.NO, 'stateful': PropertyVal.NO},
    'dest_sink': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO},
    'dest_sink_serde': {'monotone': PropertyVal.PRESERVE, 'deterministic': PropertyVal.PRESERVE, 'stateful': PropertyVal.NO}
}


class Vertex:
    def __init__(self, name, properties):
        self.name = name
        self.properties = properties


class Edge:
    def __init__(self, source, destination, properties):
        self.source = source
        self.destination = destination
        self.properties = properties


class Graph:
    def __init__(self):
        self.vertices = {}
        self.edges = []
        self.outbounds = {}
        self.inbounds = {}
        self.edge_ix = {}

    def add_vertex(self, vertex):
        if not isinstance(vertex, Vertex):
            raise ValueError("vertex should be of type Vertex")
        self.vertices[vertex.name] = vertex

    def add_edge(self, source, destination, properties):
        if source not in self.vertices:
            raise ValueError("source vertex not found in the graph")
        if destination not in self.vertices:
            raise ValueError("destination vertex not found in the graph")
        new_edge = Edge(source, destination, properties)
        self.edges.append(new_edge)
        self.edge_ix[(source, destination)] = new_edge
        self.outbounds.setdefault(self.vertices[source], []).append(new_edge)
        self.inbounds.setdefault(
            self.vertices[destination], []).append(new_edge)


def extract_operator(string):
    start = string.index(')') + 1
    try:
        end = string.index('(', start)
        return string[start:end].strip().strip('"')
    except ValueError:
        return string[start:].strip().strip('"')


def parse_graph(graph):
    my_graph = Graph()
    for s in graph.get_subgraphs():
        for n in s.get_nodes():
            my_graph.add_vertex(
                Vertex(n.get_name().strip(), {"op": extract_operator(n.get_label())}))
            if extract_operator(n.get_label()) == 'handoff"':
                print("%s" % n.get_label())
        for e in s.get_edges():
            my_graph.add_edge(e.get_source(), e.get_destination(), {})
            print("%s -> %s" % (e.get_source(), e.get_destination()))
    for n in graph.get_nodes():
        my_graph.add_vertex(
            Vertex(n.get_name().strip(), {"op": extract_operator(n.get_label())}))
        if extract_operator(n.get_label()) == 'handoff"':
            print("%s" % n.get_label())

    for e in graph.get_edges():
        my_graph.add_edge(e.get_source(), e.get_destination(), {})
        print("%s -> %s" % (e.get_source(), e.get_destination()))
    return my_graph


def monotone(op_entry, monotone_block):
    if op_entry['monotone'] == PropertyVal.CODE_BLOCK:
        if monotone_block == PropertyVal.YES:
            return PropertyVal.PRESERVE
        else:
            return PropertyVal.NO
    else:
        return op_entry['monotone']


def deterministic(op_entry, deterministic_block):
    if op_entry['deterministic'] == PropertyVal.CODE_BLOCK:
        if deterministic_block == PropertyVal.YES:
            return PropertyVal.PRESERVE
        else:
            return PropertyVal.NO
    else:
        return op_entry['deterministic']


def lifetime(op_name):
    try:
        return op_name.split('< ')[1].split(' >')[0]
    except IndexError:
        return None


def op_props(op_name, monotonic_block=PropertyVal.NO, deterministic_block=PropertyVal.NO):
    op = op_name.split(' ')[0]
    retval = {}
    try:
        op_entry = op_props_table[op]
    except KeyError:
        raise ValueError("Unknown operator: (%s)" % op)

    # EXPERIMENT/HACK let's assume map, group_by and for_each are monotone and deterministic
    if op == 'map' or op == 'filter' or op == 'filter_map' or op == 'flat_map' or op == 'fold' or op == 'for_each':
        monotonic_block = PropertyVal.YES
        deterministic_block = PropertyVal.YES

    # op monotonicity
    if op_entry['monotone'] == PropertyVal.NO or op_entry['monotone'] == PropertyVal.YES:
        # already determined
        retval['monotone'] = op_entry['monotone']
    elif op_entry['monotone'] == PropertyVal.CODE_BLOCK and monotonic_block == PropertyVal.NO:
        # non-monotonic code block
        retval['monotone'] = PropertyVal.NO
    # by here, op_entry['monotone'] must effectively be PropertyVal.PRESERVE
    elif op_entry['stateful'] == PropertyVal.YES:
        # monotonic iff all input lifetimes are non-tick (the default)
        lt = lifetime(op_name)
        if (lt is None or not 'tick' in lt) and monotone(op_entry, monotonic_block) != PropertyVal.NO:
            retval['monotone'] = PropertyVal.YES
        else:
            retval['monotone'] = PropertyVal.NO
    else:
        # stateless and
        # #PRESERVE
        retval['monotone'] = PropertyVal.PRESERVE

    # op deterministic
    retval['deterministic'] = deterministic(op_entry, deterministic_block)

    if 'deterministic' not in retval.keys():
        retval['deterministic'] = PropertyVal.NO
    if 'monotone' not in retval.keys():
        retval['monotone'] = PropertyVal.NO
    return retval


def propagate_props(graph):
    # Conservatively mark all edges PRESERVE, i.e. unknown
    for e in graph.edges:
        e.properties['deterministic'] = PropertyVal.PRESERVE
        e.properties['monotone'] = PropertyVal.PRESERVE
        e.properties['taint'] = PropertyVal.PRESERVE
    # An mark taint on all vertices PRESERVE
    for v in graph.vertices.values():
        v.properties['taint'] = PropertyVal.PRESERVE

    while True:
        change = False
        for name, v in graph.vertices.items():
            # if name == 'n25v1':
            #     import pdb
            #     pdb.set_trace()
            if (v.properties['deterministic'] == PropertyVal.PRESERVE
                or v.properties['monotone'] == PropertyVal.PRESERVE) \
                    and graph.inbounds.get(v) is not None:
                # upgrade vertex to YES if currently PRESERVE and ALL inbound edges are YES
                # upgrade vertex to NO if currently PRESERVE and ANY inbound edges are NO
                all_deterministic = (
                    v.properties['deterministic'] == PropertyVal.PRESERVE)
                all_monotone = (
                    v.properties['monotone'] == PropertyVal.PRESERVE)
                for e in graph.inbounds[v]:
                    if e.properties['deterministic'] != PropertyVal.YES:
                        all_deterministic = False
                        if v.properties['deterministic'] == PropertyVal.PRESERVE and e.properties['deterministic'] == PropertyVal.NO:
                            v.properties['deterministic'] = PropertyVal.NO
                            change = True
                    if e.properties['monotone'] != PropertyVal.YES:
                        all_monotone = False
                        if v.properties['monotone'] == PropertyVal.PRESERVE and e.properties['monotone'] == PropertyVal.NO:
                            v.properties['monotone'] = PropertyVal.NO
                            change = True
                if all_deterministic and v.properties['deterministic'] != PropertyVal.YES:
                    v.properties['deterministic'] = PropertyVal.YES
                    change = True
                if all_monotone and v.properties['monotone'] != PropertyVal.YES:
                    v.properties['monotone'] = PropertyVal.YES
                    change = True
                if v.properties['taint'] == PropertyVal.PRESERVE and v.properties['deterministic'] == PropertyVal.NO and v.properties['monotone'] == PropertyVal.NO:
                    v.properties['taint'] = PropertyVal.YES
                    change = True

            # upgrade outbounds if vertex is no longer PRESERVE
            if graph.outbounds.get(v) is not None:
                for e in graph.outbounds[v]:
                    if e.properties['deterministic'] == PropertyVal.PRESERVE and e.properties['deterministic'] != v.properties['deterministic']:
                        e.properties['deterministic'] = v.properties['deterministic']
                        change = True
                    if e.properties['monotone'] == PropertyVal.PRESERVE and e.properties['monotone'] != v.properties['monotone']:
                        e.properties['monotone'] = v.properties['monotone']
                        change = True
                    if e.properties['taint'] == PropertyVal.PRESERVE and e.properties['taint'] != v.properties['taint']:
                        e.properties['taint'] = v.properties['taint']
                        graph.vertices[e.destination].properties['taint'] = v.properties['taint']
                        change = True
        if not change:
            break

    # now that everything is propagated, resolve any remaining ambiguity
    # - tainted if non-deterministic and non-monotone
    # - properties left at PRESERVE go to YES
    for v in graph.vertices.values():
        if v.properties['deterministic'] == PropertyVal.NO and v.properties['monotone'] == PropertyVal.NO:
            v.properties['taint'] = PropertyVal.YES
        if v.properties['deterministic'] == PropertyVal.PRESERVE:
            v.properties['deterministic'] = PropertyVal.YES
        if v.properties['monotone'] == PropertyVal.PRESERVE:
            v.properties['monotone'] = PropertyVal.YES
    for e in graph.edges:
        if e.properties['deterministic'] == PropertyVal.NO and e.properties['monotone'] == PropertyVal.NO:
            e.properties['taint'] = PropertyVal.YES
            graph.vertices[e.destination].properties['taint'] = PropertyVal.YES
        if e.properties['deterministic'] == PropertyVal.PRESERVE:
            e.properties['deterministic'] = PropertyVal.YES
        if e.properties['monotone'] == PropertyVal.PRESERVE:
            e.properties['monotone'] = PropertyVal.YES

    # # taint is transitive: push it through
    # while True:
    #     change = False
    #     for v in graph.vertices.values():
    #         if v.properties.get('taint') and v.properties['taint'] == PropertyVal.YES:
    #             for e in graph.outbounds.get(v, []):
    #                 if not e.properties.get('taint') or e.properties['taint'] != PropertyVal.YES:
    #                     e.properties['taint'] = PropertyVal.YES
    #                     change = True
    #                 if not graph.vertices[e.destination].properties.get('taint') or graph.vertices[e.destination].properties['taint'] != PropertyVal.YES:
    #                     graph.vertices[e.destination].properties['taint'] = PropertyVal.YES
    #                     change = True
    #     if not change:
    #         break
    return graph


def color_node(node, v):
    if node.properties.get("taint") == PropertyVal.YES:
        if node.properties.get("monotone") == PropertyVal.YES:
            v.set_fillcolor("red")
            v.set_color("green")
            v.set_penwidth(5)
        else:
            v.set_fillcolor("red")
            v.set_color("black")
            v.set_penwidth(5)
    elif node.properties.get("monotone") == PropertyVal.YES:
        v.set_color("green")
        v.set_penwidth(5)
    else:
        v.set_color("black")
        v.set_penwidth(5)

    # let color_str = match mode {
    #     Color::Push => "style = filled, color = \"#ffff00\"",
    #     Color::Pull => "style = filled, color = \"#0022ff\", fontcolor = \"#ffffff\"",
    #     Color::Hoff => "style = filled, color = \"#ddddff\"",
    #     Color::Comp => "style = filled, color = white",
    # };
    if node.properties.get("deterministic") == PropertyVal.NO:
        v.set_style("dashed, filled")
    else:
        v.set_style("filled")

    if node.properties.get("taint") != PropertyVal.YES:
        if v.get_shape() == "invhouse":
            v.set_fillcolor("#0022ff")
            v.set_fontcolor = "#ffffff"
        elif v.get_shape() == "house":
            v.set_fillcolor("#ffff00")
        else:
            v.set_fillcolor("#ddddff")


def color_edge(edge, e):
    if edge.properties.get("taint") == PropertyVal.YES:
        if edge.properties.get("monotone") == PropertyVal.YES:
            e.set_color("#c303c3")
            e.set_penwidth(5)
        else:
            e.set_color("red")
            e.set_penwidth(5)
    elif edge.properties.get("monotone") == PropertyVal.YES:
        e.set_color("green")
        e.set_penwidth(5)
    if edge.properties.get("deterministic") == PropertyVal.NO:
        e.set_style("dashed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=str,
                        help="the name of the file to read")
    args = parser.parse_args()

    # print(help(pydot))
    dot_graph = None
    try:
        # read the dot file
        with open(args.file, "r") as f:
            dot_string = f.read()
        # use pydot to create a graph from the dot string
        dot_graph = pydot.graph_from_dot_data(dot_string)[0]
    except IOError as e:
        print("An error occurred while trying to read the file:", e)
        exit(1)
    except pydot.InvocationException as e:
        print("An error occurred while trying to parse the dot file:", e)
        exit(1)

    graph = parse_graph(dot_graph)
    for v in graph.vertices.values():
        v.properties.update(op_props(v.properties["op"]))

    graph = propagate_props(graph)

    for v in graph.vertices.values():
        print("op_props(%s): %s" % (v.name, v.properties))

    for v in dot_graph.get_nodes():
        node = graph.vertices[v.get_name().strip()]
        color_node(node, v)
    for s in dot_graph.get_subgraphs():
        for v in s.get_nodes():
            node = graph.vertices[v.get_name().strip()]
            color_node(node, v)

    for e in dot_graph.get_edges():
        edge = graph.edge_ix[e.get_source().strip(),
                             e.get_destination().strip()]
        color_edge(edge, e)
    for s in dot_graph.get_subgraphs():
        for e in s.get_edges():
            edge = graph.edge_ix[e.get_source().strip(),
                                 e.get_destination().strip()]
            color_edge(edge, e)

    dot_graph.write_dot("taint.dot")


if __name__ == "__main__":
    main()


def op_props_old(op, nonmon=PropertyVal.NO):
    retval = {}
    if lifetime != "":
        retval['lifetime'] = lifetime
    if op in ['difference', 'sort', 'sort_by']:
        retval['monotone'] = PropertyVal.NO
    elif op in ['demux', 'for_each', 'handoff', 'identity', 'inspect', 'unzip', 'unique', 'merge', 'tee', 'handoff', 'map', 'filter', 'flat_map', 'flatten', 'filter_map']:
        retval['monotone'] = PropertyVal.PRESERVE
    elif op in ['next_tick', 'next_stratum', 'dest_sink', 'dest_sink_serde']:
        pass
    elif op in ['source_stream', 'source_stream_serde', 'source_stdin']:
        retval['non_deterministic'] = PropertyVal.YES
    elif op in ['source_iter']:
        retval['monotone'] = PropertyVal.YES
    elif op in ['group_by']:
        retval['monotone'] = PropertyVal.YES
    elif op in ['fold', 'reduce']:
        if nonmon:
            retval['monotone'] = PropertyVal.NO
        if 'tick' in lifetime:
            retval['monotone'] = PropertyVal.NO
    elif op in ['join', 'cross_join']:
        if not 'tick' in lifetime:
            retval['monotone'] = PropertyVal.YES
        else:
            retval['monotone'] = PropertyVal.NO
    else:
        raise ValueError("Unknown operator: (%s)" % op)
    return retval
