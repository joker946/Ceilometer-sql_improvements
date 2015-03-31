obj = {
    'and': [{'=': {'counter_name': 'cpu_util'}}, {'>': {'counter_volume': 5}}]}

obj_complex = {"and":
               [{"and":
                 [{"=": {"counter_name": "cpu_util"}},
                  {">": {"counter_volume": 0.23}},
                  {"<": {"counter_volume": 0.26}}]},
                {"or":
                 [{"and":
                   [{">": {"timestamp": "2013-12-01T18:00:00"}},
                    {"<": {"timestamp": "2013-12-01T18:15:00"}}]},
                  {"and":
                   [{">": {"timestamp": "2013-12-01T18:30:00"}},
                    {"<": {"timestamp": "2013-12-01T18:45:00"}}]}]}]}

complex_operators = ['and', 'or', 'not']


def _handle_complex_op(complex_op, nodes):
    items = []
    t = True
    for node in nodes:
        if node.keys()[0] in complex_operators and node.values()[0][0].keys()[0] in complex_op:
            t = True
        else:
            t = False
        node_str = transform(node)
        items.append(node_str)
    if t:
        return '(' + ' {} '.format(complex_op).join(items) + ')'
    else:
        return ' {} '.format(complex_op).join(items)


def _handle_simple_op(simple_op, nodes):
    return "%s %s %s" % (nodes.keys()[0], simple_op, nodes.values()[0])


def transform(tree):
    operator = tree.keys()[0]
    nodes = tree.values()[0]
    if operator in complex_operators:
        return _handle_complex_op(operator, nodes)
    else:
        return _handle_simple_op(operator, nodes)

print transform(obj_complex)
