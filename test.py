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

obj_complex1 = {"or":[{"=": {"counter_name": "cpu_util"}},{"and":[{">": {"timestamp": "2013-12-01T18:00:00"}},{"<": {"timestamp": "2013-12-01T18:15:00"}}]}]}
complex_operators = ['and', 'or', 'not']


def _handle_complex_op(complex_op, nodes, values):
    items = []
    for node in nodes:
        node_str = _transform_filter(node, values)
        items.append(node_str)
    if complex_op is 'or':
        return '(' + ' {} '.format(complex_op).join(items) + ')'
    if complex_op is 'and':
        return ' {} '.format(complex_op).join(items)


def _handle_simple_op(simple_op, nodes, values):
    if nodes.keys()[0].startswith('resource_metadata'):
        q, v = apply_metaquery_filter(nodes)
        values.append(v)
        return q
    values.append(nodes.values()[0])
    return "%s %s %%s" % (nodes.keys()[0], simple_op)


def _transform_filter(tree, values):
    operator = tree.keys()[0]
    nodes = tree.values()[0]
    if operator in complex_operators:
        return _handle_complex_op(operator, nodes, values)
    else:
        return _handle_simple_op(operator, nodes, values)


def transform_filter(tree):
    values = []
    res = _transform_filter(tree, values)
    return ' WHERE ' + res, values


def transform_orderby(orderby):
    return ' ORDER BY ' + ', '.join(['%s %s' % (x.keys()[0], x.values()[0])
                                     for x in orderby])

print transform_filter(obj_complex1)
