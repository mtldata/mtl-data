import bulbs.rexster as graph
import datanommer.models as m


graph_uri = "http://localhost:8182/db/data"


class GraphFeed(object):

    def __init__(self):
        m.init(config='alembric.ini')
        config = graph.Config(graph_uri)
        self.graph = graph.Graph(config)

    def buildGraph(self, offset=0, limit=100):
        rows = m.Message.query.all().offset(offset).limit(limit)
        for row in rows:
            users = [self.graph.vertices.get_or_create('user', x.name)
                     for x in row.users.all()]
            pkgs = [self.graph.vertices.get_or_create('package', x.name)
                    for x in row.packages.all()]
            props = {'timestamp': row.timestamp,
                     'category': row.category}
            for user in users:
                [self.graph.edges.create(user, row.topic, p, props) for p in pkgs]


def main():
    stream = GraphFeed()
    stream.buildGraph(start=0, offset=100)

if __name__ == '__main__':
    main()
