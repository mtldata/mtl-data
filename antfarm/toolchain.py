from py2neo import Graph, Node, Relationship
import datanommer.models as m


graph_uri = "http://localhost:8182/db/data"


class GraphFeed(object):

    def __init__(self):
        # m.init(uri= 'postgresql://datanommer:datanommer@localhost/datanommer')
        m.init('postgresql+psycopg2://datanommer:datanommer@localhost:5432/datanommer')
        self.graph = Graph()

    def buildGraph(self, offset=0, limit=100):
        rows = m.session.query(m.Message).offset(offset).limit(limit).all()
        for row in rows:
            users = [Node('user', x.name)
                     for x in row.users.all()]
            pkgs = [Node('package', x.name)
                    for x in row.packages.all()]
            props = {'timestamp': row.timestamp,
                     'category': row.category}
            for user in users:
                [Relationship(user, row.topic, p)
                 for p in pkgs]


def main():
    stream = GraphFeed()
    stream.buildGraph(start=0, offset=100)

if __name__ == '__main__':
    main()
