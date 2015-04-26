from py2neo import Graph, Node, Relationship
from py2neo.batch import WriteBatch
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
                     for x in row.users]
            pkgs = [Node('package', x.name)
                    for x in row.packages]
            props = {'timestamp': row.timestamp,
                     'category': row.category}
            batch = WriteBatch(self.graph)
            for user in users:
                [batch.get_or_create_relationship(user, row.topic, p)
                 for p in pkgs]

    def addUsers(self):
        users = m.session.query(m.User).all()
        tx = self.graph.cypher.begin()
        user_list = [x.name for x in users]
        cypher = "MERGE (n:user {name:{N}}) RETURN n"
        for name in user_list:
            tx.append(cypher, {'N': name})
        tx.process()
        tx.commit()

    def addPackages(self):
        pkgs = m.session.query(m.Package).all()
        cypher = "MERGE (n:package {name:{N}}) RETURN n"
        pkgs_list = [x.name for x in pkgs]
        tx = self.graph.cypher.begin()
        for name in pkgs_list:
            tx.append(cypher, {'N': name})
        tx.process()
        tx.commit()


def main():
    stream = GraphFeed()
    stream.buildGraph(limit=100, offset=0)

if __name__ == '__main__':
    main()
