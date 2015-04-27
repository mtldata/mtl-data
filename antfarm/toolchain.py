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
            cypher = 'MATCH (u:user {name:{user}}), (p:package {name: {pkg}}) ' \
                     'MERGE (u)-[r:{topic} {timestamp: {time}, category: {cat}, msg_id: {msg_id}}]->(p) ' \
                     'RETURN r'
            tx = self.graph.cypher.begin()
            for row in rows:
                param = {'user': row.users[0].name,
                         'pkg': row.packages[0].name,
                         'topic': row.topic,
                         'time': row.timestamp,
                         'cat': row.category,
                         'msg_id': row.msg_id}
                tx.append(cypher, param)
            tx.process()
            tx.commit()

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
