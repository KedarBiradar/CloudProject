import cassandra
from cassandra.cluster import Cluster
import logging

class SimpleClient:
	session = None

	def connect(self, nodes):
		cluster = Cluster(nodes)
		metadata = cluster.metadata
		self.session = cluster.connect()
		result=self.session.execute("SELECT keyspace_name from system.schema_keyspaces")
		return result

	def close(self):
		self.session.cluster.shutdown()
		self.session.shutdown()

	def show_All_Keyspaces(self):
		client = SimpleClient()
		result=client.connect(['127.0.0.1'])
		client.close()
		x = [""]
		for key in result:
			print str(key.keyspace_name)		
			x.append(str(key.keyspace_name))
		return x
