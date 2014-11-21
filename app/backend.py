import sys
import cassandra
from cassandra.cluster import Cluster
from cassandra.query import named_tuple_factory
from cassandra.util import OrderedDict
import logging 
log = logging.getLogger()
log.setLevel('INFO')

class CassandraConnect:
	session = None
#*****************************************************************************************************
	def connect(self,node):
		#print "@@@@@@@@@ "+ str(node)
		try:
			cluster = Cluster(node)
			metadata = cluster.metadata
			self.session = cluster.connect()
			log.info('Connected to Cluster : ' + metadata.cluster_name)
			#self.close()
		except:
			raise
			
#*****************************************************************************************************
	def listKeyspaces(self,node):
		#print " $$$$$$$$$$$$$$$$$$$$$$$$$ "+ node
		client = CassandraConnect()
		result = []
		try:
			client.connect([node])
		except Exception as e:
			#print e
			return True,"None",e

		try:
			keyspaces = client.session.execute("SELECT keyspace_name from system.schema_keyspaces")
			for row in keyspaces:
				result.append(str(row.keyspace_name))
		except Exception as e:
			#print e
			client.close();
			return True,result,e
		client.close();
		return False,result,"None"

#*****************************************************************************************************	
	def showTables(self,node,keyspace):
		client = CassandraConnect()

		try:
			client.connect([node])
		except Exception as e:
			#print e
			return True,"None",e

		#tables = client.session.execute("select columnfamily_name from " + keyspace +".schema_columnfamilies")
		tableNames = []
		try:
			tables = client.session.execute("SELECT columnfamily_name FROM system.schema_columnfamilies where keyspace_name = '" + keyspace + "'")
			#print tables
			for row in tables:
				tableNames.append(str(row.columnfamily_name))
		except Exception as e:
			print e
			client.close();
			return True,tableNames,e

		client.close();
		return False,tableNames,"None"

#*****************************************************************************************************
	def showAllTables(self,node):
		client = CassandraConnect()

		try:
			client.connect([node])
		except Exception as e:
			#print e
			return True,"None",e

		#tables = client.session.execute("select columnfamily_name from " + keyspace +".schema_columnfamilies")
		try:
			tables = client.session.execute("SELECT keyspace_name,columnfamily_name FROM system.schema_columnfamilies")
			tableNames = []
			for row in tables:
					tableNames.append(str(row.keyspace_name))
					tableNames.append(str(row.columnfamily_name))
			client.close();
		except Exception as e:
			#print e
			return True,tableNames,e
		
		return False,tableNames,"None"
#*****************************************************************************************************
	def showTableSchema(self,node,keyspace,table):
		client = CassandraConnect()

		try:
			client.connect([node])
		except Exception as e:
			#print e
			return True,"None",e

		columnNames = []
		try:
			columns = client.session.execute("SELECT column_name FROM system.schema_columns  WHERE keyspace_name = '" + keyspace+"' AND columnfamily_name = '" + table +"'")
			for row in columns:
				columnNames.append(str(row.column_name))
		except Exception as e:
			#print e
			client.close();
			return True,columnNames,e
		client.close();
		return False,columnNames,"None"

#*****************************************************************************************************
	def execute_select(self,node,keyspace,table):
		client = CassandraConnect()
		try:
			client.connect([node])
		except Exception as e:
			#print e
			return True,"None","None",e
		columnNames = []
		data = []
		try:
			columns = client.session.execute("SELECT column_name FROM system.schema_columns  WHERE keyspace_name = '" + keyspace+"' AND columnfamily_name = '" + table +"'")
			for col in columns:
				columnNames.append(str(col.column_name))
			colstr = str()
			for c in range(0,len(columnNames)):
				#print columnNames[c];
				if c == 0:
					colstr = colstr + columnNames[c] 	
				else:
					colstr = colstr + "," + columnNames[c]
		
		        data  = client.session.execute("select "+ colstr + " from "+ keyspace + "." + table)
		except Exception as e:
			#print e
			client.close();
			return True,columnNames,data,e
		client.close();
		return False,columnNames,data,"None"
#*****************************************************************************************************
	def execute_select_col(self,node,keyspace,table,colstr):
		client = CassandraConnect()
		try:
			client.connect([node])
		except Exception as e:
			#print e
			return True,"None","None",e

		columnNames = colstr.split(",")
		data = []
		try:
			data  = client.session.execute("select "+ colstr + " from "+ keyspace + "." + table)
		except Exception as e:
			#print e
			client.close();
			return True,columnNames,data,e
		client.close();
		#print "**********************&^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
		#print data
		return False,columnNames,data,"None"
#*****************************************************************************************************
	def close(self):
		self.session.cluster.shutdown()
		self.session.shutdown()
		log.info('Connection closed .')
#*****************************************************************************************************
