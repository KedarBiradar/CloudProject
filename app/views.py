from app import app
from flask import render_template
from flask import request
from flask import make_response
from flask import Blueprint
#from flask.ext.paginate import Pagination
import cassandra
from cassandra.cluster import Cluster
import logging
import cassandraCode
import backend
from flask import session, redirect, url_for, escape

#global node
node = "127.0.0.1"

def getMachineIP(session1):
	if 'node' in session1:
		return session1['node']
	else:
		return "127.0.0.1"
#*****************************************************************************************************
@app.route('/')
@app.route('/index',methods=["GET","POST"])
def index():
	if not 'node' in session:
		session['node']="127.0.0.1"	
	return render_template('index.html',title='Cassandra Browser Tool :: CBT')

#*****************************************************************************************************
@app.route('/about-us')
def aboutUs():
	return render_template('about-us.html',title='About Us :: CBT')

#*****************************************************************************************************

@app.route('/about-project')
def aboutProject():
	return render_template('about-project.html',title='About Project :: CBT')

#*****************************************************************************************************
@app.route('/help')
def help():
	return render_template('help.html',title='Help :: CBT')
#*****************************************************************************************************
@app.route('/showAllKeyspaces')
def show_all_keySpaces(machine_ip = node):
	machine_ip= getMachineIP(session)
	output = backend.CassandraConnect()
	(flag,result,errorMsg)=output.listKeyspaces(machine_ip)	
	return render_template('AllKeyspaces.html',title='All Keyspaces :: CBT',keyspaces = result , error = flag , errorMsg = errorMsg)
#*****************************************************************************************************

@app.route('/ShowKeySpace/<string:keyspaceName>')
def show_Keyspace(keyspaceName,machine_ip=node):
	machine_ip= getMachineIP(session)
	output = backend.CassandraConnect()
	(flag,result,errorMsg)=output.showTables(machine_ip,keyspaceName)
	return render_template('List_ColumnFamily.html', title="Column-Family list of '"+ keyspaceName +"' :: CBT" , keyspaceName=keyspaceName,result = result , error = flag,errorMsg=errorMsg)
#*****************************************************************************************************
@app.route('/showTableSchema/<string:keyspaceName>/<string:tableName>')
def show_tableSchema(keyspaceName,tableName,machine_ip=node):
	machine_ip= getMachineIP(session)
	output = backend.CassandraConnect()
	(flag,result,errorMsg)=output.showTableSchema(machine_ip,keyspaceName,tableName)
	#print flag
	#print errorMsg
	return render_template('show_tableSchema.html', title="Schema of '"+tableName+"' :: CBT" ,keyspace = keyspaceName,column_family=tableName,result = result , error = flag , errorMsg=errorMsg)
#*****************************************************************************************************
@app.route('/showTableContent/<string:keyspaceName>/<string:tableName>')
def show_tableContent(keyspaceName,tableName,machine_ip=node):
	machine_ip= getMachineIP(session)
	output=backend.CassandraConnect()
	(flag,columnNames,rows,errorMsg)=output.execute_select(machine_ip,keyspaceName,tableName)
	'''print flag
	if flag:
		errorMsg=rows
		rows=None;
	else:		
		errorMsg=None
	'''
	return render_template('selectRows.html', title="Table Content of '"+ tableName +"' :: CBT", columns = columnNames, result = rows , tableName = tableName, error = flag , errorMsg=errorMsg)
#*****************************************************************************************************
@app.route('/showTableContentCol/<string:keyspaceName>/<string:tableName>',methods=["GET","POST"])
def show_tableContentCol(keyspaceName,tableName,machine_ip=node):
	machine_ip= getMachineIP(session)
	cols = ''
	colnum = 0;
	if request.method == 'GET':
		for col in request.args:
			#print col;
			if colnum == 0:
				cols = cols + col
			else:
				cols = cols + "," + col
			colnum = colnum + 1
			#print colnum
	output=backend.CassandraConnect()
	(flag,columnNames,rows,errorMsg)=output.execute_select_col(machine_ip,keyspaceName,tableName,cols)
	'''print flag
	if flag:
		errorMsg=rows
		rows=None;
	else:
		errorMsg=None
	'''
	return render_template('selectRows.html', title="Table Content of '"+ tableName +"' :: CBT" , columns = columnNames, result = rows , tableName = tableName, error = flag , errorMsg=errorMsg)
#*****************************************************************************************************
@app.route('/showAllTables')
def show_all_tables(machine_ip=node):
	machine_ip= getMachineIP(session)
	output=backend.CassandraConnect()
	(flag,tables,errorMsg)=output.showAllTables(machine_ip)
	#print tables
	return render_template('AllColumnFamily.html', title="All Table List :: CBT" , tables = tables, error = flag , errorMsg=errorMsg)
#*****************************************************************************************************
'''
@app.route('/paginate')
def showResultPagewise():
	search = False
	q = request.args.get('q')
	if q:
		search = True
	try:
		page = int(request.args.get('page', 1))
	except ValueError:
		page = 1
	pagination = Pagination(page=page,per_page=20, css_framework='static/css/style.css',total=300, search=search).paginate(1,10,False)
	return render_template('/page.html',pagination=pagination,)
'''
#*****************************************************************************************************
@app.route('/update_node',methods=['GET','POST'])
def update_node():
	global node
	#print node
	if request.method == 'GET':
		#print "***************************************************8888 update"
		session['node'] = request.args.get("ipaddress")
		error = False
		errorMsg = None
		output=backend.CassandraConnect()
		try:
			#print getMachineIP(session)
			output.connect([getMachineIP(session)])
			output.close()
		except Exception as e:
			print e
			error = True
			errorMsg = "Unable to connect node "+session['node']+". Please try again later."
		#print node
		#print "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	#print node
	return render_template('connectnode.html', title="New Node Added :: CBT" , node = session['node'] , error = error, errorMsg = errorMsg)

#****************************************************************************************************
@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404
