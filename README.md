CloudProject
============

Cassandra Browser Tool using REST API

Run using command
  python run.py
Approach
-Create a web application that interacts with Cassandra to retrieve information about Keyspaces,Column Family
-Web Based Application so that user can access using REST API.
-Allowing the user to fetch database without knowing about Cassandra Query Language.

Architecture
  Back End
    -Load existing data into Cassandra Database
    -Web server to communicate with cassandra and execute queries.
    -Web server handles incoming request and respond them back.
  Front End
    -Accept user request in the form of URL
    -Request to Web server for connecting cassandra Node
    -Display existing keyspaces
    -Display column_families of particular Keyspace
    
