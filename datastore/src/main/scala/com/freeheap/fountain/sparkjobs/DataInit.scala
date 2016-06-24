package com.freeheap.fountain.sparkjobs

import com.datastax.spark.connector.cql.CassandraConnector


/**
  * @author Minh Do
  */
class DataInit(conn: CassandraConnector, keyspace: String) extends Serializable {

   def init() : Unit = {
     conn.withSessionDo { session =>
       session.execute("Drop Keyspace If Exists " + keyspace)
       session.execute("CREATE KEYSPACE " + keyspace + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
       session.execute("USE " + keyspace)
       session.execute("CREATE TABLE IF NOT EXISTS users (user_id int, first_name text, last_name text, age int,  PRIMARY KEY ((user_id), first_name, last_name))")

       for(i <- 1 to 10) {
          session.execute("Insert into users(user_id, first_name, last_name, age) values(" + i + ", 'first_" + i + "', 'last_" + i + "', 30)")
       }

     }
   }
}
