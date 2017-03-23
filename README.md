pg-sync-map
==========

If, for some reason, you're using PostgreSQL with dynamic data, and you read much more often than you write, this is a small tool to synchronize PostgreSQL tables to your memory.

- Read from database and create an immutable <UUID, Object> map. 
- The map is automatically updated a change occurs in the table using notify/listen feature of PostgreSQL.


<br><br>


 - [PostgreSQL](https://www.postgresql.org/) 9.3 and up for persistence.
 - [pgjdbc-ng](https://github.com/impossibl/pgjdbc-ng) for jdbc connection and listen/notify support.