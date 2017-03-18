pg-sync-map
==========

If, for some reason, you're using PostgreSQL with dynamic data, and you read much more often than you write, this is a tool to synchronize PostgreSQL tables to your memory.

- Read from database and create a <UUID, Object> map. (Initially only supports tables with `(<uuid> id, <json> data)`)
- Update the table as you use Java Map methods.
- Automatically update the Map whenever a change occurs in the table.


<br><br>


via combining:

 - [PostgreSQL](https://www.postgresql.org/) 9.3 and up for persistence.
 - [pgjdbc-ng](https://github.com/impossibl/pgjdbc-ng) for jdbc connection and listen/notify support.
 - [Jackson](http://jackson.codehaus.org) for JSON handling.