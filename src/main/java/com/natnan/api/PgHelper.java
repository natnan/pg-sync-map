package com.natnan.api;

import com.impossibl.postgres.jdbc.PGDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PgHelper {


  // TODO move this into the map or make it a usable API
  public static void main(String[] args) throws SQLException {
    PGDataSource dataSource = new PGDataSource();
    dataSource.setHost("localhost");
    dataSource.setPort(5432);
    dataSource.setDatabase("test");
    dataSource.setUser("postgres");
    dataSource.setPassword("test");

    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.executeUpdate("CREATE OR REPLACE FUNCTION notify_change() RETURNS TRIGGER AS $$\n"
                                + "    BEGIN\n"
                                + "        PERFORM pg_notify(TG_TABLE_NAME, TG_TABLE_NAME);\n"
                                + "        RETURN NEW;\n"
                                + "    END;\n"
                                + "$$ LANGUAGE plpgsql;\n"
                                + "DROP TRIGGER IF EXISTS table_change ON test_data;\n"
                                + "CREATE TRIGGER table_change \n"
                                + "    AFTER INSERT OR UPDATE OR DELETE ON test_data\n"
                                + "    FOR EACH ROW EXECUTE PROCEDURE notify_change();");
      }
    }


  }
}
