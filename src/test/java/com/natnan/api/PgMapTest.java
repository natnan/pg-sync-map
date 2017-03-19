package com.natnan.api;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.jdbc.PGDataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class PgMapTest {

  private PGDataSource dataSource = new PGDataSource();
  private PGConnection connection;
  private static final String sampleJson = "{\"name\":\"name\",\"property\":\"property\"}";

  public PgMapTest() {
    dataSource.setHost("localhost");
    dataSource.setPort(5432);
    dataSource.setDatabase("test");
    dataSource.setUser("postgres");
    dataSource.setPassword("test");
  }

  @Before
  public void before() throws SQLException {
    connection = (PGConnection) dataSource.getConnection();
  }

  @Test
  public void map_initialization_retrieves_existing_data() throws SQLException, IOException {
    UUID id = UUID.randomUUID();
    insertSampleData(id);

    PgMap<TestData> testMap = PgMap.createSyncMap(connection, TestData.class);
    assertThat(testMap).containsOnly(new AbstractMap.SimpleEntry<>(id, new TestData("name", "property")));
  }

  @Test
  public void map_put_inserts_to_table() throws SQLException, IOException {
    PgMap<TestData> testMap = PgMap.createSyncMap(connection, TestData.class);
    UUID id = UUID.randomUUID();
    TestData data = new TestData("name", "property");
    testMap.put(id, data);
    assertThat(testMap).containsOnly(new AbstractMap.SimpleEntry<>(id, data));

    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT id, data FROM test_data;");
      resultSet.next();
      assertThat(resultSet.getString(1)).isEqualTo(id.toString());
      assertThat(resultSet.getString(2)).isEqualTo(sampleJson);
      assertThat(resultSet.next()).isFalse();
    }
  }

  @Test
  public void map_remove_updates_table() throws IOException, SQLException {
    UUID id = UUID.randomUUID();
    insertSampleData(id);

    PgMap<TestData> testMap = PgMap.createSyncMap(connection, TestData.class);
    testMap.remove(id);

    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT id, data FROM test_data;");
      assertThat(resultSet.next()).isFalse();
    }
  }

  @Test
  public void map_put_updates_table_for_existing_id() throws IOException, SQLException {
    UUID id = UUID.randomUUID();
    insertData(id, "{\"name\":\"name2\",\"property\":\"property2\"}");

    PgMap<TestData> testMap = PgMap.createSyncMap(connection, TestData.class);
    testMap.put(id, new TestData("name", "property"));

    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT id, data FROM test_data;");
      resultSet.next();
      assertThat(resultSet.getString(1)).isEqualTo(id.toString());
      assertThat(resultSet.getString(2)).isEqualTo(sampleJson);
      assertThat(resultSet.next()).isFalse();
    }
  }

  private void insertSampleData(UUID id) throws SQLException {
    insertData(id, sampleJson);
  }

  private void insertData(UUID id, String json) throws SQLException {
    PreparedStatement preparedStatement = connection.prepareStatement("INSERT into test_data (id, data) VALUES(?, ?);");
    preparedStatement.setString(1, id.toString());
    preparedStatement.setString(2, json);
    preparedStatement.execute();
  }

  @Test
  public void insert_via_another_channel_updates_map_asynchronously() throws SQLException, IOException, InterruptedException {
    PgMap<TestData> testMap = PgMap.createSyncMap(connection, TestData.class);
    UUID id = UUID.randomUUID();
    insertSampleData(id);
    // poll 2 seconds for changes
    for (int i = 0; i < 200; i++) {   // TODO replace with callback wait
      if (testMap.size() > 0) {
        break;
      }
      Thread.sleep(10);
    }
    assertThat(testMap).containsOnly(new AbstractMap.SimpleEntry<>(id, new TestData("name", "property")));
  }

  @Test
  public void update_via_another_channel_updates_map_asynchronously() throws SQLException, IOException, InterruptedException {
    PgMap<TestData> testMap = PgMap.createSyncMap(connection, TestData.class);
    UUID id = UUID.randomUUID();
    testMap.put(id, new TestData("something", "else"));

    PreparedStatement preparedStatement = connection.prepareStatement("UPDATE test_data SET data=? WHERE id=?;");
    preparedStatement.setString(2, id.toString());
    preparedStatement.setString(1, sampleJson);
    preparedStatement.execute();

    Thread.sleep(200);
    assertThat(testMap).containsOnly(new AbstractMap.SimpleEntry<>(id, new TestData("name", "property")));
  }

  @Test
  public void delete_via_another_channel_updates_map_asynchronously() throws SQLException, IOException, InterruptedException {
    PgMap<TestData> testMap = PgMap.createSyncMap(connection, TestData.class);
    UUID id = UUID.randomUUID();
    testMap.put(id, new TestData("something", "else"));

    PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM test_data WHERE id=?;");
    preparedStatement.setString(1, id.toString());
    preparedStatement.execute();

    Thread.sleep(200);
    assertThat(testMap).isEmpty();
  }

  @After
  public void after() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate("truncate test_data");
    }
    connection.close();
  }
}
