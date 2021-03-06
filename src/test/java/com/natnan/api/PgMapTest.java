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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class PgMapTest {

  private PGDataSource dataSource = new PGDataSource();
  private PGConnection connection;
  private static final TableMapper<TestData> mapper = new TableMapper<TestData>() {
    @Override
    public String getTableName() {
      return "test_data";
    }

    @Override
    public String getQueryString() {
      return "SELECT * from test_data;";
    }

    @Override
    public Map.Entry<UUID, TestData> mapSingleResult(ResultSet resultSet) throws SQLException {
      UUID id = UUID.fromString(resultSet.getString("id"));
      TestData testData = new TestData(resultSet.getString("name"), resultSet.getString("property"));
      return new AbstractMap.SimpleEntry<>(id, testData);
    }
  };

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
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate("CREATE TABLE test_data (\n"
                              + "    id uuid NOT NULL,\n"
                              + "    name text COLLATE pg_catalog.\"default\",\n"
                              + "    property text COLLATE pg_catalog.\"default\",\n"
                              + "    CONSTRAINT \"map-test_pkey\" PRIMARY KEY (id)\n"
                              + ");");
    }
  }

  private void insertSampleData(UUID id) throws SQLException {
    try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT into test_data (id, name, property) VALUES(?, ?, ?);")) {
      preparedStatement.setString(1, id.toString());
      preparedStatement.setString(2, "name");
      preparedStatement.setString(3, "property");
      preparedStatement.execute();
    }
  }

  @Test
  public void map_initialization_retrieves_existing_data() throws SQLException, IOException {
    UUID id = UUID.randomUUID();
    insertSampleData(id);

    try (PgMap<TestData> testMap = PgMap.createSyncMap(connection, mapper)) {
      assertThat(testMap).containsOnly(new AbstractMap.SimpleEntry<>(id, new TestData("name", "property")));
    }
  }

  @Test
  public void insert_via_another_channel_updates_map_asynchronously() throws SQLException, IOException, InterruptedException {
    try (PgMap<TestData> testMap = PgMap.createSyncMap(connection, mapper)) {
      CountDownLatch latch = new CountDownLatch(1);
      testMap.setMapUpdatedNotification(latch::countDown);
      UUID id = UUID.randomUUID();
      insertSampleData(id);
      assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
      assertThat(testMap).containsOnly(new AbstractMap.SimpleEntry<>(id, new TestData("name", "property")));
    }
  }

  @Test
  public void update_via_another_channel_updates_map_asynchronously() throws SQLException, IOException, InterruptedException {
    UUID id = UUID.randomUUID();
    insertSampleData(id);

    try (PgMap<TestData> testMap = PgMap.createSyncMap(connection, mapper)) {
      CountDownLatch latch = new CountDownLatch(1);
      testMap.setMapUpdatedNotification(latch::countDown);

      try (PreparedStatement preparedStatement = connection.prepareStatement("UPDATE test_data SET name=? WHERE id=?;")) {
        preparedStatement.setString(2, id.toString());
        preparedStatement.setString(1, "another");
        preparedStatement.execute();
      }

      assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
      assertThat(testMap).containsOnly(new AbstractMap.SimpleEntry<>(id, new TestData("another", "property")));
    }
  }

  @Test
  public void delete_via_another_channel_updates_map_asynchronously() throws SQLException, IOException, InterruptedException {
    UUID id = UUID.randomUUID();
    insertSampleData(id);
    try (PgMap<TestData> testMap = PgMap.createSyncMap(connection, mapper)) {
      CountDownLatch latch = new CountDownLatch(1);
      testMap.setMapUpdatedNotification(latch::countDown);

      try (PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM test_data WHERE id=?;")) {
        preparedStatement.setString(1, id.toString());
        preparedStatement.execute();
      }

      assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
      assertThat(testMap).isEmpty();
    }
  }

  @Test
  public void two_maps_for_the_same_table_are_updated_and_notified_once() throws SQLException, IOException, InterruptedException {
    try (PgMap<TestData> map1 = PgMap.createSyncMap(connection, mapper)) {
      CountDownLatch latch1 = new CountDownLatch(1);
      map1.setMapUpdatedNotification(latch1::countDown);
      try (PgMap<TestData> map2 = PgMap.createSyncMap(connection, mapper)) {
        CountDownLatch latch2 = new CountDownLatch(1);
        map2.setMapUpdatedNotification(latch2::countDown);
        UUID id = UUID.randomUUID();
        insertSampleData(id);
        assertThat(latch1.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(map1).containsOnly(new AbstractMap.SimpleEntry<>(id, new TestData("name", "property")));
        assertThat(latch2.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(map2).containsOnly(new AbstractMap.SimpleEntry<>(id, new TestData("name", "property")));
      }
    }
  }

  @After
  public void after() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate("drop table test_data;");
    }
    connection.close();
  }
}
