package com.natnan.api;

import com.google.common.base.CaseFormat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import lombok.SneakyThrows;

// TODO design concurrency mechanism
// TODO close
public class PgMap<V> extends HashMap<UUID, V> implements PGNotificationListener {

  private static ObjectMapper mapper = new ObjectMapper();
  private final Connection connection; // TODO handle reconnection etc.
  private final String tableName;
  private final Class<V> clazz;

  private PgMap(Map<UUID, V> map, Connection connection, Class<V> clazz, String tableName) {
    super(map);
    this.connection = connection;
    this.clazz = clazz;
    this.tableName = tableName;
    new Thread(this::updateMapThread).start();
  }

  // TODO create table if it doesn't exist
  // TODO exceptions
  // TODO support generic type
  public static <V> PgMap<V> createSyncMap(PGConnection connection, Class<V> clazz) throws SQLException, IOException {
    String tableName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, clazz.getSimpleName());

    Map<UUID, V> map = new HashMap<>();
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(String.format("SELECT id, data FROM %s;", tableName));
      while (resultSet.next()) {
        UUID id = UUID.fromString(resultSet.getString(1));
        String json = resultSet.getString(2);
        V v = mapper.readValue(json, clazz);
        map.put(id, v);
      }
    }

    // TODO test same two objects at the same time
    try (Statement statement = connection.createStatement()) {
      statement.execute(String.format("LISTEN %s", tableName));
    }

    PgMap<V> vPgMap = new PgMap<>(map, connection, clazz, tableName);
    connection.addNotificationListener(vPgMap);

    return vPgMap;
  }

  @Override
  @SneakyThrows({SQLException.class, JsonProcessingException.class})
  public V put(UUID key, V value) {
    PreparedStatement preparedStatement = connection.prepareStatement(String.format("INSERT INTO %s (id, data) VALUES(?, ?);", tableName));
    preparedStatement.setString(1, key.toString());
    preparedStatement.setString(2, mapper.writeValueAsString(value));
    preparedStatement.execute(); // TODO handle result
    return super.put(key, value);
  }

  @Override
  public synchronized void notification(int processId, String channelName, String payload) {
    // can't do update on this thread (probably?). Trigger update
    this.notifyAll();
  }

  @SneakyThrows({SQLException.class, IOException.class, InterruptedException.class})
  private synchronized void updateMapThread() {
    while (true) {
      this.wait();
      super.clear();
      try (Statement statement = connection.createStatement()) {
        ResultSet resultSet = statement.executeQuery(String.format("SELECT id, data FROM %s;", tableName));
        while (resultSet.next()) {
          UUID id = UUID.fromString(resultSet.getString(1));
          String json = resultSet.getString(2);
          V v = mapper.readValue(json, clazz);
          super.put(id, v);
        }
      }
    }
  }

  // TODO proxy objects for updates
  // TODO delete
  // TODO putall (test first)
}
