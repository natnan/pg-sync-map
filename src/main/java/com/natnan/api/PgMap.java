package com.natnan.api;

import com.google.common.base.CaseFormat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;

import java.io.Closeable;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Setter;
import lombok.SneakyThrows;

// TODO design concurrency mechanism
// TODO logging
// TODO the value objects must not be mutable from outside in order to avoid unsynched states. Enforcing immutability doesn't seem to be an option. Maybe use Kryo to return deep copy of the objects
// TODO consider not updating the map directly. Only use update via SELECT.
// TODO maybe don't implement Map<> so we can throw whatever we want?
public class PgMap<V> implements Map<UUID, V>, PGNotificationListener, Closeable {

  private static ObjectMapper mapper = new ObjectMapper();
  private final PGConnection connection; // TODO handle reconnection etc.
  private final String tableName;
  private final Class<V> clazz;

  private boolean updateThread = true;
  private final Thread thread;
  @Setter
  private MapUpdatedNotification mapUpdatedNotification;

  private Map<UUID, V> map; // TODO concurrent hashmap?

  private PgMap(Map<UUID, V> map, PGConnection connection, Class<V> clazz, String tableName) {
    this.map = map;
    this.connection = connection;
    this.clazz = clazz;
    this.tableName = tableName;
    this.thread = new Thread(this::updateMapThread);
    thread.start(); // TODO anti-pattern to start a thread on constructor.
  }

  // TODO create table if it doesn't exist
  // TODO exceptions
  // TODO support generic types
  public static <V> PgMap<V> createSyncMap(PGConnection connection, Class<V> clazz) throws SQLException, IOException {
    String tableName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, clazz.getSimpleName());

    Map<UUID, V> map = new HashMap<>();
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(String.format("SELECT id, data FROM %s;", tableName));
      while (resultSet.next()) {
        // TODO abstract the parsing and writing
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

  @SneakyThrows({SQLException.class, IOException.class})
  private synchronized void updateMapThread() {
    while (updateThread) {
      try {
        this.wait();
        Map<UUID, V> newMap = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
          ResultSet resultSet = statement.executeQuery(String.format("SELECT id, data FROM %s;", tableName));
          while (resultSet.next()) {
            UUID id = UUID.fromString(resultSet.getString(1));
            String json = resultSet.getString(2);
            V v = mapper.readValue(json, clazz);
            newMap.put(id, v);
          }
        }
        this.map = newMap;
        if (mapUpdatedNotification != null) {
          mapUpdatedNotification.call();
        }
      } catch (InterruptedException ignored) {
      }
    }
  }

  @Override
  @SneakyThrows({SQLException.class, JsonProcessingException.class})
  public V put(UUID key, V value) {
    if (map.containsKey(key)) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(String.format("UPDATE %s SET data=? WHERE id=?;", tableName))) {
        preparedStatement.setString(2, key.toString());
        preparedStatement.setString(1, mapper.writeValueAsString(value));
        preparedStatement.execute();
      }
    } else {
      try (PreparedStatement preparedStatement = connection.prepareStatement(String.format("INSERT INTO %s (id, data) VALUES(?, ?);", tableName))) {
        preparedStatement.setString(1, key.toString());
        preparedStatement.setString(2, mapper.writeValueAsString(value));
        preparedStatement.execute();
      }
    }
    return map.put(key, value);
  }

  @Override
  @SneakyThrows({SQLException.class})
  public V remove(Object key) {
    try (PreparedStatement preparedStatement = connection.prepareStatement(String.format("DELETE FROM %s WHERE id=?;", tableName))) {
      preparedStatement.setString(1, key.toString());
      preparedStatement.execute();
    }
    return map.remove(key);
  }

  @Override
  public void putAll(Map<? extends UUID, ? extends V> m) {
    // TODO one prepared statement for all (not transactional until then)
    for (Entry<? extends UUID, ? extends V> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  @SneakyThrows({SQLException.class})
  public void clear() {
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(String.format("TRUNCATE %s", tableName));
    }
    synchronized (this) {
      this.notify();
    }
    map.clear();
  }

  @Override
  public synchronized void notification(int processId, String channelName, String payload) {
    // can't do update on this thread (probably?). Trigger update
    this.notify();  //TODO test notify while already updating
  }

  @Override
  public void close() throws IOException {
    updateThread = false;
    connection.removeNotificationListener(this);
    thread.interrupt();
  }

  // Generic MAP delegation

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return map.get(key);
  }

  @Override
  public Set<UUID> keySet() {
    return map.keySet();
  }

  @Override
  public Collection<V> values() {
    return map.values();
  }

  @Override
  public Set<Entry<UUID, V>> entrySet() {
    return map.entrySet();
  }
}
