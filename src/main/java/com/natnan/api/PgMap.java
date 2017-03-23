package com.natnan.api;

import com.google.common.collect.ImmutableMap;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Setter;
import lombok.SneakyThrows;

// TODO rename?
// TODO logging & exceptions
// TODO the value objects must not be mutable from outside in order to avoid unsynched states. Enforcing immutability doesn't seem to be an option. Maybe use Kryo to return deep copy of the objects
public class PgMap<T> implements Map<UUID, T>, PGNotificationListener, Closeable {

  private final PGConnection connection; // TODO handle reconnection etc.
  private final TableMapper<T> tableMapper;

  private boolean updateThread = true;
  private AutoResetEvent event = new AutoResetEvent(false);
  private final Thread thread;
  @Setter
  private MapUpdatedNotification mapUpdatedNotification;

  private ImmutableMap<UUID, T> map = ImmutableMap.of();

  private PgMap(PGConnection connection, TableMapper<T> tableMapper) {
    this.connection = connection;
    this.tableMapper = tableMapper;
    this.thread = new Thread(this::updateMapThread);
  }

  private void start() throws SQLException {
    // TODO test same two objects at the same time
    updateMap();
    try (Statement statement = connection.createStatement()) {
      statement.execute(String.format("LISTEN %s", "test_data")); // TODO hardcoded :(
    }
    connection.addNotificationListener(this);
    thread.start();
  }

  public static <T> PgMap<T> createSyncMap(PGConnection connection, TableMapper<T> tableMapper) throws SQLException {
    PgMap<T> map = new PgMap<>(connection, tableMapper);
    map.start();
    return map;
  }

  @SneakyThrows({SQLException.class})
  private void updateMapThread() {
    while (updateThread) {
      try {
        event.waitOne();
        updateMap();
        event.reset();
        if (mapUpdatedNotification != null) {
          mapUpdatedNotification.call();
        }
      } catch (InterruptedException ignored) {
      }
    }
  }

  private void updateMap() throws SQLException {
    ImmutableMap.Builder<UUID, T> builder = ImmutableMap.builder();
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(tableMapper.getQueryString());
      while (resultSet.next()) {
        Entry<UUID, T> entry = tableMapper.mapSingleResult(resultSet);
        builder.put(entry.getKey(), entry.getValue());
      }
    }
    this.map = builder.build();
  }

  @Override
  public void notification(int processId, String channelName, String payload) {
    // can't do update on this thread (probably?). Trigger update
    event.set();
  }

  @Override
  public void close() throws IOException {
    updateThread = false;
    connection.removeNotificationListener(this);
    thread.interrupt();
  }

  // Generic MAP delegation

  @Override
  public T put(UUID key, T value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends UUID, ? extends T> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

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
  public T get(Object key) {
    return map.get(key);
  }

  @Override
  public Set<UUID> keySet() {
    return map.keySet();
  }

  @Override
  public Collection<T> values() {
    return map.values();
  }

  @Override
  public Set<Entry<UUID, T>> entrySet() {
    return map.entrySet();
  }
}
