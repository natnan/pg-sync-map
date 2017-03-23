package com.natnan.api;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public interface TableMapper<T> {

  String getQueryString();

  Map.Entry<UUID, T> mapSingleResult(ResultSet resultSet) throws SQLException;
}
