package abc.java.sql;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * An object that represents a precompiled SQL statement. <P>A SQL statement is precompiled and stored in a
 * <code>NamedParameterPreparedStatement</code> object.
 * This object can then be used to efficiently execute this statement multiple times.
 *
 * @autor abc
 * @modified abc on 08.03.2016.
 */
public class NamedParameterPreparedStatement implements PreparedStatement
{
   private final Map<String, Integer> _indexMap;
   private final PreparedStatement _statement;

   public NamedParameterPreparedStatement(PreparedStatement statement, Map<String, Integer> indexMap)
   {
      _indexMap = indexMap;
      _statement = statement;
   }

   public PreparedStatement getStatement()
   {
      return _statement;
   }

   @Override
   public ResultSet executeQuery() throws SQLException
   {
      return _statement.executeQuery();
   }

   @Override
   public int executeUpdate() throws SQLException
   {
      return _statement.executeUpdate();
   }

   public int executeUpdateAndClose() throws SQLException
   {
      int result = _statement.executeUpdate();
      _statement.close();
      return result;
   }

   @Override
   public void setNull(int parameterIndex, int sqlType) throws SQLException
   {
      _statement.setNull(parameterIndex, sqlType);
   }

   @Override
   public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
   {
      _statement.setNull(parameterIndex, sqlType, typeName);
   }

   @Override
   public void setBoolean(int parameterIndex, boolean x) throws SQLException
   {
      _statement.setBoolean(parameterIndex, x);
   }

   @Override
   public void setByte(int parameterIndex, byte x) throws SQLException
   {
      _statement.setByte(parameterIndex, x);
   }

   @Override
   public void setShort(int parameterIndex, short x) throws SQLException
   {
      _statement.setShort(parameterIndex, x);
   }

   @Override
   public void setInt(int parameterIndex, int x) throws SQLException
   {
      _statement.setInt(parameterIndex, x);
   }

   @Override
   public void setLong(int parameterIndex, long x) throws SQLException
   {
      _statement.setLong(parameterIndex, x);
   }

   @Override
   public void setFloat(int parameterIndex, float x) throws SQLException
   {
      _statement.setFloat(parameterIndex, x);
   }

   @Override
   public void setDouble(int parameterIndex, double x) throws SQLException
   {
      _statement.setDouble(parameterIndex, x);
   }

   @Override
   public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
   {
      _statement.setBigDecimal(parameterIndex, x);
   }

   @Override
   public void setString(int parameterIndex, String x) throws SQLException
   {
      _statement.setString(parameterIndex, x);
   }

   @Override
   public void setBytes(int parameterIndex, byte[] x) throws SQLException
   {
      _statement.setBytes(parameterIndex, x);
   }

   @Override
   public void setDate(int parameterIndex, Date x) throws SQLException
   {
      _statement.setDate(parameterIndex, x);
   }

   @Override
   public void setTime(int parameterIndex, Time x) throws SQLException
   {
      _statement.setTime(parameterIndex, x);
   }

   @Override
   public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
   {
      _statement.setTimestamp(parameterIndex, x);
   }

   @Override
   public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
   {
      _statement.setTimestamp(parameterIndex, x, cal);
   }

   public void setLocalDateTime(int parameterIndex, LocalDateTime x) throws SQLException
   {
      _statement.setTimestamp(parameterIndex, Timestamp.valueOf(x));
   }

   @Override
   public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
   {
      _statement.setAsciiStream(parameterIndex, x, length);
   }

   @Override
   public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
   {
      _statement.setUnicodeStream(parameterIndex, x, length);
   }

   @Override
   public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
   {
      _statement.setBinaryStream(parameterIndex, x, length);
   }

   @Override
   public void clearParameters() throws SQLException
   {
      _statement.clearParameters();
   }

   @Override
   public void setObject(int parameterIndex, Object x) throws SQLException
   {
      _statement.setObject(parameterIndex, x);
   }

   @Override
   public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
   {
      _statement.setObject(parameterIndex, x, targetSqlType);
   }

   @Override
   public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
   {
      _statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
   }

   @Override
   public boolean execute() throws SQLException
   {
      return _statement.execute();
   }

   public boolean executeAndClose() throws SQLException
   {
      boolean result = _statement.execute();
      _statement.close();
      return result;
   }

   @Override
   public void addBatch() throws SQLException
   {
      _statement.addBatch();
   }

   @Override
   public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
   {
      _statement.setCharacterStream(parameterIndex, reader, length);
   }

   @Override
   public void setRef(int parameterIndex, Ref x) throws SQLException
   {
      _statement.setRef(parameterIndex, x);
   }

   @Override
   public void setBlob(int parameterIndex, Blob x) throws SQLException
   {
      _statement.setBlob(parameterIndex, x);
   }

   @Override
   public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
   {
      _statement.setBlob(parameterIndex, inputStream);
   }

   @Override
   public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
   {
      _statement.setBlob(parameterIndex, inputStream, length);
   }

   @Override
   public void setClob(int parameterIndex, Clob x) throws SQLException
   {
      _statement.setClob(parameterIndex, x);
   }

   @Override
   public void setClob(int parameterIndex, Reader reader) throws SQLException
   {
      _statement.setClob(parameterIndex, reader);
   }

   @Override
   public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
   {
      _statement.setClob(parameterIndex, reader, length);
   }

   @Override
   public void setArray(int parameterIndex, Array x) throws SQLException
   {
      _statement.setArray(parameterIndex, x);
   }

   @Override
   public ResultSetMetaData getMetaData() throws SQLException
   {
      return _statement.getMetaData();
   }

   @Override
   public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
   {
      _statement.setDate(parameterIndex, x, cal);
   }

   @Override
   public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
   {
      _statement.setTime(parameterIndex, x, cal);
   }

   @Override
   public void setURL(int parameterIndex, URL x) throws SQLException
   {
      _statement.setURL(parameterIndex, x);
   }

   @Override
   public ParameterMetaData getParameterMetaData() throws SQLException
   {
      return _statement.getParameterMetaData();
   }

   @Override
   public void setRowId(int parameterIndex, RowId x) throws SQLException
   {
      _statement.setRowId(parameterIndex, x);
   }

   @Override
   public void setNString(int parameterIndex, String value) throws SQLException
   {
      _statement.setNString(parameterIndex, value);
   }

   @Override
   public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
   {
      _statement.setNCharacterStream(parameterIndex, value, length);
   }

   @Override
   public void setNClob(int parameterIndex, NClob value) throws SQLException
   {
      _statement.setNClob(parameterIndex, value);
   }

   @Override
   public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
   {
      _statement.setNClob(parameterIndex, reader, length);
   }

   @Override
   public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
   {
      _statement.setSQLXML(parameterIndex, xmlObject);
   }

   @Override
   public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
   {
      _statement.setAsciiStream(parameterIndex, x, length);
   }

   @Override
   public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
   {
      _statement.setBinaryStream(parameterIndex, x, length);
   }

   @Override
   public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
   {
      _statement.setCharacterStream(parameterIndex, reader, length);
   }

   @Override
   public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
   {
      _statement.setAsciiStream(parameterIndex, x);
   }

   @Override
   public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
   {
      _statement.setBinaryStream(parameterIndex, x);
   }

   @Override
   public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
   {
      _statement.setCharacterStream(parameterIndex, reader);
   }

   @Override
   public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
   {
      _statement.setNCharacterStream(parameterIndex, value);
   }

   @Override
   public void setNClob(int parameterIndex, Reader reader) throws SQLException
   {
      _statement.setNClob(parameterIndex, reader);
   }

   @Override
   public ResultSet executeQuery(String sql) throws SQLException
   {
      return _statement.executeQuery(sql);
   }

   @Override
   public int executeUpdate(String sql) throws SQLException
   {
      return _statement.executeUpdate(sql);
   }

   public int executeUpdateAndClose(String sql) throws SQLException
   {
      int result = _statement.executeUpdate(sql);
      _statement.close();
      return result;
   }

   @Override
   public void close() throws SQLException
   {
      _statement.close();
   }

   @Override
   public int getMaxFieldSize() throws SQLException
   {
      return _statement.getMaxFieldSize();
   }

   @Override
   public void setMaxFieldSize(int max) throws SQLException
   {
      _statement.setMaxFieldSize(max);
   }

   @Override
   public int getMaxRows() throws SQLException
   {
      return _statement.getMaxRows();
   }

   @Override
   public void setMaxRows(int max) throws SQLException
   {
      _statement.setMaxRows(max);
   }

   @Override
   public void setEscapeProcessing(boolean enable) throws SQLException
   {
      _statement.setEscapeProcessing(enable);
   }

   @Override
   public int getQueryTimeout() throws SQLException
   {
      return _statement.getQueryTimeout();
   }

   @Override
   public void setQueryTimeout(int seconds) throws SQLException
   {
      _statement.setQueryTimeout(seconds);
   }

   @Override
   public void cancel() throws SQLException
   {
      _statement.cancel();
   }

   @Override
   public SQLWarning getWarnings() throws SQLException
   {
      return _statement.getWarnings();
   }

   @Override
   public void clearWarnings() throws SQLException
   {
      _statement.clearWarnings();
   }

   @Override
   public void setCursorName(String name) throws SQLException
   {
      _statement.setCursorName(name);
   }

   @Override
   public boolean execute(String sql) throws SQLException
   {
      return _statement.execute(sql);
   }

   public boolean executeAndClose(String sql) throws SQLException
   {
      boolean result = _statement.execute(sql);
      _statement.close();
      return result;
   }

   @Override
   public ResultSet getResultSet() throws SQLException
   {
      return _statement.getResultSet();
   }

   @Override
   public int getUpdateCount() throws SQLException
   {
      return _statement.getUpdateCount();
   }

   @Override
   public boolean getMoreResults() throws SQLException
   {
      return _statement.getMoreResults();
   }

   @Override
   public void setFetchDirection(int direction) throws SQLException
   {
      _statement.setFetchDirection(direction);
   }

   @Override
   public int getFetchDirection() throws SQLException
   {
      return _statement.getFetchDirection();
   }

   @Override
   public void setFetchSize(int rows) throws SQLException
   {
      _statement.setFetchSize(rows);
   }

   @Override
   public int getFetchSize() throws SQLException
   {
      return _statement.getFetchSize();
   }

   @Override
   public int getResultSetConcurrency() throws SQLException
   {
      return _statement.getResultSetConcurrency();
   }

   @Override
   public int getResultSetType() throws SQLException
   {
      return _statement.getResultSetType();
   }

   @Override
   public void addBatch(String sql) throws SQLException
   {
      _statement.addBatch(sql);
   }

   @Override
   public void clearBatch() throws SQLException
   {
      _statement.clearBatch();
   }

   @Override
   public int[] executeBatch() throws SQLException
   {
      return _statement.executeBatch();
   }

   @Override
   public Connection getConnection() throws SQLException
   {
      return _statement.getConnection();
   }

   @Override
   public boolean getMoreResults(int current) throws SQLException
   {
      return _statement.getMoreResults(current);
   }

   @Override
   public ResultSet getGeneratedKeys() throws SQLException
   {
      return _statement.getGeneratedKeys();
   }

   @Override
   public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
   {
      return _statement.executeUpdate(sql, autoGeneratedKeys);
   }

   @Override
   public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
   {
      return _statement.executeUpdate(sql, columnIndexes);
   }

   @Override
   public int executeUpdate(String sql, String[] columnNames) throws SQLException
   {
      return _statement.executeUpdate(sql, columnNames);
   }

   @Override
   public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
   {
      return _statement.execute(sql, autoGeneratedKeys);
   }

   @Override
   public boolean execute(String sql, int[] columnIndexes) throws SQLException
   {
      return _statement.execute(sql, columnIndexes);
   }

   @Override
   public boolean execute(String sql, String[] columnNames) throws SQLException
   {
      return _statement.execute(sql, columnNames);
   }

   @Override
   public int getResultSetHoldability() throws SQLException
   {
      return _statement.getResultSetHoldability();
   }

   @Override
   public boolean isClosed() throws SQLException
   {
      return _statement.isClosed();
   }

   @Override
   public void setPoolable(boolean poolable) throws SQLException
   {
      _statement.setPoolable(poolable);
   }

   @Override
   public boolean isPoolable() throws SQLException
   {
      return _statement.isPoolable();
   }

   @Override
   public void closeOnCompletion() throws SQLException
   {
      _statement.closeOnCompletion();
   }

   @Override
   public boolean isCloseOnCompletion() throws SQLException
   {
      return _statement.isCloseOnCompletion();
   }

   @Override
   public <T> T unwrap(Class<T> iface) throws SQLException
   {
      return _statement.unwrap(iface);
   }

   @Override
   public boolean isWrapperFor(Class<?> iface) throws SQLException
   {
      return _statement.isWrapperFor(iface);
   }

   /**
    * Returns the index for a parameter.
    * @param name    Parameter name
    * @return Parameter index
    * @throws IllegalArgumentException    If the parameter does not exist
    */
   protected int getIndex(String name)
   {
      Integer index = _indexMap.get(name);
      if (index == null) throw new IllegalArgumentException("Parameter not found: " + name);

      return index.intValue();
   }

   /**
    * Sets a parameter.
    * @param parameterName    parameter name
    * @param value            parameter value
    * @throws SQLException                if an error occurred
    * @throws IllegalArgumentException    if the parameter does not exist
    * @see PreparedStatement#setObject(int, java.lang.Object)
    */
   public void setObject(String parameterName, Object value) throws SQLException
   {
      _statement.setObject(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName    parameter name
    * @param value            parameter value
    * @param targetSqlType
    * @throws SQLException
    */
   public void setObject(String parameterName, Object value, int targetSqlType) throws SQLException
   {
      _statement.setObject(getIndex(parameterName), value, targetSqlType);
   }

   /**
    * Sets a parameter.
    * @param parameterName    parameter name
    * @param value            parameter value
    * @param targetSqlType
    * @param scaleOrLength
    * @throws SQLException
    */
   public void setObject(String parameterName, Object value, int targetSqlType, int scaleOrLength) throws SQLException
   {
      _statement.setObject(getIndex(parameterName), value, targetSqlType, scaleOrLength);
   }

   /**
    * Sets a parameter.
    * @param parameterName    parameter name
    * @param value            parameter value
    * @throws SQLException    if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    * @see PreparedStatement#setString(int, java.lang.String)
    */
   public void setString(String parameterName, String value) throws SQLException
   {
      _statement.setString(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName    parameter name
    * @param value            parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setNString(String parameterName, String value) throws SQLException
   {
      _statement.setNString(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName    parameter name
    * @param value            parameter value
    * @throws SQLException    if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    * @see PreparedStatement#setInt(int, int)
    */
   public void setInt(String parameterName, int value) throws SQLException
   {
      _statement.setInt(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName    parameter name
    * @param value            parameter value
    * @throws SQLException    if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    * @see PreparedStatement#setInt(int, int)
    */
   public void setInt(String parameterName, Integer value) throws SQLException
   {
      _statement.setInt(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName    parameter name
    * @param value            parameter value
    * @throws SQLException    if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    * @see PreparedStatement#setInt(int, int)
    */
   public void setLong(String parameterName, long value) throws SQLException
   {
      _statement.setLong(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName  parameter name
    * @param value parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    * @see PreparedStatement#setInt(int, int)
    */
   public void setLong(String parameterName, Long value) throws SQLException
   {
      _statement.setLong(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName  parameter name
    * @param value parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    * @see PreparedStatement#setInt(int, int)
    */
   public void setBigDecimal(String parameterName, BigDecimal value) throws SQLException
   {
      _statement.setBigDecimal(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName  parameter name
    * @param value parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    * @see PreparedStatement#setTimestamp(int, java.sql.Timestamp)
    */
   public void setTimestamp(String parameterName, Timestamp value) throws SQLException
   {
      _statement.setTimestamp(getIndex(parameterName), value);
   }

   /**
    *
    * @param parameterName
    * @param x
    * @param cal
    * @throws SQLException
    */
   public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException
   {
      _statement.setTimestamp(getIndex(parameterName), x, cal);
   }

   /**
    * Sets a parameter.
    * @param parameterName  parameter name
    * @param value parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    * @see PreparedStatement#setTimestamp(int, java.sql.Timestamp)
    */
   public void setLocalDateTime(String parameterName, LocalDateTime value) throws SQLException
   {
      _statement.setTimestamp(getIndex(parameterName), Timestamp.valueOf(value));
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setURL(String parameterName, URL value) throws SQLException
   {
      _statement.setURL(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param sqlType       parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setNull(String parameterName, int sqlType) throws SQLException
   {
      _statement.setNull(getIndex(parameterName), sqlType);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param sqlType       parameter value
    * @param typeName      parameter type
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setNull(String parameterName, int sqlType, String typeName) throws SQLException
   {
      _statement.setNull(getIndex(parameterName), sqlType, typeName);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setBoolean(String parameterName, boolean value) throws SQLException
   {
      _statement.setBoolean(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setByte(String parameterName, byte value) throws SQLException
   {
      _statement.setByte(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setBytes(String parameterName, byte[] value) throws SQLException
   {
      _statement.setBytes(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setShort(String parameterName, short value) throws SQLException
   {
      _statement.setShort(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setFloat(String parameterName, float value) throws SQLException
   {
      _statement.setFloat(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setDouble(String parameterName, double value) throws SQLException
   {
      _statement.setDouble(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setDate(String parameterName, Date value) throws SQLException
   {
      _statement.setDate(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setDate(String parameterName, Date value, Calendar cal) throws SQLException
   {
      _statement.setDate(getIndex(parameterName), value, cal);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setTime(String parameterName, Time value) throws SQLException
   {
      _statement.setTime(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setTime(String parameterName, Time value, Calendar cal) throws SQLException
   {
      _statement.setTime(getIndex(parameterName), value, cal);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setBlob(String parameterName, Blob value) throws SQLException
   {
      _statement.setBlob(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param inputStream
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setBlob(String parameterName, InputStream inputStream) throws SQLException
   {
      _statement.setBlob(getIndex(parameterName), inputStream);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param inputStream
    * @param length
    * @throws SQLException
    */
   public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException
   {
      _statement.setBlob(getIndex(parameterName), inputStream, length);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setClob(String parameterName, Clob value) throws SQLException
   {
      _statement.setClob(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param reader
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setClob(String parameterName, Reader reader) throws SQLException
   {
      _statement.setClob(getIndex(parameterName), reader);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param reader
    * @param length
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setClob(String parameterName, Reader reader, long length) throws SQLException
   {
      _statement.setClob(getIndex(parameterName), reader, length);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param value         parameter value
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setNClob(String parameterName, NClob value) throws SQLException
   {
      _statement.setNClob(getIndex(parameterName), value);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param reader
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setNClob(String parameterName, Reader reader) throws SQLException
   {
      _statement.setNClob(getIndex(parameterName), reader);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param reader
    * @param length
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setNClob(String parameterName, Reader reader, long length) throws SQLException
   {
      _statement.setNClob(getIndex(parameterName), reader, length);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param x
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setAsciiStream(String parameterName, InputStream x) throws SQLException
   {
      _statement.setAsciiStream(getIndex(parameterName), x);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param x
    * @param length
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException
   {
      _statement.setAsciiStream(getIndex(parameterName), x, length);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param x
    * @param length
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException
   {
      _statement.setAsciiStream(getIndex(parameterName), x, length);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param reader
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setCharacterStream(String parameterName, Reader reader) throws SQLException
   {
      _statement.setCharacterStream(getIndex(parameterName), reader);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param reader
    * @param length
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException
   {
      _statement.setCharacterStream(getIndex(parameterName), reader, length);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param reader
    * @param length
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException
   {
      _statement.setCharacterStream(getIndex(parameterName), reader, length);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param reader
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setNCharacterStream(String parameterName, Reader reader) throws SQLException
   {
      _statement.setNCharacterStream(getIndex(parameterName), reader);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param reader
    * @param length
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setNCharacterStream(String parameterName, Reader reader, long length) throws SQLException
   {
      _statement.setNCharacterStream(getIndex(parameterName), reader, length);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param x
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setBinaryStream(String parameterName, InputStream x) throws SQLException
   {
      _statement.setBinaryStream(getIndex(parameterName), x);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param x
    * @param length
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException
   {
      _statement.setBinaryStream(getIndex(parameterName), x, length);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param x
    * @param length
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException
   {
      _statement.setBinaryStream(getIndex(parameterName), x, length);
   }

   /**
    * Sets a parameter.
    * @param parameterName parameter name
    * @param x
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setRowId(String parameterName, RowId x) throws SQLException
   {
      _statement.setRowId(getIndex(parameterName), x);
   }

   /**
    *
    * @param parameterName
    * @param xmlObject
    * @throws SQLException if an error occurred
    * @throws IllegalArgumentException if the parameter does not exist
    */
   public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException
   {
      _statement.setSQLXML(getIndex(parameterName), xmlObject);
   }

   /**
    * Parse the query string containing named parameters and result a parse result, which holds
    * the parsed sql (named parameters replaced by standard '?' parameters and an ordered list of the
    * named parameters.
    *
    * SQL parsing code borrowed from Axiom Data Science
    * See <a href="https://github.com/axiom-data-science/jdbc-named-parameters.git">https://github.com/axiom-data-science/jdbc-named-parameters.git</a>
    *
    * @param query Query containing named parameters
    * @param indexMap prameters map
    * @return
    * @throws SQLException if an error occurred
    */
   protected static String parseSql(String query, Map<String, Integer> indexMap)
         throws SQLException
   {
      int length = query.length();
      StringBuilder parsedQuery = new StringBuilder(length);
      int index = 1;

      boolean inSingleQuote = false;
      boolean inDoubleQuote = false;
      boolean inSingleLineComment = false;
      boolean inMultiLineComment = false;

      for (int i = 0; i < length; i++)
      {
         char c = query.charAt(i);
         if (inSingleQuote)
         {
            if (c == '\'') inSingleQuote = false;
         }
         else if (inDoubleQuote)
         {
            if (c == '"') inDoubleQuote = false;
         }
         else if (inMultiLineComment)
         {
            if (c == '*' && query.charAt(i + 1) == '/') inMultiLineComment = false;
         }
         else if (inSingleLineComment)
         {
            if (c == '\n') inSingleLineComment = false;
         }
         else
         {
            if (c == '\'')
            {
               inSingleQuote = true;
            }
            else if (c == '"')
            {
               inDoubleQuote = true;
            }
            else if (c == '/' && query.charAt(i + 1) == '*')
            {
               inMultiLineComment = true;
            }
            else if (c == '-' && query.charAt(i + 1) == '-')
            {
               inSingleLineComment = true;
            }
            else if (c == ':' && i + 1 < length && Character.isJavaIdentifierStart(query.charAt(i + 1)))
            {
               int j = i + 2;
               while (j < length && Character.isJavaIdentifierPart(query.charAt(j))) j++;

               String name = query.substring(i + 1, j);

               if (indexMap.containsKey(name)) throw new SQLException("Parameter name must be unique");

               indexMap.put(name, index++);
               c = '?';             // replace the parameter with a question mark
               i += name.length();  // skip past the end if the parameter
            }
         }
         parsedQuery.append(c);
      }
      return parsedQuery.toString();
   }

   /**
    * Create NamedParameterPreparedStatement object
    * @param c                   the database connection
    * @param sql                 the parameterized query
    * @return
    * @throws SQLException if the statement could not be created
    */
   public static NamedParameterPreparedStatement create(Connection c, String sql)
         throws SQLException
   {
      HashMap<String, Integer> indexMap = new HashMap<>();
      String preparedSql = parseSql(sql, indexMap);
      PreparedStatement statement = c.prepareStatement(preparedSql);
      return new NamedParameterPreparedStatement(statement, indexMap);
   }
}
