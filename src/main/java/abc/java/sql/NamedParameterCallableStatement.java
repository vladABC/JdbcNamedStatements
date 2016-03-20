package abc.java.sql;

import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @autor abc
 * @modified abc on 07.03.2016.
 */
public class NamedParameterCallableStatement extends NamedParameterPreparedStatement implements CallableStatement
{
   private final CallableStatement _statement;

   public NamedParameterCallableStatement(CallableStatement statement, Map<String, Integer> indexMap)
   {
      super(statement, indexMap);

      _statement = statement;
   }

   public CallableStatement getStatement()
   {
      return _statement;
   }

   @Override
   public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException
   {
      _statement.registerOutParameter(parameterIndex, sqlType);
   }

   @Override
   public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException
   {
      _statement.registerOutParameter(parameterIndex, sqlType, scale);
   }

   @Override
   public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException
   {
      _statement.registerOutParameter(parameterIndex, sqlType, typeName);
   }

   @Override
   public void registerOutParameter(String parameterName, int sqlType) throws SQLException
   {
      _statement.registerOutParameter(getIndex(parameterName), sqlType);
   }

   @Override
   public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException
   {
      _statement.registerOutParameter(getIndex(parameterName), sqlType, scale);
   }

   @Override
   public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException
   {
      _statement.registerOutParameter(getIndex(parameterName), sqlType, typeName);
   }

   @Override
   public boolean wasNull() throws SQLException
   {
      return _statement.wasNull();
   }

   @Override
   public String getString(int parameterIndex) throws SQLException
   {
      return _statement.getString(parameterIndex);
   }

   @Override
   public String getString(String parameterName) throws SQLException
   {
      return _statement.getString(getIndex(parameterName));
   }

   @Override
   public boolean getBoolean(int parameterIndex) throws SQLException
   {
      return _statement.getBoolean(parameterIndex);
   }

   @Override
   public boolean getBoolean(String parameterName) throws SQLException
   {
      return _statement.getBoolean(getIndex(parameterName));
   }

   @Override
   public byte getByte(int parameterIndex) throws SQLException
   {
      return _statement.getByte(parameterIndex);
   }

   @Override
   public byte getByte(String parameterName) throws SQLException
   {
      return _statement.getByte(getIndex(parameterName));
   }

   @Override
   public short getShort(int parameterIndex) throws SQLException
   {
      return _statement.getShort(parameterIndex);
   }

   @Override
   public short getShort(String parameterName) throws SQLException
   {
      return _statement.getShort(getIndex(parameterName));
   }

   @Override
   public int getInt(int parameterIndex) throws SQLException
   {
      return _statement.getInt(parameterIndex);
   }

   @Override
   public int getInt(String parameterName) throws SQLException
   {
      return _statement.getInt(getIndex(parameterName));
   }

   @Override
   public long getLong(int parameterIndex) throws SQLException
   {
      return _statement.getLong(parameterIndex);
   }

   @Override
   public long getLong(String parameterName) throws SQLException
   {
      return _statement.getLong(getIndex(parameterName));
   }

   @Override
   public float getFloat(int parameterIndex) throws SQLException
   {
      return _statement.getFloat(parameterIndex);
   }

   @Override
   public float getFloat(String parameterName) throws SQLException
   {
      return _statement.getFloat(getIndex(parameterName));
   }

   @Override
   public double getDouble(int parameterIndex) throws SQLException
   {
      return _statement.getDouble(parameterIndex);
   }

   @Override
   public double getDouble(String parameterName) throws SQLException
   {
      return _statement.getFloat(getIndex(parameterName));
   }

   @Override
   public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException
   {
      return _statement.getBigDecimal(parameterIndex);
   }

   @Override
   public byte[] getBytes(int parameterIndex) throws SQLException
   {
      return _statement.getBytes(parameterIndex);
   }

   @Override
   public byte[] getBytes(String parameterName) throws SQLException
   {
      return _statement.getBytes(getIndex(parameterName));
   }

   @Override
   public Date getDate(int parameterIndex) throws SQLException
   {
      return _statement.getDate(parameterIndex);
   }

   @Override
   public Date getDate(String parameterName) throws SQLException
   {
      return _statement.getDate(getIndex(parameterName));
   }

   @Override
   public Time getTime(int parameterIndex) throws SQLException
   {
      return _statement.getTime(parameterIndex);
   }

   @Override
   public Time getTime(String parameterName) throws SQLException
   {
      return _statement.getTime(getIndex(parameterName));
   }

   @Override
   public Timestamp getTimestamp(int parameterIndex) throws SQLException
   {
      return _statement.getTimestamp(parameterIndex);
   }

   @Override
   public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException
   {
      return _statement.getTimestamp(parameterIndex, cal);
   }

   @Override
   public Timestamp getTimestamp(String parameterName) throws SQLException
   {
      return _statement.getTimestamp(getIndex(parameterName));
   }

   @Override
   public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException
   {
      return _statement.getTimestamp(getIndex(parameterName), cal);
   }

   public LocalDateTime getLocalDateTime(int parameterIndex) throws SQLException
   {
      LocalDateTime result = null;
      Timestamp t = _statement.getTimestamp(parameterIndex);
      if (t != null) result = t.toLocalDateTime();
      return result;
   }

   public LocalDateTime getLocalDateTime(int parameterIndex, Calendar cal) throws SQLException
   {
      LocalDateTime result = null;
      Timestamp t = _statement.getTimestamp(parameterIndex, cal);
      if (t != null) result = t.toLocalDateTime();
      return result;
   }

   public LocalDateTime getLocalDateTime(String parameterName) throws SQLException
   {
      return getLocalDateTime(getIndex(parameterName));
   }

   public LocalDateTime getLocalDateTime(String parameterName, Calendar cal) throws SQLException
   {
      return getLocalDateTime(getIndex(parameterName), cal);
   }

   @Override
   public BigDecimal getBigDecimal(int parameterIndex) throws SQLException
   {
      return _statement.getBigDecimal(parameterIndex);
   }

   @Override
   public BigDecimal getBigDecimal(String parameterName) throws SQLException
   {
      return _statement.getBigDecimal(getIndex(parameterName));
   }

   @Override
   public Object getObject(int parameterIndex) throws SQLException
   {
      return _statement.getObject(parameterIndex);
   }

   @Override
   public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException
   {
      return _statement.getObject(parameterIndex, map);
   }

   @Override
   public Object getObject(String parameterName) throws SQLException
   {
      return _statement.getObject(getIndex(parameterName));
   }

   @Override
   public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException
   {
      return _statement.getObject(getIndex(parameterName), map);
   }

   @Override
   public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException
   {
      return _statement.getObject(parameterIndex, type);
   }

   @Override
   public <T> T getObject(String parameterName, Class<T> type) throws SQLException
   {
      return _statement.getObject(getIndex(parameterName), type);
   }

   @Override
   public Ref getRef(int parameterIndex) throws SQLException
   {
      return _statement.getRef(parameterIndex);
   }

   @Override
   public Ref getRef(String parameterName) throws SQLException
   {
      return _statement.getRef(getIndex(parameterName));
   }

   @Override
   public Blob getBlob(int parameterIndex) throws SQLException
   {
      return _statement.getBlob(parameterIndex);
   }

   @Override
   public Blob getBlob(String parameterName) throws SQLException
   {
      return _statement.getBlob(getIndex(parameterName));
   }

   @Override
   public Clob getClob(int parameterIndex) throws SQLException
   {
      return _statement.getClob(parameterIndex);
   }

   @Override
   public Clob getClob(String parameterName) throws SQLException
   {
      return _statement.getClob(getIndex(parameterName));
   }

   @Override
   public Array getArray(int parameterIndex) throws SQLException
   {
      return _statement.getArray(parameterIndex);
   }

   @Override
   public Array getArray(String parameterName) throws SQLException
   {
      return _statement.getArray(getIndex(parameterName));
   }

   @Override
   public Date getDate(int parameterIndex, Calendar cal) throws SQLException
   {
      return _statement.getDate(parameterIndex, cal);
   }

   @Override
   public Date getDate(String parameterName, Calendar cal) throws SQLException
   {
      return _statement.getDate(getIndex(parameterName));
   }

   @Override
   public Time getTime(int parameterIndex, Calendar cal) throws SQLException
   {
      return _statement.getTime(parameterIndex, cal);
   }

   @Override
   public Time getTime(String parameterName, Calendar cal) throws SQLException
   {
      return _statement.getTime(getIndex(parameterName), cal);
   }

   @Override
   public URL getURL(int parameterIndex) throws SQLException
   {
      return _statement.getURL(parameterIndex);
   }

   @Override
   public URL getURL(String parameterName) throws SQLException
   {
      return _statement.getURL(getIndex(parameterName));
   }

   @Override
   public NClob getNClob(int parameterIndex) throws SQLException
   {
      return _statement.getNClob(parameterIndex);
   }

   @Override
   public NClob getNClob(String parameterName) throws SQLException
   {
      return _statement.getNClob(getIndex(parameterName));
   }

   @Override
   public RowId getRowId(int parameterIndex) throws SQLException
   {
      return _statement.getRowId(parameterIndex);
   }

   @Override
   public RowId getRowId(String parameterName) throws SQLException
   {
      return _statement.getRowId(getIndex(parameterName));
   }

   @Override
   public SQLXML getSQLXML(int parameterIndex) throws SQLException
   {
      return _statement.getSQLXML(parameterIndex);
   }

   @Override
   public SQLXML getSQLXML(String parameterName) throws SQLException
   {
      return _statement.getSQLXML(getIndex(parameterName));
   }

   @Override
   public String getNString(int parameterIndex) throws SQLException
   {
      return _statement.getNString(parameterIndex);
   }

   @Override
   public String getNString(String parameterName) throws SQLException
   {
      return _statement.getNString(getIndex(parameterName));
   }

   @Override
   public Reader getCharacterStream(int parameterIndex) throws SQLException
   {
      return _statement.getCharacterStream(parameterIndex);
   }

   @Override
   public Reader getCharacterStream(String parameterName) throws SQLException
   {
      return _statement.getCharacterStream(getIndex(parameterName));
   }

   @Override
   public Reader getNCharacterStream(int parameterIndex) throws SQLException
   {
      return _statement.getNCharacterStream(parameterIndex);
   }

   @Override
   public Reader getNCharacterStream(String parameterName) throws SQLException
   {
      return _statement.getNCharacterStream(getIndex(parameterName));
   }

   /**
    * Create NamedParameterCallableStatement object
    * @param c                   the database connection
    * @param sql                 the parameterized query
    * @return
    * @throws SQLException if the statement could not be created
    */
   public static NamedParameterCallableStatement create(Connection c, String sql)
         throws SQLException
   {
      HashMap<String, Integer> indexMap = new HashMap<>();
      String preparedSql = parseSql(sql, indexMap);
      return new NamedParameterCallableStatement(c.prepareCall(preparedSql), indexMap);
   }
}
