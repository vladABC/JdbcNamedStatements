package abc.java.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Random;

/**
 * Base DB utils
 * @autor abc
 * @modified abc on 12.03.2016.
 */
public class DbUtils
{
   private static final Logger _log = LoggerFactory.getLogger(DbUtils.class);

   private Connection _connection = null;

   public Connection getConnection()
   {
      return _connection;
   }

   /**
    * Connect to DB
    * @param destinationDriver   DB driver name
    * @param connectionString    DB connectrion string
    * @param login               user login
    * @param password            user password
    * @return
    */
   protected Connection connect(String destinationDriver, String connectionString, String login, String password)
   {
      Connection result = null;

      if (destinationDriver == null || connectionString == null || login == null || password == null ||
            destinationDriver.isEmpty() || connectionString.isEmpty() || login.isEmpty() || password.isEmpty())
         return result;

      try
      {
         Class.forName(destinationDriver);
      }
      catch (ClassNotFoundException e)
      {
         _log.error("Unable to load JDBC driver " + e.getMessage());
         throw ApplicationException.create("Unable to load JDBC driver " + e.getMessage());
      }

      try
      {
         result = _connection = DriverManager.getConnection(connectionString, login, password);
      }
      catch (SQLException e)
      {
         _log.error("Unable to create connection to database");
         _log.error(e.getMessage());
         throw ApplicationException.create("Unable to create connection to database " + e.getMessage());
      }

      return result;
   }

   public void disconnect()
   {
      if (_connection != null)
      {
         try
         {
            _connection.close();
         }
         catch (SQLException e)
         {
            _log.error(e.getMessage());
         }
      }
   }

   /**
    * Drop DB object
    * @param objectTypeName   DB object type
    * @param objectName       DB object name
    * @return
    */
   private boolean dropObject(String objectTypeName, String objectName)
   {
      String sql = "drop " + objectTypeName + " " + objectName;
      try
      {
         CallableStatement c = getConnection().prepareCall(sql);
         c.execute();
         c.close();
      }
      catch (SQLException e)
      {
         _log.error(e.getMessage());
         return false;
      }
      return true;
   }

   /**
    * Drop table
    * @param tableName  table name
    * @return
    */
   public boolean dropTable(String tableName)
   {
      return dropObject("table", tableName);
   }

   /**
    * Drop view
    * @param viewName   view name
    * @return
    */
   public boolean dropView(String viewName)
   {
      return dropObject("view", viewName);
   }

   /**
    * Drop dunction
    * @param functionName  function name
    * @return
    */
   public boolean dropFunction(String functionName)
   {
      return dropObject("function", functionName);
   }

   /**
    * Drop procedure
    * @param procedureName procedure name
    * @return
    */
   public boolean dropProcedure(String procedureName)
   {
      return dropObject("procedure", procedureName);
   }

   /**
    * Drop package
    * @param packageName
    * @return
    */
   public boolean dropPackage(String packageName)
   {
      return dropObject("package", packageName);
   }

   /**
    * Get table row count
    * @param tableName  table name
    * @return
    */
   public int getTableCount(String tableName)
   {
      int result = -1;
      try
      {
         String sql = "select count(*) from " + tableName;
         Statement stmt = getConnection().createStatement();
         ResultSet rs = stmt.executeQuery(sql);
         rs.next();
         result = rs.getInt(1);
         rs.close();
         stmt.close();
      }
      catch (SQLException e)
      {
         _log.error(e.getMessage());
      }
      return result;
   }

   /**
    * Delete rows from table
    * @param tableName     table name
    * @throws SQLException
    */
   public void deleteFromTable(String tableName) throws SQLException
   {
      String sql = "delete from " + tableName;
      CallableStatement c = getConnection().prepareCall(sql);
      c.execute();
      c.close();
   }

   /**
    * Generate test string
    * @param length
    * @return
    */
   public String generateString(int length)
   {
      String result = "";
      for (int i = 0; i <= length / 32; i++)
      {
         result += java.util.UUID.randomUUID().toString().replace("-", "");
      }
      return result.substring(0, length);
   }

   public int getMaxN1()
   {
      return 9;
   }

   public int getMaxN9()
   {
      return 999999999;
   }

   public long getMaxN18()
   {
      return 999999999999999999L;
   }

   public int generateID()
   {
      return (new Random()).nextInt(Integer.MAX_VALUE);
   }

   public long generateBigID()
   {
      return getMaxN18() - generateID();
   }

   public BigDecimal getMaxMoney()
   {
      return new BigDecimal("99999999999999.999999");
   }

   public BigDecimal getMaxFactor()
   {
      return new BigDecimal("9999999999.9999999999");
   }

   public BigDecimal getMaxPercent()
   {
      return new BigDecimal("999.999999999999999");
   }
}
