package abc.java.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

/**
 * Oracle DB utils
 * @autor abc
 * @modified abc on 20.03.2016.
 */
public class DbUtilsOra extends DbUtils
{
   private static final Logger _log = LoggerFactory.getLogger(DbUtilsOra.class);
   private static DbUtilsOra _instance;

   public static synchronized DbUtilsOra getInstance()
   {
      if (_instance == null) _instance = new DbUtilsOra();
      return _instance;
   }

   @Override
   public Connection getConnection()
   {
      Connection result = super.getConnection();
      if (result == null)
      {
         String destinationDriver = "oracle.jdbc.OracleDriver";
         String connectionString = "jdbc:oracle:oci:@DBNAME";
         String login = "db_schema";
         String password = "password";

         Properties properties = new Properties();
         try
         {
            properties.load(new FileInputStream("src/test/resources/config.properties"));

            destinationDriver = properties.getProperty("oracle.connection.driver_class");
            connectionString = properties.getProperty("oracle.connection.url");
            login = properties.getProperty("oracle.connection.username");
            password = properties.getProperty("oracle.connection.password");
         }
         catch (IOException _noop) { }

         result = connect(destinationDriver, connectionString, login, password);
         if (result == null) _log.debug("Test ignored");
      }
      return result;
   }
}
