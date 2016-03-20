package abc.java.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

/**
 * Postgres DB utils
 * @autor abc
 * @modified abc on 20.03.2016.
 */
public class DbUtilsPg extends DbUtils
{
   private static final Logger _log = LoggerFactory.getLogger(DbUtilsPg.class);
   private static DbUtilsPg _instance;

   public static synchronized DbUtilsPg getInstance()
   {
      if (_instance == null) _instance = new DbUtilsPg();
      return _instance;
   }

   @Override
   public Connection getConnection()
   {
      Connection result = super.getConnection();
      if (result == null)
      {
         String destinationDriver = "org.postgresql.Driver";
         String connectionString = "jdbc:postgresql://localhost:5432/dbname";
         String login = "db_role";
         String password = "password";

         Properties properties = new Properties();
         try
         {
            properties.load(new FileInputStream("src/test/resources/config.properties"));

            destinationDriver = properties.getProperty("postgres.connection.driver_class");
            connectionString = properties.getProperty("postgres.connection.url");
            login = properties.getProperty("postgres.connection.username");
            password = properties.getProperty("postgres.connection.password");
         }
         catch (IOException _noop) { }

         result = connect(destinationDriver, connectionString, login, password);
         if (result == null) _log.debug("Test ignored");
      }
      return result;
   }
}
