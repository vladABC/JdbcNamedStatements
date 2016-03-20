package abc.java.sql;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;

public class JdbcDriverTestPg
{
   private static final Logger _log = LoggerFactory.getLogger(JdbcDriverTestPg.class);

   private String _str1;
   private String _str32;
   private String _str48;
   private String _str128;
   private String _str250;
   private String _str1024;
   private String _str4000;

   private int _N1;
   private int _N9;
   private Long _N18;
   private BigDecimal _M;
   private BigDecimal _F;
   private BigDecimal _P;
   private LocalDateTime _date;

   public JdbcDriverTestPg()
   {
      DbUtilsPg db = DbUtilsPg.getInstance();

      _N1 = db.getMaxN1();
      _N9 = db.getMaxN9();
      _N18 = db.getMaxN18();
      _M = db.getMaxMoney();
      _F = db.getMaxFactor();
      _P = db.getMaxPercent();
      _date = LocalDateTime.now();

      _str32 = "V";
      _str32 = db.generateString(32);
      _str48 = db.generateString(48);
      _str128 = db.generateString(128);
      _str250 = db.generateString(250);
      _str1024 = db.generateString(1024);
      _str4000 = db.generateString(4000);
   }

   @BeforeClass
   public static void setUp()
   {
      DbUtilsPg db = DbUtilsPg.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("In setup");
      _log.debug("Connected");

      db.dropTable("TMP_TEST_TYPES");
      _log.debug("Table droped");

      try
      {
         String sql;

         //create test table
         sql = "create table TMP_TEST_TYPES (" +
               "F_N1            int4, " +
               "F_N9            int4, " +
               "F_N18           int8, " +
               "F_D             timestamptz, " +
               "F_C1            char(1), " +
               "F_VC32          varchar(32), " +
               "F_VC48          varchar(48), " +
               "F_VC128         varchar(128), " +
               "F_VC250         varchar(250), " +
               "F_VC1024        varchar(1024), " +
               "F_VC4000        varchar(4000), " +
               "F_T             text, " +
               "F_M             numeric(20,6), " +
               "F_F             numeric(20,10), " +
               "F_C             numeric(18,15));";

         Statement s = conn.createStatement();
         s.execute(sql);
         s.close();

         _log.debug("TMP_TEST_TYPES table created");

         // create func Insert
         sql = "create or replace function LoadRecordTestTypes (" +
               "   vF_N1           in int4, " +
               "   vF_N9           in int4, " +
               "   vF_N18          in int8, " +
               "   vF_D            in timestamptz, " +
               "   vF_C1           in char(1), " +
               "   vF_VC32         in varchar(32), " +
               "   vF_VC48         in varchar(48), " +
               "   vF_VC128        in varchar(128), " +
               "   vF_VC250        in varchar(250), " +
               "   vF_VC1024       in varchar(1024), " +
               "   vF_VC4000       in varchar(4000), " +
               "   vF_T            in text, " +
               "   vF_M            in numeric(20,6), " +
               "   vF_F            in numeric(20,10), " +
               "   vF_C            in numeric(18,15)) " +
               "returns void as \n" +
               "$$ \n declare " +
               "begin " +
               "   insert into TMP_TEST_TYPES " +
               "          (F_N1, F_N9, F_N18, F_D, F_C1, F_VC32, F_VC48, F_VC128, F_VC250, F_VC1024, F_VC4000, F_T, F_M, F_F, F_C) " +
               "   values (vF_N1, vF_N9, vF_N18, vF_D, vF_C1, vF_VC32, vF_VC48, vF_VC128, vF_VC250, vF_VC1024, vF_VC4000, vF_T, vF_M, vF_F, vF_C); " +
               "end; \n" +                                // <- magic here. dont work without \n
               "$$ language plpgsql security definer;";

         s = conn.createStatement();
         s.execute(sql);
         s.close();

         _log.debug("LoadRecordTestTypes procedure created");

         // create func Select
         sql = "create or replace function SelectTestTypes " +
               "  (vUserID        in numeric(10)) " +
               "returns refcursor as \n$$ " +
               "declare " +
               "   result_cursor refcursor; " +
               "begin " +
               "   open result_cursor for " +
               "   select * from TMP_TEST_TYPES; " +
               "   return result_cursor; " +
               "end; $$ \n" +
               "language plpgsql security definer;";

         s = conn.createStatement();
         s.execute(sql);
         s.close();

         _log.debug("LoadRecordTestTypes procedure created");

         // create func with result
         sql = "create or replace function SelectTestTypes2 " +
               "  (vUserID        in int4) " +
               "returns int4 as \n$$ " +
               "declare " +
               "begin " +
               "   return vUserID; " +
               "end; $$ \n" +
               "language plpgsql;";

         s = conn.createStatement();
         s.execute(sql);
         s.close();

         _log.debug("LoadRecordTestTypes 2 procedure created");

         sql = "create or replace function SelectTestTypes3 " +
               "  (vUserID         in     int4, " +
               "   vUserStr        in out varchar(250)) " +
               "as $$ declare " +
               "begin " +
               "   vUserStr := vUserStr || ' ' || vUserID; " +
               "end; $$ \n" +
               "language plpgsql;";

         s = conn.createStatement();
         s.execute(sql);
         s.close();

         _log.debug("LoadRecordTestTypes 3 procedure created");
      }
      catch (SQLException e)
      {
         _log.error(e.getMessage());
      }
   }

   @AfterClass
   public static void tearDown()
   {
      DbUtilsPg db = DbUtilsPg.getInstance();
      if (db.getConnection() == null) return;

      //dropTable("TMP_TEST_TYPES");
      _log.debug("TMP_TEST_TYPES table droped");

      Assert.assertTrue(db.dropFunction("SelectTestTypes (vUserID in numeric(10))"));
      _log.debug("SelectTestTypes procedure droped");

      Assert.assertTrue(db.dropFunction("LoadRecordTestTypes (" +
            "   vF_N1           in int4, " +
            "   vF_N9           in int4, " +
            "   vF_N18          in int8, " +
            "   vF_D            in timestamptz, " +
            "   vF_C1           in char(1), " +
            "   vF_VC32         in varchar(32), " +
            "   vF_VC48         in varchar(48), " +
            "   vF_VC128        in varchar(128), " +
            "   vF_VC250        in varchar(250), " +
            "   vF_VC1024       in varchar(1024), " +
            "   vF_VC4000       in varchar(4000), " +
            "   vF_T            in text, " +
            "   vF_M            in numeric(20,6), " +
            "   vF_F            in numeric(20,10), " +
            "   vF_C            in numeric(18,15))"));

      Assert.assertTrue(db.dropFunction("SelectTestTypes2(vUserID   in int4)"));

      _log.debug("LoadRecordTestTypes procedure droped");
   }

   public void testRow() throws SQLException
   {
      DbUtilsPg db = DbUtilsPg.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      // We must be inside a transaction for cursors to work.
      conn.setAutoCommit(false);

      // Procedure call.
      CallableStatement p = conn.prepareCall("{ ? = call SelectTestTypes(?) }");
      p.registerOutParameter(1, Types.OTHER);
      p.setInt(2, 1);
      p.execute();
      ResultSet r = (ResultSet) p.getObject(1);
      while (r.next())
      {
         Assert.assertEquals(_N1, r.getInt(1));
         Assert.assertEquals(_N1, r.getInt("F_N1"));

         Assert.assertEquals(_N9, r.getInt(2));
         Assert.assertEquals(_N9, r.getInt("F_N9"));

         Assert.assertEquals((long) _N18, r.getLong(3));
         Assert.assertEquals((long) _N18, r.getLong("F_N18"));

         // with nanoseconds
         Assert.assertEquals(_date, r.getTimestamp(4).toLocalDateTime());
         Assert.assertEquals(_date, r.getTimestamp("F_D").toLocalDateTime());

         Assert.assertEquals(_str1, r.getString(5));
         Assert.assertEquals(_str1, r.getString("F_C1"));

         Assert.assertEquals(_str32, r.getString(6));
         Assert.assertEquals(_str32, r.getString("F_VC32"));

         Assert.assertEquals(_str48, r.getString(7));
         Assert.assertEquals(_str48, r.getString("F_VC48"));

         Assert.assertEquals(_str128, r.getString(8));
         Assert.assertEquals(_str128, r.getString("F_VC128"));

         Assert.assertEquals(_str250, r.getString(9));
         Assert.assertEquals(_str250, r.getString("F_VC250"));

         Assert.assertEquals(_str1024, r.getString(10));
         Assert.assertEquals(_str1024, r.getString("F_VC1024"));

         Assert.assertEquals(_str4000, r.getString(11));
         Assert.assertEquals(_str4000, r.getString("F_VC4000"));

         Assert.assertEquals(_str1024, r.getString(12));
         Assert.assertEquals(_str1024, r.getString("F_T"));

         Assert.assertEquals(_M, r.getBigDecimal(13));
         Assert.assertEquals(_M, r.getBigDecimal("F_M"));

         Assert.assertEquals(_F, r.getBigDecimal(14));
         Assert.assertEquals(_F, r.getBigDecimal("F_F"));

         Assert.assertEquals(_P, r.getBigDecimal(15));
         Assert.assertEquals(_P, r.getBigDecimal("F_C"));
      }
      r.close();
      p.close();

      conn.setAutoCommit(true);
   }

   @Test
   public void testInsert() throws SQLException
   {
      DbUtilsPg db = DbUtilsPg.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testInsert");

      // Prepare
      db.deleteFromTable("TMP_TEST_TYPES");

      // Test
      String sql = "insert into TMP_TEST_TYPES(F_N1, F_N9, F_N18, F_D, " +
            "F_C1, F_VC32, F_VC48, F_VC128, F_VC250, F_VC1024, F_VC4000, F_T, F_M, F_F, F_C) " +
            "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + "?, ?, ?, ?, ?)";

      CallableStatement s = db.getConnection().prepareCall(sql);
      s.setInt(1, _N1);                     // F_N1
      s.setInt(2, _N9);                     // F_N9
      s.setLong(3, _N18);                   // F_N18
      s.setTimestamp(4, Timestamp.valueOf(_date)); // F_D
      s.setString(5, _str1);                // F_C1
      s.setString(6, _str32);               // F_VC32
      s.setString(7, _str48);               // F_VC48
      s.setString(8, _str128);              // F_VC128
      s.setString(9, _str250);              // F_VC250
      s.setString(10, _str1024);            // F_VC1024
      s.setString(11, _str4000);            // F_VC4000
      s.setString(12, _str1024);            // F_T
      s.setBigDecimal(13, _M);              // F_M
      s.setBigDecimal(14, _F);              // F_F
      s.setBigDecimal(15, _P);              // F_C

      s.execute();
      s.close();

      Assert.assertEquals("TMP_TEST_TYPES bad rows count", 1, db.getTableCount("TMP_TEST_TYPES"));
      _log.debug("Record added");

      // Test fields
      testRow();
   }

   @Test
   public void testInsertNamedParam() throws SQLException
   {
      DbUtilsPg db = DbUtilsPg.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testInsertNamedParam");

      // Prepare
      db.deleteFromTable("TMP_TEST_TYPES");

      // Test
      String sql = "insert into TMP_TEST_TYPES " +
            "              ( F_N1,  F_N9,  F_N18,  F_D,  F_C1,  F_VC32,  F_VC48,  F_VC128,  F_VC250,  F_VC1024, " +
            "                F_VC4000,  F_T,  F_M,  F_F,  F_C) " +
            "       values (:F_N1, :F_N9, :F_N18, :F_D, :F_C1, :F_VC32, :F_VC48, :F_VC128, :F_VC250, :F_VC1024, " +
            "               :F_VC4000, :F_T, :F_M, :F_F, :F_C)";

      NamedParameterPreparedStatement n = NamedParameterPreparedStatement.create(db.getConnection(), sql);
      n.setInt("F_N1", _N1);
      n.setInt("F_N9", _N9);
      n.setLong("F_N18", _N18);
      n.setLocalDateTime("F_D", _date);
      n.setString("F_C1", _str1);
      n.setString("F_VC32", _str32);
      n.setString("F_VC48", _str48);
      n.setString("F_VC128", _str128);
      n.setString("F_VC250", _str250);
      n.setString("F_VC1024", _str1024);
      n.setString("F_VC4000", _str4000);
      n.setString("F_T", _str1024);
      n.setBigDecimal("F_M", _M);
      n.setBigDecimal("F_F", _F);
      n.setBigDecimal("F_C", _P);

      n.executeAndClose();

      Assert.assertEquals("TMP_TEST_TYPES bad rows count", 1, db.getTableCount("TMP_TEST_TYPES"));
      _log.debug("Record added");

      // Test fields
      testRow();
   }

   @Test
   public void testProcParam() throws SQLException
   {
      DbUtilsPg db = DbUtilsPg.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testProcParam");

      // Prepare
      db.deleteFromTable("TMP_TEST_TYPES");

      // Test
      String sql = "select * from LoadRecordTestTypes(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?)";

      CallableStatement p = db.getConnection().prepareCall(sql);
      p.setInt(1, _N1);
      p.setInt(2, _N9);
      p.setLong(3, _N18);
      p.setTimestamp(4, Timestamp.valueOf(_date));
      p.setString(5, _str1);
      p.setString(6, _str32);
      p.setString(7, _str48);
      p.setString(8, _str128);
      p.setString(9, _str250);
      p.setString(10, _str1024);
      p.setString(11, _str4000);
      p.setString(12, _str1024);
      p.setBigDecimal(13, _M);
      p.setBigDecimal(14, _F);
      p.setBigDecimal(15, _P);

      p.execute();
      p.close();

      Assert.assertEquals("TMP_TEST_TYPES bad rows count", 1, db.getTableCount("TMP_TEST_TYPES"));
      _log.debug("Record added");

      // Test fields
      testRow();
   }

   @Test
   public void testProcNamedParam() throws SQLException
   {
      DbUtilsPg db = DbUtilsPg.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testProcNamedParam");

      // Prepare
      db.deleteFromTable("TMP_TEST_TYPES");

      // Test
      String sql = "select * from LoadRecordTestTypes(" +
            ":vF_N1, :vF_N9, :vF_N18, :vF_D, :vF_C1, :vF_VC32, :vF_VC48, :vF_VC128," +
            ":vF_VC250, :vF_VC1024, :vF_VC4000, :vF_T, :vF_M, :vF_F, :vF_C)";

      NamedParameterPreparedStatement n = NamedParameterPreparedStatement.create(db.getConnection(), sql);

      // different order
      n.setBigDecimal("vF_M", _M);
      n.setBigDecimal("vF_F", _F);
      n.setBigDecimal("vF_C", _P);

      n.setInt("vF_N1", _N1);
      n.setInt("vF_N9", _N9);
      n.setLong("vF_N18", _N18);
      n.setLocalDateTime("vF_D", _date);
      n.setString("vF_C1", _str1);
      n.setString("vF_VC32", _str32);
      n.setString("vF_VC48", _str48);
      n.setString("vF_VC128", _str128);
      n.setString("vF_VC250", _str250);
      n.setString("vF_VC1024", _str1024);
      n.setString("vF_VC4000", _str4000);
      n.setString("vF_T", _str1024);

      n.executeAndClose();

      Assert.assertEquals("TMP_TEST_TYPES bad rows count", 1, db.getTableCount("TMP_TEST_TYPES"));
      _log.debug("Record added");

      // Test fields
      testRow();
   }

   @Test
   public void testProc2Param() throws SQLException
   {
      DbUtilsPg db = DbUtilsPg.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testProc2Param");

      // Test
      String sql = "{ ? = call SelectTestTypes2(?) }";
      CallableStatement p = db.getConnection().prepareCall(sql);
      p.registerOutParameter(1, Types.INTEGER);
      p.setInt(2, _N9);

      p.execute();

      int result = p.getInt(1);

      Assert.assertEquals(_N9, result);

      p.close();
   }

   @Test
   public void testProc2NamedParam() throws SQLException
   {
      DbUtilsPg db = DbUtilsPg.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testProc2NamedParam");

      // Test
      String sql = "{ :vResult = call SelectTestTypes2(:vUserID) }";
      NamedParameterCallableStatement p = NamedParameterCallableStatement.create(db.getConnection(), sql);
      p.registerOutParameter("vResult", Types.INTEGER);
      p.setInt("vUserID", _N9);

      p.execute();

      int result = p.getInt("vResult");

      Assert.assertEquals(_N9, result);

      p.close();
   }

   @Test
   public void testProc3Param() throws SQLException
   {
      DbUtilsPg db = DbUtilsPg.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testProc3Param");

      // Test
      String sql = "{ call SelectTestTypes3(?, ?) }";
      CallableStatement p = db.getConnection().prepareCall(sql);
      p.setInt(1, _N9);
      p.setString(2, _str128);
      p.registerOutParameter(2, Types.VARCHAR);

      p.execute();

      String result = p.getString(2);

      String r = _str128 + " " + (new Integer(_N9)).toString();
      Assert.assertEquals(r, result);

      p.close();
   }

   @Test
   public void testProc3NamedParam() throws SQLException
   {
      DbUtilsPg db = DbUtilsPg.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testProc3NamedParam");

      // Test
      String sql = "{ call SelectTestTypes3(:vUserID, :vUserStr) }";
      NamedParameterCallableStatement p = NamedParameterCallableStatement.create(db.getConnection(), sql);
      p.setInt("vUserID", _N9);
      p.setString("vUserStr", _str128);
      p.registerOutParameter("vUserStr", Types.VARCHAR);

      p.execute();

      String result = p.getString("vUserStr");

      String r = _str128 + " " + (new Integer(_N9)).toString();
      Assert.assertEquals(r, result);

      p.close();
   }
}
