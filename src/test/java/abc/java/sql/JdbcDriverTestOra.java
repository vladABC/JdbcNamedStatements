package abc.java.sql;

import oracle.jdbc.OracleTypes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class JdbcDriverTestOra
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

   public JdbcDriverTestOra()
   {
      DbUtilsOra db = DbUtilsOra.getInstance();

      _N1 = db.getMaxN1();
      _N9 = db.getMaxN9();
      _N18 = db.getMaxN18();
      _M = db.getMaxMoney();
      _F = db.getMaxFactor();
      _P = db.getMaxPercent();
      _date = LocalDateTime.now();

      _str1 = "V";
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
      DbUtilsOra db = DbUtilsOra.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("In setup");
      _log.debug("Connected");

      db.dropTable("TMP_TEST_TYPES");
      _log.debug("Table droped");

      try
      {
         String sql;
         sql = "create table TMP_TEST_TYPES (" +
               "F_N1           NUMBER(1), " +
               "F_N9           NUMBER(9), " +
               "F_N18          NUMBER(18), " +
               "F_D            DATE, " +
               "F_C1           CHAR(1), " +
               "F_VC32         VARCHAR2(32), " +
               "F_VC48         VARCHAR2(48), " +
               "F_VC128        VARCHAR2(128), " +
               "F_VC250        VARCHAR2(250), " +
               "F_VC1024       VARCHAR2(1024), " +
               "F_VC4000       VARCHAR2(4000), " +
               "F_M            NUMBER(20,6), " +
               "F_F            NUMBER(20,10), " +
               "F_C            NUMBER(18,15))";

         Statement s = conn.createStatement();
         s.execute(sql);
         s.close();

         _log.debug("TMP_TEST_TYPES table created");

         sql = "create or replace procedure LoadRecordTestTypes " +
               "  (vF_N1         in number," +
               "   vF_N9         in number," +
               "   vF_N18        in number," +
               "   vF_D          in date," +
               "   vF_C1         in char," +
               "   vF_VC32       in varchar2," +
               "   vF_VC48       in varchar2," +
               "   vF_VC128      in varchar2," +
               "   vF_VC250      in varchar2," +
               "   vF_VC1024     in varchar2," +
               "   vF_VC4000     in varchar2," +
               "   vF_M          in number," +
               "   vF_F          in number," +
               "   vF_C          in number) " +
               "is " +
               "begin " +
               "   insert into TMP_TEST_TYPES " +
               "          (F_N1, F_N9, F_N18, F_M, F_F, F_C, F_D, F_C1, F_VC32, F_VC48, F_VC128, F_VC250, F_VC1024, F_VC4000)  " +
               "   values (vF_N1, vF_N9, vF_N18, vF_M, vF_F, vF_C, vF_D, vF_C1, vF_VC32, vF_VC48, vF_VC128, vF_VC250, vF_VC1024, vF_VC4000); " +
               "end;";

         s = conn.createStatement();
         s.execute(sql);
         s.close();

         _log.debug("LoadRecordTestTypes procedure created");

         sql = "create or replace procedure SelectTestTypes " +
               "  (vUserID         in  number, " +
               "   result_cursor   out sys_refcursor) " +
               "is " +
               "begin " +
               "   open result_cursor for " +
               "   select * from TMP_TEST_TYPES; " +
               "end;";

         s = conn.createStatement();
         s.execute(sql);
         s.close();

         _log.debug("LoadRecordTestTypes function created");

         sql = "create or replace function SelectTestTypes2 " +
               "  (vUserID         in  integer) " +
               "return integer " +
               "is " +
               "begin " +
               "   return vUserID; " +
               "end;";

         s = conn.createStatement();
         s.execute(sql);
         s.close();

         _log.debug("SelectTestTypes2 function created");

         sql = "create or replace procedure SelectTestTypes3 " +
               "  (vUserID         in      integer,  " +
               "   vUserStr        in out  varchar2) " +
               "is " +
               "begin " +
               "   vUserStr := vUserStr || ' ' || TO_CHAR(vUserID); " +
               "end;";

         s = conn.createStatement();
         s.execute(sql);
         s.close();

         _log.debug("SelectTestTypes3 procedure created");
      }
      catch (SQLException e)
      {
         _log.error(e.getMessage());
      }
   }

   @AfterClass
   public static void tearDown()
   {
      DbUtilsOra db = DbUtilsOra.getInstance();
      if (db.getConnection() == null) return;

      //dropTable("TMP_TEST_TYPES");
      _log.debug("TMP_TEST_TYPES table droped");

      db.dropProcedure("SelectTestTypes");
      _log.debug("SelectTestTypes procedure droped");

      db.dropProcedure("LoadRecordTestTypes");
      _log.debug("LoadRecordTestTypes procedure droped");

      db.dropFunction("SelectTestTypes2");
      _log.debug("SelectTestTypes2 function droped");
   }

   public void testRow() throws SQLException
   {
      DbUtilsOra db = DbUtilsOra.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      CallableStatement p = conn.prepareCall("{ call SelectTestTypes(?, ?) }");
      p.setInt(1, 1);
      p.registerOutParameter(2, OracleTypes.CURSOR);
      p.execute();

      ResultSet r = (ResultSet) p.getObject(2);
      while (r.next())
      {
         Assert.assertEquals(_N1, r.getInt(1));
         Assert.assertEquals(_N1, r.getInt("F_N1"));

         Assert.assertEquals(_N9, r.getInt(2));
         Assert.assertEquals(_N9, r.getInt("F_N9"));

         Assert.assertEquals((long) _N18, r.getLong(3));
         Assert.assertEquals((long) _N18, r.getLong("F_N18"));

         // without nanoseconds
         Assert.assertEquals(_date.truncatedTo(ChronoUnit.SECONDS), r.getTimestamp(4).toLocalDateTime());
         Assert.assertEquals(_date.truncatedTo(ChronoUnit.SECONDS), r.getTimestamp("F_D").toLocalDateTime());

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

         Assert.assertEquals(_M, r.getBigDecimal(12));
         Assert.assertEquals(_M, r.getBigDecimal("F_M"));

         Assert.assertEquals(_F, r.getBigDecimal(13));
         Assert.assertEquals(_F, r.getBigDecimal("F_F"));

         Assert.assertEquals(_P, r.getBigDecimal(14));
         Assert.assertEquals(_P, r.getBigDecimal("F_C"));
      }
      r.close();
      p.close();
   }

   @Test
   public void testInsert() throws SQLException
   {
      DbUtilsOra db = DbUtilsOra.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testInsert");

      // Prepare
      db.deleteFromTable("TMP_TEST_TYPES");

      // Test
      String sql = "insert into TMP_TEST_TYPES(F_N1, F_N9, F_N18, F_D, " +
            "F_C1, F_VC32, F_VC48, F_VC128, F_VC250, F_VC1024, F_VC4000, F_M, F_F, F_C) " +
            "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + "?, ?, ?, ?)";

      PreparedStatement s = db.getConnection().prepareStatement(sql);
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
      s.setBigDecimal(12, _M);              // F_M
      s.setBigDecimal(13, _F);              // F_F
      s.setBigDecimal(14, _P);              // F_C

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
      DbUtilsOra db = DbUtilsOra.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testInsertNamedParam");

      // Prepare
      db.deleteFromTable("TMP_TEST_TYPES");

      // Test
      String sql = "insert into TMP_TEST_TYPES " +
            "              ( F_N1,  F_N9,  F_N18,  F_D,  F_C1,  F_VC32,  F_VC48,  F_VC128,  F_VC250,  F_VC1024, " +
            "                F_VC4000,  F_M,  F_F,  F_C) " +
            "       values (:F_N1, :F_N9, :F_N18, :F_D, :F_C1, :F_VC32, :F_VC48, :F_VC128, :F_VC250, :F_VC1024, " +
            "               :F_VC4000, :F_M, :F_F, :F_C)";

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
      DbUtilsOra db = DbUtilsOra.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testProcParam");

      // Prepare
      db.deleteFromTable("TMP_TEST_TYPES");

      // Test
      String sql = "{ call LoadRecordTestTypes(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?) }";
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
      p.setBigDecimal(12, _M);
      p.setBigDecimal(13, _F);
      p.setBigDecimal(14, _P);

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
      DbUtilsOra db = DbUtilsOra.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testProcNamedParam");

      // Prepare
      db.deleteFromTable("TMP_TEST_TYPES");

      // Test
      String sql = "{ call LoadRecordTestTypes(" +
            ":vF_N1, :vF_N9, :vF_N18, :vF_D, :vF_C1, :vF_VC32, :vF_VC48, :vF_VC128," +
            ":vF_VC250, :vF_VC1024, :vF_VC4000, :vF_M, :vF_F, :vF_C) }";

      NamedParameterPreparedStatement n = NamedParameterPreparedStatement.create(db.getConnection(), sql);
      // test different order
      n.setBigDecimal("vF_M", _M);
      n.setBigDecimal("vF_F", _F);
      n.setBigDecimal("vF_C", _P);
      //
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

      n.executeAndClose();

      Assert.assertEquals("TMP_TEST_TYPES bad rows count", 1, db.getTableCount("TMP_TEST_TYPES"));
      _log.debug("Record added");

      // Test fields
      testRow();
   }

   @Test
   public void testProc2Param() throws SQLException
   {
      DbUtilsOra db = DbUtilsOra.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testProc2Param");

      // Test
      String sql = "{ ? = call SelectTestTypes2(?) }";
      CallableStatement p = db.getConnection().prepareCall(sql);
      p.registerOutParameter(1, OracleTypes.NUMBER);
      p.setInt(2, _N9);

      p.execute();

      int result = p.getInt(1);

      Assert.assertEquals(_N9, result);

      p.close();
   }

   @Test
   public void testProc2NamedParam() throws SQLException
   {
      DbUtilsOra db = DbUtilsOra.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testProc2NamedParam");

      // Test
      String sql = "{ :vResult = call SelectTestTypes2(:vUserID) }";
      NamedParameterCallableStatement p = NamedParameterCallableStatement.create(db.getConnection(), sql);
      p.registerOutParameter("vResult", OracleTypes.NUMBER);
      p.setInt("vUserID", _N9);

      p.execute();

      int result = p.getInt("vResult");

      Assert.assertEquals(_N9, result);

      p.close();
   }

   @Test
   public void testProc3Param() throws SQLException
   {
      DbUtilsOra db = DbUtilsOra.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testProc3Param");

      // Test
      String sql = "{ call SelectTestTypes3(?, ?) }";
      CallableStatement p = db.getConnection().prepareCall(sql);
      p.setInt(1, _N9);
      p.setString(2, _str128);
      p.registerOutParameter(2, OracleTypes.VARCHAR);

      p.execute();

      String result = p.getString(2);

      String r = _str128 + " " + (new Integer(_N9)).toString();
      Assert.assertEquals(r, result);

      p.close();
   }

   @Test
   public void testProc3NamedParam() throws SQLException
   {
      DbUtilsOra db = DbUtilsOra.getInstance();
      Connection conn = db.getConnection();
      if (conn == null) return;

      _log.debug("Begin testProc3NamedParam");

      // Test
      String sql = "{ call SelectTestTypes3(:vUserID, :vUserStr) }";
      NamedParameterCallableStatement p = NamedParameterCallableStatement.create(db.getConnection(), sql);
      p.setInt("vUserID", _N9);
      p.setString("vUserStr", _str128);
      p.registerOutParameter("vUserStr", OracleTypes.VARCHAR);

      p.execute();

      String result = p.getString("vUserStr");

      String r = _str128 + " " + (new Integer(_N9)).toString();
      Assert.assertEquals(r, result);

      p.close();
   }
}
