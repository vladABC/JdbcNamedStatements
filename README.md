# JdbcNamedParameters

Provides a NamedParameterPreparedStatement and NamedParameterCallableStatement classes which allows the use of named
parameters in JDBC prepared SQL statements.

Credit to Axiom Data Science for his implementation of the SQL query parser:
https://github.com/axiom-data-science/jdbc-named-parameters.git

## Lib install

Install oracle driver into maven repository

rem mvn install:install-file -Dfile=ojdbc6.jar -DgroupId=com.oracle  -DartifactId=ojdbc6 -Dversion=11.2.0.4 -Dpackaging=jar -DgeneratePom=true

for run examples, install JdbcNamedParameters into maven repository

mvn install:install-file -Dfile=JdbcNamedStatements-1.0.0.jar -DgroupId=abc.java.sql -DartifactId=JdbcNamedStatements -Dversion=1.0.0 -Dpackaging=jar -DgeneratePom=true

## Postgres examples

```
String sql = "{ :vResult = call SelectTestTypes2(:vUserID) }";
NamedParameterCallableStatement p = NamedParameterCallableStatement.create(db.getConnection(), sql);
p.registerOutParameter("vResult", Types.INTEGER);
p.setInt("vUserID", _N9);

```

## Oracle examples

```
String sql = "{ :vResult = call SelectTestTypes2(:vUserID) }";
NamedParameterCallableStatement p = NamedParameterCallableStatement.create(db.getConnection(), sql);
p.registerOutParameter("vResult", OracleTypes.NUMBER);
p.setInt("vUserID", _N9);
```

See tests for more details
