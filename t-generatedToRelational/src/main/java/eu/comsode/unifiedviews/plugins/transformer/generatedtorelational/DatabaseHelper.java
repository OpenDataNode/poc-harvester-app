/**
 * Copyright 2015 Peter Goliuan.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package eu.comsode.unifiedviews.plugins.transformer.generatedtorelational;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.relational.WritableRelationalDataUnit;

public class DatabaseHelper {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseHelper.class);

    public static boolean checkTableExists(Connection conn, String targetTableName) throws SQLException {
        boolean bTableExists = false;
        DatabaseMetaData dbm = null;
        ResultSet tables = null;
        try {
            dbm = conn.getMetaData();
            tables = dbm.getTables(null, null, targetTableName, null);
            if (tables.next()) {
                bTableExists = true;
            }
        } finally {
            tryCloseResultSet(tables);
        }
        return bTableExists;
    }

    public static String convertSelectQueryToSelectIntoQuery(String query, String targetTable) {
        StringBuilder createQuery = new StringBuilder();
        createQuery.append("CREATE TABLE ");
        createQuery.append(targetTable);
        createQuery.append(" AS ");
        createQuery.append(query);

        return createQuery.toString().toUpperCase();
    }

    public static String createAlterColumnSetNotNullQuery(String tableName, String keyColumn) {
        StringBuilder query = new StringBuilder();
        query.append("ALTER TABLE ");
        query.append(tableName);
        query.append(" ");
        query.append("ALTER COLUMN ");
        query.append(keyColumn);
        query.append(" ");
        query.append("SET NOT NULL");

        return query.toString();
    }

    /**
     * Generates INSERT query for prepared statement (values as ?) with given column names and for given table
     *
     * @param columns
     *            Columns to insert into
     * @param tableName
     *            Table to insert into
     * @return INSERT query for prepared statement
     * @throws SQLException
     */
    public static String getInsertQueryForPreparedStatement(List<ColumnDefinition> columns, String tableName) throws SQLException {
        StringBuilder query = new StringBuilder("INSERT INTO ");
        query.append(tableName);
        query.append(" (");
        for (ColumnDefinition column : columns) {
            query.append(column.getColumnName());
            query.append(", ");
        }
        query.setLength(query.length() - 2);
        query.append(") VALUES (");

        for (int i = 1; i <= columns.size(); i++) {
            query.append("?");
            query.append(", ");
        }
        query.setLength(query.length() - 2);
        query.append(")");

        return query.toString();
    }

    /**
     * Generates CREATE table query with given column definitions
     *
     * @param columns
     *            List of column definitions
     * @param tableName
     *            Name of created table
     * @return CREATE TABLE query
     * @throws SQLException
     */
    public static String getCreateTableQueryFromMetaData(List<ColumnDefinition> columns, String tableName) throws SQLException {
        StringBuilder query = new StringBuilder();
        query.append("CREATE TABLE ");
        query.append(tableName);
        query.append(" (");
        for (ColumnDefinition column : columns) {
            query.append("\"");
            query.append(column.getColumnName());
            query.append("\"");
            query.append(" ");
            query.append(column.getColumnTypeName());
            if (column.getColumnSize() != -1) {
                query.append("(");
                query.append(column.getColumnSize());
                query.append(")");
            }
            if (column.isNotNull()) {
                query.append(" ");
                query.append("NOT NULL");
            }
            query.append(", ");
        }

        query.setLength(query.length() - 2);
        query.append(")");

        return query.toString();
    }

    /**
     * Get list of table columns from {@link ResultSetMetaData} result set meta data
     *
     * @param meta
     *            ResultSet meta data
     * @return List of columns
     * @throws SQLException
     */
    public static List<ColumnDefinition> getTableColumnsFromMetaData(ResultSetMetaData meta) throws SQLException, SQLTransformException {
        int columnsCount = meta.getColumnCount();
        List<ColumnDefinition> columns = new ArrayList<>();
        Set<String> uniqueColumns = new HashSet<String>();
        // If result set contains multiple columns with the same name, add index
        for (int i = 1; i <= columnsCount; i++) {
            int type = meta.getColumnType(i);
            String columnLabel = meta.getColumnLabel(i);
            String typeName = meta.getColumnTypeName(i);
            String typeClass = meta.getColumnClassName(i);
            LOG.debug("Column name: {}, type name: {}, SQL type: {}", columnLabel, typeName, type);
            boolean columnNotNull = (meta.isNullable(i) == ResultSetMetaData.columnNoNulls);
            if (uniqueColumns.contains(columnLabel)) {
                LOG.error("Multiple occurences of column with the same name: {}, rename column in select via AS keyword!", columnLabel);
                throw new SQLTransformException("Multiple column name occurrences: '" + columnLabel + "'", SQLTransformException.TransformErrorCode.DUPLICATE_COLUMN_NAME);
            }
            uniqueColumns.add(columnLabel);
            ColumnDefinition column = new ColumnDefinition(columnLabel, typeName, type, columnNotNull, typeClass);
            columns.add(column);
        }

        return columns;
    }

    public static void tryCloseStatement(Statement stmnt) {
        try {
            if (stmnt != null) {
                stmnt.close();
            }
        } catch (SQLException e) {
            LOG.warn("Error occurred during closing statement", e);
        }
    }

    public static void tryCloseResultSet(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            LOG.warn("Error occurred during closing result set", e);
        }
    }

    public static void tryCloseConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.warn("Error occurred during closing result set", e);
            }
        }
    }

    public static void tryRollbackConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                LOG.warn("Error occurred during rollback of connection", e);
            }
        }
    }

    public static String createPrimaryKeysQuery(String tableName, List<String> primaryKeys) {
        StringBuilder query = new StringBuilder("ALTER TABLE ");
        query.append(tableName);
        query.append(" ADD PRIMARY KEY (");
        for (String key : primaryKeys) {
            query.append(key);
            query.append(",");
        }
        query.setLength(query.length() - 1);
        query.append(")");

        return query.toString();
    }

    public static void tryCloseDbResources(Connection conn, Statement stmnt, ResultSet rs) {
        tryCloseResultSet(rs);
        tryCloseStatement(stmnt);
        tryCloseConnection(conn);
    }

    public static String getCreateIndexQuery(String tableName, String columnName) {
        StringBuilder query = new StringBuilder("CREATE INDEX ");
        query.append(columnName + "_idx ");
        query.append("ON ");
        query.append(tableName);
        query.append("(");
        query.append(columnName);
        query.append(")");

        return query.toString();
    }

    public static String getListAsCommaSeparatedString(List<String> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (String key : list) {
            sb.append(key);
            sb.append(",");
        }
        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    public static String createDatabaseUserQuery(String userName, String password) {
        StringBuilder query = new StringBuilder("CREATE USER ");
        query.append(userName);
        query.append(" PASSWORD '");
        query.append(password);
        query.append("'");

        return query.toString();
    }

    public static String createGrantSelectOnTableQuery(String tableName, String userName) {
        StringBuilder query = new StringBuilder("GRANT SELECT ON ");
        query.append(tableName);
        query.append(" TO ");
        query.append(userName);

        return query.toString();
    }

    public static String createDropUserQuery(String dbUserName) {
        StringBuilder query = new StringBuilder("DROP USER ");
        query.append(dbUserName);
        query.append(" IF EXISTS");

        return query.toString();
    }

    public static void executeUpdate(String query, WritableRelationalDataUnit relationalDataUnit) throws DataUnitException {
        Statement stmnt = null;
        Connection conn = null;
        try {
            conn = relationalDataUnit.getDatabaseConnection();
            stmnt = conn.createStatement();
            stmnt.executeUpdate(query);
            conn.commit();
        } catch (SQLException e) {
            LOG.error("Error at executing query to relational dataunit: ", e);
            DatabaseHelper.tryRollbackConnection(conn);
            throw new DataUnitException("Error at executing query: ", e);
        } finally {
            DatabaseHelper.tryCloseStatement(stmnt);
            DatabaseHelper.tryCloseConnection(conn);
        }
    }

}
