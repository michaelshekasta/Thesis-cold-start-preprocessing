package DAL.utils;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class DBUtils
{
    public static final int MAX_BATCH_COMMANDS = 65533;

    public static Connection getConnection(String connectionString, String dbname, String username, String password,
                                           boolean create) throws SQLException
    {
        Connection conn = null;
        DriverManager.registerDriver(new org.apache.derby.jdbc.ClientDriver());
        if (create)
        {
            conn = DriverManager.getConnection(connectionString + dbname + ";create=true", username, password);
        } else
        {
            conn = DriverManager.getConnection(connectionString + dbname, username, password);
        }
        return conn;
    }

    public static Connection getConnectionLocalDerby(String dbname, String username, String password, boolean create)
            throws SQLException
    {
        return getConnection("jdbc:derby://localhost:1527/", dbname, username, password, create);
    }

    public static Connection getConnectionLocalPostgre(String dbname, String username, String password)
            throws SQLException, ClassNotFoundException
    {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://localhost/" + dbname + "?user=" + username + "&password=" + password + "&ssl=true";
        Connection conn = DriverManager.getConnection(url);
        return conn;
    }

    public static Connection getConnectionRemotePostgre(String host, String port, String dbname, String username, String password)
            throws SQLException, ClassNotFoundException
    {
        Class.forName("org.postgresql.Driver");
//        String url = "jdbc:postgresql://"+host+":"+port+"/" + dbname + "?user=" + username + "&password=" + password + "&ssl=true";
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbname + "?user=" + username + "&password=" + password + "";
        Connection conn = DriverManager.getConnection(url);
        return conn;
    }


    public static void runDDLQuery(Connection conn, String query) throws SQLException
    {
        System.out.println("running "+query);
        PreparedStatement ps = conn.prepareStatement(query);
        ps.executeUpdate();
        ps.close();
    }

    public static void runReplaceValue(Connection conn, String table, String fieldname, String oldvalue, String newValue) throws SQLException
    {
        DBUtils.runDDLQuery(conn, "update " + table + " set " + fieldname + "=\'" + newValue + "\' where " + fieldname + "=\'" + oldvalue + "\'");
    }

    public static void insertBatch(Connection conn, PreparedStatement ps, String raw, int numFields) throws Exception
    {
        ps.clearParameters();
        String[] fields = raw.split(",");
        for (int i = 0; i < numFields; i++)
        {
            ps.setString(i + 1, fields[i]);
        }
        ps.addBatch();
    }

    public static void writeQueryToCSV(Connection conn, String sql, String path) throws SQLException, IOException
    {
        writeQueryToCSV(conn, sql, path, false, ",");
    }

    public static void writeQueryToCSV(Connection conn, String sql, String path, boolean deleteFile) throws SQLException, IOException
    {
        writeQueryToCSV(conn, sql, path, deleteFile, ",");
    }

    public static void writeQueryToCSV(Connection conn, String sql, String path, boolean deleteFile,String delimiter) throws SQLException, IOException
    {
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet executeQuery = ps.executeQuery();
        ResultSetMetaData rsmd = ps.getMetaData();
        StringBuilder t = new StringBuilder();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 0; i < columnsNumber; i++)
        {
            t.append(rsmd.getColumnLabel(i + 1));
            if (i != columnsNumber - 1)
            {
                t.append(delimiter);
            }
        }
        String title = t.toString();
        if (!deleteFile)
        {
            FileUtils.writeToFile(path, title + "\n", deleteFile);
        }
        while (executeQuery.next())
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < columnsNumber; i++)
            {
                int columnType = rsmd.getColumnType(i + 1);
                switch (columnType)
                {
                    case 4:
                        sb.append(executeQuery.getInt(i + 1));
                        break;
                    case -5:
                        sb.append(executeQuery.getBigDecimal(i + 1));
                        break;
                    case 8:
                        sb.append(executeQuery.getFloat(i + 1));
                        break;
                    case 7:
                        sb.append(executeQuery.getFloat(i + 1));
                        break;
                    case 12:
                        sb.append(executeQuery.getString(i + 1));
                        break;
                    case 91:
                        sb.append(executeQuery.getDate(i + 1));
                        break;
                    case 92:
                        sb.append(executeQuery.getTime(i + 1));
                        break;
                    case 93:
                        sb.append(executeQuery.getTime(i + 1));
                        break;
                    default:
                        System.err.println("Error type: " + columnType + " " + rsmd.getColumnTypeName(i + 1));
                }
                if (i != columnsNumber - 1)
                {
                    sb.append(delimiter);
                }
            }
            String tuple = sb.toString();
            FileUtils.writeToFile(path, tuple + "\n", true);
        }
        executeQuery.close();
        ps.close();
    }

    public static String getStringSkalar(Connection conn, String sql) throws SQLException
    {
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet executeQuery = ps.executeQuery();
        String ans = "";
        if (executeQuery.next())
        {
            ans = executeQuery.getString(1);
        }
        executeQuery.close();
        ps.close();
        return ans;
    }

    public static int getIntSkalar(Connection conn, String sql) throws SQLException
    {
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet executeQuery = ps.executeQuery();
        int ans = -1;
        if (executeQuery.next())
        {
            ans = executeQuery.getInt(1);
        }
        executeQuery.close();
        ps.close();
        return ans;
    }

    public static java.sql.Timestamp getTimestampSkalar(Connection conn, String sql) throws SQLException
    {
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet executeQuery = ps.executeQuery();
        Timestamp ans = null;
        if (executeQuery.next())
        {
            ans = executeQuery.getTimestamp(1);
        }
        executeQuery.close();
        ps.close();
        return ans;
    }

    public static void copyCSVToPostgre(Connection conn, String table, String path) throws SQLException, IOException
    {
        copyCSVToPostgre(conn, table, path, ',');
    }


    public static void copyCSVToPostgre(Connection conn, String table, String path, char delimiter) throws SQLException, IOException
    {

        CopyManager copyManager = new CopyManager((BaseConnection) conn);
        FileReader fileReader = new FileReader(path);
        copyManager.copyIn("COPY " + table + " FROM STDIN  DELIMITER '" + delimiter + "' CSV", fileReader);

    }

    public static double getDoubleSkalar(Connection conn, String query) throws SQLException
    {
        PreparedStatement ps = conn.prepareStatement(query);
        ResultSet executeQuery = ps.executeQuery();
        double ans = -1;
        if (executeQuery.next())
        {
            ans = executeQuery.getDouble(1);
        }
        executeQuery.close();
        ps.close();
        return ans;
    }

    public static List<String> getAllUsers(Connection conn) throws SQLException
    {
        List<String> ans = new ArrayList<>();
        PreparedStatement ps = conn.prepareStatement("select distinct userid from ((select userid from click_table) UNION (select userid from buy_table)) as t");
        ResultSet executeQuery = ps.executeQuery();
        while (executeQuery.next())
        {
            ans.add(executeQuery.getString(1));
        }
        executeQuery.close();
        ps.close();
        return ans;

    }

    public static void insertToTempTableClick(Connection conn, String user, String currentUser) throws SQLException
    {

        DBUtils.runDDLQuery(conn, "SELECT '" + currentUser + "',c.timestamp,c.itemId INTO tempclick_table FROM click_table as c where c.userid=" + user);

    }

    public static void insertToTempTableBuy(Connection conn, String user, String currentUser) throws SQLException
    {

        DBUtils.runDDLQuery(conn, "SELECT '" + currentUser + "',b.timestamp,b.itemId,b.quantity,b.price INTO tempbuy_table as t FROM buy_table as b where b.userid=" + user);
    }

    public static void renameTable(Connection conn, String oldTableName, String newTableName) throws SQLException
    {
        DBUtils.runDDLQuery(conn, "ALTER TABLE " + oldTableName + " RENAME TO " + newTableName);

    }

    public static void dropTable(Connection conn, String tableName) throws SQLException
    {
        DBUtils.runDDLQuery(conn, "drop TABLE " + tableName);
    }
}