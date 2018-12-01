package BL.controllers.flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import DAL.utils.DBUtils;

public class UpdateSessionState
{

    public static void run(Connection conn)
    {
        // TODO Auto-generated method stub
        String query = "select sessionid from session_table order by sessionid";
        try
        {
            PreparedStatement ps = conn.prepareStatement(query);
            ResultSet executeQuery = ps.executeQuery();
            while (executeQuery.next())
            {
                int sessionid = executeQuery.getInt(1);
                int countClick = getCount(conn, sessionid, "click_table");
                int countBuy = getCount(conn, sessionid, "buy_table");
                //	private static final String SQL_CREATE_SESSION_TABLE = "CREATE TABLE session_table ( dayofsession date,userid varchar(255), sessionid int,clicks int, buy int)";
                String queryUp = "update session_table set clicks=" + countClick + " , buy=" + countBuy + " where sessionid=" + sessionid;
                DBUtils.runDDLQuery(conn, queryUp);
            }
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static int getCount(Connection conn, int sessionid, String table)
    {
        int count = -1;
        String query = "select count(*) from " + table + " where sessionid=" + sessionid;
//		System.out.println(query);
        try
        {
            count = DBUtils.getIntSkalar(conn, query);
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return count;
    }

}
