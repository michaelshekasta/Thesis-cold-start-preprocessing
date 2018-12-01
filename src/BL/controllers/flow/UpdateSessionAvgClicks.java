package BL.controllers.flow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import DAL.utils.DBUtils;

public class UpdateSessionAvgClicks
{
    public static void run(Connection conn)
    {
        // TODO Auto-generated method stub
        try
        {
            PrintWriter out = new PrintWriter("more_sessions.txt");
            String query = "select sessionid from session_table order by sessionid";
            try
            {
                out.write("sessionid,countClick,morning,afternoon,evening,night\n");
                PreparedStatement ps = conn.prepareStatement(query);
                ResultSet executeQuery = ps.executeQuery();
                while (executeQuery.next())
                {
                    int sessionid = executeQuery.getInt(1);
                    double countClick = avgClicks(conn, sessionid) / 1000;
                    int morning = getSessionsInTime(conn, sessionid, 5, 12);
                    int afternoon = getSessionsInTime(conn, sessionid, 5, 12);
                    int evening = getSessionsInTime(conn, sessionid, 5, 12);
                    int night = getSessionsInTime(conn, sessionid, 5, 12);
                    out.write(String.format("%d,%f,%d,%d,%d,%d\n", sessionid, countClick, morning, afternoon, evening, night));
                    //	private static final String SQL_CREATE_SESSION_TABLE = "CREATE TABLE session_table ( dayofsession date,userid varchar(255), sessionid int,clicks int, buy int)";
                }
                ps.close();
            } catch (SQLException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            out.close();
        } catch (FileNotFoundException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }

    private static int getSessionsInTime(Connection conn, int sessionid, int fromtime, int totime)
    {
        int count = -1;
        String query = "select count(*) from click_table where sessionid =" + sessionid + " and extract(min from timestamp) >= " + fromtime + " and extract(min from timestamp) <" + totime;
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

    private static long avgClicks(Connection conn, int sessionid)
    {
        long count = -1l;
        String query1 = "select count(*) from click_table where sessionid=" + sessionid;
        try
        {
            count = DBUtils.getIntSkalar(conn, query1);
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (count < 2)
        {
            return -1;
        }
        String query = "select (max(timestamp)-min(timestamp))/(count(*)-1) from click_table where sessionid=" + sessionid;
        //		System.out.println(query);
        try
        {
            Timestamp timestampSkalar = DBUtils.getTimestampSkalar(conn, query);
            count = timestampSkalar.getTime();
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return count;
    }
}
