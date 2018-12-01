package BL.controllers.flow;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;

import org.apache.derby.iapi.types.SQLBoolean;

import DAL.utils.DBUtils;
import DAL.utils.FileUtils;

public class UpdateSessions
{

    public static void run(Connection conn) throws SQLException
    {
//		try
//		{
//			DBUtils.runDDLQuery(conn, "CREATE INDEX new_buy_index_session on buy_table (userid,timestamp)");
//			DBUtils.runDDLQuery(conn, "CREATE INDEX new_buy_index_buy on buy_table (userid,timestamp)");
//		}
//		catch(Exception ex)
//		{
//			ex.printStackTrace();
//		}
        int sessionid = 0;
        String sql = ""
                + "select distinct date(c.timestamp) AS dc,c.userid AS uc"
                + " from click_table AS c"
                + " order by dc,uc";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next())
        {
            sessionid++;
            Date date = resultSet.getDate(1);
            LocalDate sessoinDate = date.toLocalDate();
            String userid = resultSet.getString(2);
            String sqlBuyUpdate = getSqlUpdateCommand(sessionid, sessoinDate, userid, "buy_table");
            String sqlClickUpdate = getSqlUpdateCommand(sessionid, sessoinDate, userid, "click_table");
//			String sqlBasketUpdate = getSqlUpdateCommand(sessionid, sessoinDate, userid, "basket_table");
            DBUtils.runDDLQuery(conn, sqlBuyUpdate);
            DBUtils.runDDLQuery(conn, sqlClickUpdate);
//			DBUtils.runDDLQuery(conn, sqlBasketUpdate);
            DBUtils.runDDLQuery(conn, "insert into session_table (dayofsession,userid,sessionid) "
                    + "values (\'" + date + "\',\'" + userid + "\'," + sessionid + ")");
        }
        DBUtils.runDDLQuery(conn, "CREATE INDEX session_index_sessionid on session_table (sessionid)");
    }

    private static String getSqlUpdateCommand(int sessionid, LocalDate sessoinDate, String userid, String table)
    {
        return "update " + table + " "
                + "set sessionid=" + sessionid + " "
                + "where userid=\'" + userid + "\' and date(timestamp)=\'" + Date.valueOf(sessoinDate) + "\'";
    }

}
