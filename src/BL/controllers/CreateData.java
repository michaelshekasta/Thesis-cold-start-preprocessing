package BL.controllers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;

import DAL.utils.DBUtils;
import DAL.utils.FileUtils;

public class CreateData
{
    private static void createBuyFile(String date) throws Exception, SQLException
    {
        Connection derby = getConnection();
        String sql1 = "select date(b.timestamp) as date,b.userid,b.itemid,b.timestamp,t.newuserid" + " from buy_table as b" + " left outer join transfer_table as t on (b.userid=t.userid and b.timestamp > t.timestamp) where date(b.timestamp) < date('"
                + date + "')" + " order by date(b.timestamp),b.userid,b.itemid";
        PreparedStatement ps = derby.prepareStatement(sql1);
        ResultSet resultSet = ps.executeQuery();
        FileUtils.writeToFile("buys.csv", "date,userid,itemid,timestamp\n", false);
        while (resultSet.next())
        {
            LocalDate sessoinDate = resultSet.getDate(1).toLocalDate();
            String userid = resultSet.getString(2);
            String itemid = resultSet.getString(3);
            Timestamp timestamp = resultSet.getTimestamp(4);
            String newUserid = resultSet.getString(5);
            if (newUserid != null)
            {
                userid = newUserid;
            }
            FileUtils.writeToFile("buys.csv", sessoinDate + "," + userid + "," + itemid + "," + timestamp + "\n", true);
        }
        //DBUtils.writeQueryToCSV(derby, sql1, "buys.csv");
        closeConnection(derby);
    }

    private static void closeConnection(Connection derby) throws SQLException
    {
        derby.close();
    }

    private static Connection getConnection() throws Exception
    {
        Connection derby = DBUtils.getConnectionLocalDerby("yoochose", "user2", "user2", false);
        return derby;
    }

    private static void createClickFile(String date) throws Exception, SQLException
    {
        Connection derby = getConnection();
        String sql2 = "select date(c.timestamp) as date,c.userid,c.itemid,c.timestamp,t.newuserid" + " from click_table as c" + " left outer join transfer_table as t on (c.userid=t.userid and c.timestamp > t.timestamp)" + " where date(c.timestamp) < date('"
                + date
                + "')" + " order by date(c.timestamp),c.userid,c.itemid";
        PreparedStatement ps = derby.prepareStatement(sql2);
        ResultSet resultSet = ps.executeQuery();
        FileUtils.writeToFile("clicks.csv", "date,userid,itemid,timestamp\n", false);
        while (resultSet.next())
        {
            LocalDate sessoinDate = resultSet.getDate(1).toLocalDate();
            String userid = resultSet.getString(2);
            String itemid = resultSet.getString(3);
            Timestamp timestamp = resultSet.getTimestamp(4);
            String newUserid = resultSet.getString(5);
            if (newUserid != null)
            {
                userid = newUserid;
            }
            FileUtils.writeToFile("clicks.csv", sessoinDate + "," + userid + "," + itemid + "," + timestamp + "\n", true);
        }
        closeConnection(derby);
    }

    private static void getStatOfBuy(String date) throws Exception, SQLException
    {
        Connection derby = getConnection();
        getTotalSessionStat(date, derby);
        getSessionBuy(date, derby);
        getBadSessionsStat(date, derby);
        closeConnection(derby);

    }

    private static void getSessionBuy(String date, Connection derby) throws Exception
    {
        String sql4 = "select count(*) " + "from buys c " + "where date(timestamp) < date('" + date + "')" + "";
        System.out.print(" buy sessions:");
        String ans2 = DBUtils.getStringSkalar(derby, sql4);
        System.out.println(ans2);
    }

    private static void getBadSessionsStat(String date, Connection derby) throws Exception
    {
        String sql3 = "select count(*) " + "from (" + "select distinct b.userid,date(b.timestamp) "
                + "from buys b join click_table c on (b.userid=c.userid and date(b.timestamp) = date(c.timestamp)) "
                + "where b.timestamp < c.timestamp and date(b.timestamp) < date('" + date + "')" + ") t";
        System.out.print("bad sessions:");
        String ans = DBUtils.getStringSkalar(derby, sql3);
        System.out.println(ans);
    }

    private static void getTotalSessionStat(String date, Connection derby) throws Exception
    {
        String sql2 = "select count(*) " + "from buys b " + "where date(timestamp) < date('" + date + "')";
        System.out.print("total session:");
        String ans1 = DBUtils.getStringSkalar(derby, sql2);
        System.out.println(ans1);
    }

    private static void createSessionFile(String date) throws Exception, SQLException
    {
        Connection derby = getConnection();
        String sql2 = "select date(c.timestamp) as date,c.userid,t.newuserid" + " from click_table as c" + " left outer join transfer_table as t on (c.userid=t.userid and c.timestamp > t.timestamp)" + " where date(c.timestamp) < date('"
                + date
                + "')" + " order by date(c.timestamp),c.userid,c.itemid";
        PreparedStatement ps = derby.prepareStatement(sql2);
        ResultSet resultSet = ps.executeQuery();
        FileUtils.writeToFile("sessions.csv", "date,userid\n", false);
        while (resultSet.next())
        {
            LocalDate sessoinDate = resultSet.getDate(1).toLocalDate();
            String userid = resultSet.getString(2);
            String newUserid = resultSet.getString(3);
            if (newUserid != null)
            {
                userid = newUserid;
            }
            FileUtils.writeToFile("sessions.csv", sessoinDate + "," + userid + "\n", true);
        }
        System.out.println("done");
        closeConnection(derby);
    }

    public static void createFiles(String date) throws Exception
    {
        createBuyFile(date);
        System.out.println("done create buy file");
        createClickFile(date);
        System.out.println("done create click file");
        createSessionFile(date);
        System.out.println("done create sessoin file");
//		getStatOfBuy(date);
    }

}
