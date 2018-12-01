package BL.controllers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import DAL.utils.DBUtils;
import DAL.utils.FileUtils;

public class CreateDataWithSessionTable
{
    private static HashMap<String, String> usersTable;

    private static void createBuyFile(Connection conn, String date) throws Exception, SQLException
    {
        String sql1 = "select date,userid,itemid,timestamp"
                + " from ("
                + "select date(b.timestamp) as date,b.userid,b.itemid,b.timestamp"
                + " from buy_table as b"
                + " where date(b.timestamp) < date('"
                + date
                + "')) t"
                + " order by date,userid,itemid";
        PreparedStatement ps = conn.prepareStatement(sql1);
        ResultSet resultSet = ps.executeQuery();
        FileUtils.writeToFile("buys.csv", "date,userid,itemid,timestamp\n", false);
        while (resultSet.next())
        {
            LocalDate sessoinDate = resultSet.getDate(1).toLocalDate();
            String userid = getUserFromTable(resultSet.getString(2));
            String itemid = resultSet.getString(3);
            Timestamp timestamp = resultSet.getTimestamp(4);
            FileUtils.writeToFile("buys.csv", sessoinDate + "," + userid + "," + itemid + "," + timestamp + "\n", true);
        }
    }

    private static void closeConnection(Connection derby) throws SQLException
    {
        derby.close();
    }

    private static Connection getConnection() throws Exception
    {
//		Connection derby = DBUtils.getConnectionLocalDerby("yoochose", "user2", "user2", false);
        Connection conn = DBUtils.getConnectionLocalPostgre("yoochose", "yoochose", "1234");
        return conn;
    }

    private static void createClickFile(Connection conn, String date) throws Exception, SQLException
    {
        String sql2 = "select date,userid,itemid,timestamp"
                + " from (select date(timestamp) as date,userid,itemid,timestamp"
                + " from click_table"
                + " where date(timestamp) < date('"
                + date
                + "')) t"
                + " order by date,userid,itemid,timestamp";
        PreparedStatement ps = conn.prepareStatement(sql2);
        ResultSet resultSet = ps.executeQuery();
        FileUtils.writeToFile("clicks.csv", "date,userid,itemid,timestamp\n", false);
        while (resultSet.next())
        {
            LocalDate sessoinDate = resultSet.getDate(1).toLocalDate();
            String userid = getUserFromTable(resultSet.getString(2));
            String itemid = resultSet.getString(3);
            Timestamp timestamp = resultSet.getTimestamp(4);
            FileUtils.writeToFile("clicks.csv", sessoinDate + "," + userid + "," + itemid + "," + timestamp + "\n", true);
        }
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

    private static void createSessionFile(Connection conn, String date) throws Exception, SQLException
    {
        int sessionid = 1;
        String sql2 =
                "select distinct date,userid"
                        + " from ("
                        + "select date(timestamp) as date,userid"
                        + " from click_table"
                        + " where date(timestamp) < date('"
                        + date
                        + "')"
                        + ") as t"
                        + " order by date,userid";

        PreparedStatement ps = conn.prepareStatement(sql2);
        ResultSet resultSet = ps.executeQuery();
        FileUtils.writeToFile("sessions.csv", "date,userid\n", false);
        while (resultSet.next())
        {
            LocalDate sessoinDate = resultSet.getDate(1).toLocalDate();
            String userid = resultSet.getString(2);
            FileUtils.writeToFile("sessions.csv", sessionid + "," + sessoinDate + "," + userid + "\n", true);
            sessionid++;
        }
        System.out.println("done");
    }


    public static void createFiles(Connection conn) throws Exception
    {
        createFiles(conn, "12/12/2020");
    }

    public static void createFiles(Connection conn, String date) throws Exception
    {
        loadTable("fullTableupdate.txt");
        createBuyFile(conn, date);
        System.out.println("done create buy file");
        createClickFile(conn, date);
        System.out.println("done create click file");
        createSessionFile(conn, date);
        System.out.println("done create sessoin file");
//		getStatOfBuy(date);
    }

    private static void loadTable(String string) throws IOException
    {
        usersTable = new HashMap<String, String>();
        FileUtils.writeToFile("fullTableupdate.txt", "", false);
        File f = new File("old_fulltable.txt");
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        while (line != null)
        {
            String[] fields = line.split(" ");
            ArrayList<String> users = new ArrayList<String>();
            for (String field : fields)
            {
                if (field.length() != 0)
                {
                    users.add(field);
                }
            }
            ArrayList<String> sortArrayList = sortArrayList(users);
            String userid = sortArrayList.get(0);
            for (String user : sortArrayList)
            {
                usersTable.put(user, userid);
                FileUtils.writeToFile("fullTableupdate.txt", user + " ", true);
            }
            FileUtils.writeToFile("fullTableupdate.txt", "\n", true);
            line = br.readLine();
        }
        br.close();
        fr.close();
    }

    private static ArrayList<String> sortArrayList(ArrayList<String> value)
    {
        Collections.sort(value, new Comparator<String>()
        {
            @Override
            public int compare(String o1, String o2)
            {
                if (o1.indexOf('-') == -1 && o2.indexOf('-') != -1)
                {
                    return -1;
                }
                if (o1.indexOf('-') != -1 && o2.indexOf('-') == -1)
                {
                    return 1;
                }
                return o1.compareTo(o2);
            }
        });
        return value;
    }

    private static String getUserFromTable(String user)
    {
        return usersTable.get(user);
    }
}
