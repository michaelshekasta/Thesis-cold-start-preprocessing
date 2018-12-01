package BL;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import BL.controllers.CreateData;
import BL.controllers.flow.CreateSessions;
import BL.controllers.input.IInputActions;
import BL.controllers.input.raw.InputActionFromFile;
import BL.controllers.output.IOutput;
import BL.controllers.output.OutputCSV;
import BL.controllers.output.db.actions.ITableAction;
import BL.controllers.output.db.actions.TableActionBuy;
import BL.controllers.output.db.actions.TableActionClick;
import BL.controllers.output.db.actions.TableActionRecommendedClick;
import BL.controllers.output.db.actions.TableActionTransfer;
import BL.controllers.parser.IActionParser;
import BL.controllers.parser.IParseActions;
import BL.controllers.parser.ParserBasic;
import BL.controllers.parser.action.raw.BuyActionParserFromRaw;
import BL.controllers.parser.action.raw.ClickActionParserFromRaw;
import BL.controllers.parser.action.raw.ClickRecommendedParserFromRaw;
import BL.controllers.parser.action.raw.TransferActionParser;
import BL.controllers.preprocessing.YCReaderBatch;
import DAL.utils.DBUtils;
import DAL.utils.FileUtils;

public class LoginAnalysis
{
    private static void writeFullTable(Connection conn) throws IOException
    {
        // TODO Auto-generated method stub
        String path = "old_fulltable.txt";
        FileUtils.writeToFile(path, "");
        HashMap<String, Integer> users = new HashMap<String, Integer>();
        int count = 0;
        try
        {
//			conn = DBUtils.getConnectionLocalDerby("yoochose", "user2", "user2", false);
            PreparedStatement ps = conn.prepareStatement("select userid,timestamp,newuserid from transfer_table");
            ResultSet executeQuery = ps.executeQuery();
            while (executeQuery.next())
            {
                String userid = executeQuery.getString(1);
                LocalDateTime ldt = executeQuery.getTimestamp(2).toLocalDateTime();
                String newuserid = executeQuery.getString(3);
                if (userid == null || userid.equals("null") || newuserid.equals("null") || userid.equals(newuserid))
                {
                    continue;
                }
                boolean found = false;
                if (users.containsKey(userid) || users.containsKey(newuserid))
                {
                    if (!users.containsKey(userid))
                    {
                        found = true;
                        users.put(userid, users.get(newuserid));
                    }
                    if (!users.containsKey(newuserid))
                    {
                        found = true;
                        users.put(newuserid, users.get(userid));
                    }
                }
                if (!found)
                {
                    users.put(userid, count);
                    users.put(newuserid, count);
                    count++;
                }
            }
            executeQuery.close();
//			conn.close();

        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        HashMap<Integer, List<String>> invertedTable = new HashMap<Integer, List<String>>();
        for (Entry<String, Integer> user : users.entrySet())
        {
            String key = user.getKey();
            Integer value = user.getValue();
            if (!invertedTable.containsKey(value))
            {
                invertedTable.put(value, new ArrayList<String>());
            }
            invertedTable.get(value).add(key);
        }
        for (Entry<Integer, List<String>> iterable_element : invertedTable.entrySet())
        {
            for (String user : iterable_element.getValue())
            {
                FileUtils.writeToFile(path, user + " ", true);
            }
            FileUtils.writeToFile(path, "\n", true);
        }
    }

    private static void writeLogins()
    {
        try
        {
            Connection conn = DBUtils.getConnectionLocalDerby("yoochose", "user2", "user2", false);
            DBUtils.writeQueryToCSV(conn,
                    "select userid,timestamp,NEWUSERID from transfer_table where userid <> 'null' and newuserid <>'null' and userid <> newuserid order by timestamp",
                    "logins.csv");
        } catch (SQLException | IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void writeHistoryOfUsers()
    {
        try
        {
            Connection conn = DBUtils.getConnectionLocalDerby("yoochose", "user2", "user2", false);
            PreparedStatement ps = conn.prepareStatement("select distinct userid from buy_table");
            ResultSet executeQuery = ps.executeQuery();
            while (executeQuery.next())
            {
                String userid = executeQuery.getString(1);
                DBUtils.writeQueryToCSV(conn,
                        "select * from buy_table where userid='" + userid + "' order by timestamp,itemid,price",
                        "users\\" + userid + ".csv");
            }
            executeQuery.close();
            conn.close();
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private static void deleteTables() throws SQLException
    {
        // TODO Auto-generated method stub
        Connection conn = DBUtils.getConnectionLocalDerby("yoochose", "user2", "user2", true);
        ITableAction[] t1 = new ITableAction[]{new TableActionBuy(), new TableActionClick(),
                new TableActionRecommendedClick(), new TableActionTransfer()};
        for (ITableAction iTableAction : t1)
        {
            iTableAction.dropTable(conn);
            iTableAction.createTable(conn);
        }

    }

    private static void createFiles() throws IOException
    {
        String feb = "C:\\Users\\Michael\\Documents\\�������\\���� ���\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-feb\\461-tracking-feb.out";
        String mar = "C:\\Users\\Michael\\Documents\\�������\\���� ���\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-mar\\461-tracking-mar.out";
        String[] filenames = new String[]{feb, mar};
        IInputActions input = new InputActionFromFile(filenames);
        IParseActions parser = new ParserBasic(new IActionParser[]{new BuyActionParserFromRaw(),
                new ClickActionParserFromRaw(), new ClickRecommendedParserFromRaw(), new TransferActionParser()});
        IOutput output = new OutputCSV("buy.csv", "click.csv", "rclick.csv", "trans.csv");
        YCReaderBatch reader = new YCReaderBatch(input, output, parser);
        reader.parse();
    }

    private static void updateSessions()
    {
        boolean emtpy = true;
        try
        {
            Connection conn = DBUtils.getConnectionLocalDerby("yoochose", "user2", "user2", false);
            PreparedStatement ps = conn.prepareStatement("select userid,timestamp,newuserid from transfer_table");
            ResultSet executeQuery = ps.executeQuery();
            while (executeQuery.next())
            {
                String userid = executeQuery.getString(1);
                LocalDateTime ldt = executeQuery.getTimestamp(2).toLocalDateTime();
                String newuserid = executeQuery.getString(3);
                // update clicks
                // PreparedStatement update = conn.prepareStatement("update
                // click_table set userid=? where userid=?");
                // update.setString(1, newuserid);
                // update.setString(2, userid);
                // update.executeUpdate();
                //
                // PreparedStatement update = conn.prepareStatement("update
                // buy_table set userid=? where userid=?");
                // update.setString(1, newuserid);
                // update.setString(2, userid);
                // update.executeUpdate();
                //
                // PreparedStatement update = conn.prepareStatement("update
                // rclick_table set userid=? where userid=?");
                // update.setString(1, newuserid);
                // update.setString(2, userid);
                // update.executeUpdate();
                if (userid == null || userid.equals("null") || newuserid.equals("null") || userid.equals(newuserid))
                {
                    continue;
                }
                PreparedStatement ps2 = conn
                        .prepareStatement("select userid,timestamp from click_table where userid=? and timestamp > ?");
                ps2.setString(1, userid);
                ps2.setTimestamp(2, Timestamp.valueOf(ldt));
                ResultSet executeQuery2 = ps2.executeQuery();
                while (executeQuery2.next())
                {
                    if (executeQuery.getString(1).equals("null"))
                    {
                        continue;
                    }
                    emtpy = false;
                    System.out.println("usrid =" + userid + " newuserid=" + newuserid);
                    System.out.println(executeQuery.getString(1) + " " + executeQuery.getTimestamp(2));
                }
                ps2.close();
                // change userid
            }
            executeQuery.close();
//			conn.close();
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // if (emtpy)
        // {
        // try
        // {
        // Connection conn = DBUtils.getConnectionLocalDerby("yoochose",
        // "user2", "user2", false);
        // PreparedStatement ps = conn.prepareStatement("select
        // userid,timestamp,newuserid from transfer_table");
        // ResultSet executeQuery = ps.executeQuery();
        // while (executeQuery.next())
        // {
        // String userid = executeQuery.getString(1);
        // String newuserid = executeQuery.getString(1);
        // if(userid.equals(newuserid))
        // // update clicks
        // PreparedStatement update = conn.prepareStatement("update click_table
        // set userid=? where userid=?");
        // update.setString(1, newuserid);
        // update.setString(2, userid);
        // update.executeUpdate();
        // update.close();
        //
        // update = conn.prepareStatement("update buy_table set userid=? where
        // userid=?");
        // update.setString(1, newuserid);
        // update.setString(2, userid);
        // update.executeUpdate();
        // update.close();
        //
        // update = conn.prepareStatement("update rclick_table set userid=?
        // where userid=?");
        // update.setString(1, newuserid);
        // update.setString(2, userid);
        // update.executeUpdate();
        // update.close();
        // }
        // executeQuery.close();
        // conn.close();
        // }
        // catch (SQLException e)
        // {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        // }
    }

    private static void createSession() throws Exception
    {
        CreateData.createFiles("2016-02-08");
        String outputbuys = "output_buy.txt";
        String outputClicks = "output_click.txt";
        String sessionFile = "sessions.csv";
        String buyFile = "clicks.csv";
        String clickFile = "buys.csv";
        CreateSessions cr = new CreateSessions(clickFile, buyFile, sessionFile, outputClicks, outputbuys);
        cr.run();
    }

    public static void run(Connection conn) throws SQLException, IOException
    {
//		 createFiles();
//		 deleteTables();
//		 RemoveDuplicates.runAll();
//		 writeHistoryOfUsers();
//		 writeLogins();
        writeFullTable(conn);
    }
}
