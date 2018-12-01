package BL.controllers;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import DAL.CSVReader;
import DAL.Parser;
import DAL.utils.DBUtils;
import DAL.utils.DateUtils;
import DAL.utils.FileUtils;

public class PreproceesData
{

    private static final String SELECT_SQL = "select distinct * from user1.buys order by BUYTIME,user,price,QUANTITY";
    private static final String DROP_BUY_TABLE_SQL = "drop TABLE buys";
    private static final String INSERT_BUY_SQL = "insert into buys (userid,buytime,itemId,quantity,price) values(?,?,?,?,?)";
    private static final String CREATE_BUY_TABLE_SQL = "CREATE TABLE buys ( userid varchar(255), buytime TIMESTAMP,itemid varchar(255),quantity int,price REAL  )";
    private static final String INSERT_CLICK_SQL = "insert into clicks (userid,buytime,itemId) values(?,?,?)";
    private static final String CREATE_CLICK_TABLE_SQL = "CREATE TABLE clicks ( userid varchar(255), buytime TIMESTAMP,itemid varchar(255))";
    private static final String DROP_CLICK_TABLE_SQL = "drop TABLE clicks";

    public static void run() throws IOException, Exception, SQLException
    {
        // init vars
        // createTables();
        // String[] monthes = new String[]{"feb","mar"};
        String[] monthes = new String[]{"mar"};
        for (String month : monthes)
        {
            String dir = "C:\\Users\\Michael\\Documents\\�������\\���� ���\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-"
                    + month + "\\";
            String errorFilePath = dir + "error.log";
            String inputFilePath = dir + "461-tracking-" + month + ".out";
            String outputFilePath = dir + "output.csv";
            // Run parsing
            Parser p = new Parser(errorFilePath, inputFilePath, outputFilePath);
            p.praseAndWrite();
            System.out.println("done to parse");

            // insert data to db
            insertData(month, "BUY");
            insertData(month, "CLICK");
            System.out.println("done to insert data");

            // get the clean data to file
            // writeClearDataToFile(month);
            // System.out.println("done all");
        }
    }

    private static void writeClearDataToFile(String month) throws Exception, SQLException, IOException
    {
        String query = SELECT_SQL;
        Connection derby = DBUtils.getConnectionLocalDerby("yoochose", "user1", "user1", false);
        PreparedStatement ps = derby.prepareStatement(query);
        ResultSet executeQuery = ps.executeQuery();
        String outputFilePath = "C:\\Users\\Michael\\Documents\\�������\\���� ���\\Thesis\\data\\Yoochose\\461-tracking-"
                + month + "\\buys.csv";
        FileUtils.writeToFile(outputFilePath, "userId,timestamp,ItemId,Qty,price\n");
        while (executeQuery.next())
        {
            StringBuilder sb = new StringBuilder();
            sb.append(executeQuery.getString(1) + ",");
            sb.append(executeQuery.getTimestamp(2) + ",");
            sb.append(executeQuery.getString(3) + ",");
            sb.append(executeQuery.getString(4) + ",");
            sb.append(executeQuery.getString(5) + "\n");
            String tuple = sb.toString();
            FileUtils.writeToFile(outputFilePath, tuple, true);
        }
        executeQuery.close();
        ps.close();
        derby.close();
    }

    private static void createTables() throws Exception
    {
        Connection derby = DBUtils.getConnectionLocalDerby("yoochose", "user1", "user1", true);
        DBUtils.runDDLQuery(derby, DROP_BUY_TABLE_SQL);
        DBUtils.runDDLQuery(derby, DROP_CLICK_TABLE_SQL);
        DBUtils.runDDLQuery(derby, CREATE_BUY_TABLE_SQL);
        DBUtils.runDDLQuery(derby, CREATE_CLICK_TABLE_SQL);
    }

    private static void insertData(String month, String event) throws Exception, SQLException
    {
        CSVReader reader = new CSVReader(
                "C:\\Users\\Michael\\Documents\\�������\\���� ���\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-"
                        + month + "\\output.csv");
        Connection derby = DBUtils.getConnectionLocalDerby("yoochose", "user1", "user1", true);
        PreparedStatement ps = null;
        if (event.equals("BUY"))
        {
            ps = derby.prepareStatement(INSERT_BUY_SQL);
        } else
        {
            ps = derby.prepareStatement(INSERT_CLICK_SQL);
        }
        int index = 0;
        int indexPrint = 0;
        boolean first = true;
        for (String line : reader)
        {
            if (indexPrint % 1000 == 0)
            {
                System.out.println(indexPrint + " done");
            }
            if (first)
            {
                first = false;
                continue;
            }
            if (line.indexOf(event) == -1)
            {
                continue;
            }
            ps.clearParameters();
            String[] fields = line.split(",");
            ps.setString(1, fields[1]);
            LocalDateTime localDateTime = DateUtils.getDateTimeFromString(fields[2]);
            Timestamp timestamp = Timestamp.valueOf(localDateTime);
            ps.setTimestamp(2, timestamp);
            ps.setString(3, fields[3]);
            if (event.equals("BUY"))
            {
                ps.setInt(4, Integer.parseInt(fields[4]));
                ps.setFloat(5, Float.parseFloat(fields[5]));
            }
            ps.addBatch();
            index++;
            if (index == DBUtils.MAX_BATCH_COMMANDS)
            {
                index = 0;
                ps.executeBatch();
                // ps.close();
                ps.clearBatch();
            }
            indexPrint++;
        }
        if (index > 0)
        {
            ps.executeBatch();
        }
        ps.close();
        derby.close();
    }
}
