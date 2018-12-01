package BL.controllers.flow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import BL.dataobjects.actions.YCSession;
import DAL.utils.DBUtils;
import DAL.utils.DateUtils;
import DAL.utils.EmailUtils;
import DAL.utils.FileUtils;

public class CreateSessions
{
    private static final int MAX_CLICKS = 2701;
    private String clickFile;
    private String buyFile;
    private String sessionFile;

    private String outputClicks;
    private String outputBuys;

    private Map<YCSession, Integer> sessionTable;

    public CreateSessions(String clickFile, String buyFile, String sessionFile, String outputClicks, String outputbuys)
    {
        super();
        this.clickFile = clickFile;
        this.buyFile = buyFile;
        this.sessionFile = sessionFile;
        this.outputClicks = outputClicks;
        this.outputBuys = outputbuys;
    }

    private static Map<String, Map<LocalDateTime, String>> loadLogins()
    {
        Map<String, Map<LocalDateTime, String>> ans = new HashMap<String, Map<LocalDateTime, String>>();
        try
        {
            Connection conn = DBUtils.getConnectionLocalDerby("yoochose", "user2", "user2", false);
            PreparedStatement ps = conn.prepareStatement("select userid,timestamp,newuserid from transfer_table");
            ResultSet executeQuery = ps.executeQuery();
            while (executeQuery.next())
            {
                String userid = executeQuery.getString(1);
                LocalDateTime ldt = executeQuery.getTimestamp(2).toLocalDateTime();
                String newuserid = executeQuery.getString(1);
                if (!ans.containsKey(userid))
                {
                    ans.put(userid, new HashMap<LocalDateTime, String>());
                }
                ans.get(userid).put(ldt, newuserid);
            }
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // TODO Auto-generated method stub
        return ans;
    }

    public void run() throws Exception
    {
        createClickFile();
        dumpSessionTableToCsv();
        createBuyFile();
    }

    private void dumpSessionTableToCsv() throws IOException
    {
        Map<YCSession, Integer> sortedMap = sessionTable.entrySet().stream().sorted(Entry.comparingByValue())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        FileUtils.writeToFile(sessionFile, "sessionid,date,userid\n");
        for (Map.Entry<YCSession, Integer> tuple : sortedMap.entrySet())
        {
            FileUtils.writeToFile(sessionFile,
                    tuple.getValue() + "," + tuple.getKey().getDate() + "," + tuple.getKey().getUserid() + "\n", true);
        }
    }

    private void createBuyFile() throws IOException
    {
        int count = 0;
        FileUtils.writeToFile(outputBuys, "sessionid,itemid");
        File f = new File(buyFile);
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        line = br.readLine();
        while (line != null)
        {
            String[] fields = line.split(",");
            LocalDate date = DateUtils.getDateFromString(fields[0]);
            String userid = fields[1];
            String itemId = fields[2];
            YCSession s = new YCSession(userid, date);
            int sessionId;
            if (sessionTable.containsKey(s))
            {
                sessionId = sessionTable.get(s);
            } else
            {
                count++;
                sessionId = -1;
            }
            if (sessionId != -1)
            {
                FileUtils.writeToFile(outputBuys, sessionId + "," + itemId + "\n", true);
            } else
            {
                // FileUtils.writeToFile(outputBuys, sessionId + "," + itemId +
                // s + "\n", true);
                FileUtils.writeToFile(outputBuys, sessionId + "," + itemId + "\n", true);
            }
            line = br.readLine();
        }
        br.close();
        fr.close();
        System.out.println("without click:" + count);
    }

    private void createClickFile() throws Exception
    {
        int maxCount = 0;
        sessionTable = new HashMap<YCSession, Integer>();
        FileUtils.writeToFile(outputClicks, "sessionid");
        for (int i = 0; i < MAX_CLICKS; i++)
        {
            FileUtils.writeToFile(outputClicks, ",click" + (i + 1), true);
        }
        FileUtils.writeToFile(outputClicks, "\n", true);
        StringBuilder sb = new StringBuilder();
        File f = new File(clickFile);
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        line = br.readLine();
        YCSession correntSession = null;
        int count = 0;
        int sessionNum = 1;
        while (line != null)
        {
            String[] fields = line.split(",");
            LocalDate date = DateUtils.getDateFromString(fields[0]);
            String userid = fields[1];
            String itemid = fields[2];
            YCSession s = new YCSession(userid, date);
            boolean nequal = !s.equals(correntSession);
            if (nequal)
            {
                if (correntSession != null && count > 0)
                {
                    FileUtils.writeToFile(outputClicks, sessionNum + ",", true);
                    FileUtils.writeToFile(outputClicks, sb.toString(), true);
                    for (int i = 0; i < Math.max(0, MAX_CLICKS - count); i++)
                    {
                        FileUtils.writeToFile(outputClicks, ",0", true);
                    }
                    FileUtils.writeToFile(outputClicks, "\n", true);
                }
                correntSession = s;
                sb.setLength(0);
                if (sessionTable.containsKey(s))
                {
                    System.out.println(s);
                }
                sessionTable.put(s, sessionNum);
                // sb.append(sessionTable.get(s)+"->"+itemid);
                sb.append(itemid);
                count = 0;
                sessionNum++;
            } else
            {
                if (sb.length() > 0)
                {
                    sb.append(",");
                }
                sb.append(itemid);
                count++;
            }
            line = br.readLine();
        }
        br.close();
        fr.close();

    }

}
