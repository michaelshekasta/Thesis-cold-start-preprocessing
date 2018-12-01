package BL.garbish;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalTime;

import DAL.utils.DBUtils;
import DAL.utils.FileUtils;

public class ChangePriceStat
{
    public static void run() throws Exception
    {
        Connection derby = DBUtils.getConnectionLocalDerby("yoochose", "user1", "user1", false);
        PreparedStatement psItem = derby.prepareStatement("select distinct itemid from buys order by itemid");
        PreparedStatement ps = derby.prepareStatement(
                "select b1.itemid as item,b1.buytime as firstTS,b1.price as firstPrice,b1.userid as user1 ,b2.buytime as SecondTS,b2.price as SecondPrice,b2.userid as user2 from buys as b1, buys as b2 where b1.price != b2.price and b1.buytime < b2.buytime and b1.itemid = b2.itemid and b1.itemid =? order by b1.buytime asc,b2.buytime asc");
        ResultSet resultSet = psItem.executeQuery();
        int j = 0;
        FileUtils.writeToFile("aggrigate.txt", "item,count\n", false);
        while (resultSet.next())
        {
            j++;
            String itemid = resultSet.getString(1);
            ps.setString(1, itemid);
            ResultSet executeQuery = ps.executeQuery();
            String pathname = "diff\\" + itemid + ".txt";
            FileUtils.writeToFile(pathname, "item,buytime1,price1,user1,buytime2,price2,user2\n");
            File f = new File(pathname);
            FileWriter fw = new FileWriter(f, true);
            BufferedWriter bw = new BufferedWriter(fw);
            LocalTime start = LocalTime.now();
            int i = 0;
            // System.out.println("Starting item "+item id);
            // HashSet<String> buytime1Set = new HashSet<String>();
            // HashSet<String> buytime2Set = new HashSet<String>();
            String buytime1 = "-1";
            String price1 = "-1";
            int count = 1;
            while (executeQuery.next())
            {
                String buytime1now = executeQuery.getString(2);
                String price1now = executeQuery.getString(3);
                // if(!buytime1Set.contains(buytime1now) &&
                // !buytime2Set.contains(buytime2now))
                if (!buytime1.equals(buytime1now) && !price1.equals(price1now))
                // if(!buytime1.equals("-2") && !price1.equals("-2"))
                {
                    // buytime1Set.add(buytime1now);
                    // buytime2Set.add(buytime2now);
                    count++;
                    buytime1 = buytime1now;
                    price1 = price1now;
                    bw.write(String.format("%s,%s,%s,%s,%s,%s,%s", executeQuery.getString(1), buytime1now,
                            executeQuery.getString(3), executeQuery.getString(4), executeQuery.getString(5),
                            executeQuery.getString(6), executeQuery.getString(7)) + "\n");
                    i++;
                    if (i % 10000 == 0)
                    {
                        bw.flush();
                    }
                    System.out.print("\r itemid   " + j + "   found " + (i + 1) + "   items" + "   time:"
                            + (LocalTime.now().minusSeconds(((long) (start.toSecondOfDay()))).toString()));

                }
            }
            bw.close();
            fw.close();
            FileUtils.writeToFile("aggrigate.txt", itemid + "," + count + "\n", true);
        }

        derby.close();
    }
}
