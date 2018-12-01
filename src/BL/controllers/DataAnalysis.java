package BL.controllers;

import java.sql.Connection;

import DAL.utils.DBUtils;

public class DataAnalysis
{
    public static void Analyze() throws Exception
    {
        Connection conn = DBUtils.getConnectionLocalDerby("yoochose", "user1", "user1", false);
        String dir = "C:\\Users\\Michael\\Documents\\�������\\���� ���\\Thesis\\data\\Yoochose\\461-tracking-"
                + "feb\\";
        /*
		 * //count per user String sql1 =
		 * "select userid, count(buytime) as countItem,months,days from YooChoseAnalysis where userid != 'null' group by userid,months,days"
		 * ; DBUtils.writeQueryToCSV(conn, sql1, dir+"countperuser.csv");
		 * System.out.println("done count per user");
		 * 
		 * //avg buy per day String sql2 =
		 * "select months,days,count(*) as count from YooChoseAnalysis where userid != 'null' group by months,days"
		 * ; DBUtils.writeQueryToCSV(conn, sql2, dir+"avgbuyperday.csv");
		 * System.out.println("done avg buy per day");
		 */
        // get all diff item
        String sql3 = "select a1.itemid, a1.buytime as b1, a2.buytime as b2, a1.price as price1, a2.price as price2, a1.userid as u1 , a2.userid as u2 from YooChoseAnalysis  as a1, YooChoseAnalysis  as a2 where a1.itemid = a2.itemid and a1.price != a2.price and a1.buytime < a2.buytime";
        DBUtils.writeQueryToCSV(conn, sql3, dir + "alldiffitem.csv");
        System.out.println("done get all diff item");

        conn.close();
    }
}
