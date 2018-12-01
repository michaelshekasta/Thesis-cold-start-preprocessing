package BL.controllers.flow;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

import DAL.utils.FileUtils;

public class CreateSquenceDataSession
{

    private static final int BUY_CONSTANT = 100000000; //000000000100200792

    public static void run(Connection conn,String path) throws SQLException
    {
        try
        {
            FileUtils.writeToFile(path, "");
        }
        catch (IOException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        String sql ="select sessionid,timestamp,itemid,action from ( 	( 		select sessionid,timestamp,itemid,'buy' as action 		from buy_table 		where sessionid <> -1 		order by sessionid,itemid 	) 	union 	( 		select sessionid,timestamp,itemid,'click' as action 		from click_table 		where sessionid <> -1 		order by sessionid,itemid 	) ) as t where sessionid <> -1 order by sessionid,timestamp";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        StringBuilder row = new StringBuilder();
        int prevsessionid = -1;
        while(resultSet.next())
        {
            int sessionid = resultSet.getInt(1);
            Date date = resultSet.getDate(2);
            LocalDate sessoinDate=date.toLocalDate();
            String itemid = resultSet.getString(3);
            String action = resultSet.getString(4);
            if(prevsessionid != sessionid)
            {
                prevsessionid = sessionid;
                row.append("\n");
                try
                {
                    FileUtils.writeToFile(path, row.toString(),true);
                }
                catch (IOException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                row.setLength(0);
                row.append(sessionid);
            }
            row.append(" ");
            int itemidint = Integer.parseInt(itemid);
            if(action.equals("buy"))
            {
                row.append((itemidint+BUY_CONSTANT));
            }
            if(action.equals("click"))
            {
                row.append(itemidint);
            }
        }
        resultSet.close();
        ps.close();
    }


}
