package BL.controllers.flow;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import BL.controllers.output.db.actions.ITableAction;
import BL.controllers.output.db.actions.TableActionAbstractClick;
import BL.controllers.output.db.actions.TableActionBuy;
import BL.controllers.output.db.actions.TableActionClick;
import DAL.utils.DBUtils;
import DAL.utils.FileUtils;
import DAL.utils.Pair;

public class UpdateWithLogins
{
    private static final String CREATE_TEMP_BUY_TABLE = null;
    private static final String CREATE_TEMP_CLICK_TABLE = null;
    private static HashMap<String, String> usersTable;

    private static void updateLogins_old(Connection conn) throws IOException
    {
        HashMap<String, Integer> users = fromDBToTable(conn);
        HashMap<Integer, List<String>> invertedTable = invertTable(users);
        FileUtils.writeToFile("logins.txt", "");
        updateLoginsFromTable(conn, invertedTable);
    }

    private static void updateLoginsFromTable(Connection conn, HashMap<Integer, List<String>> invertedTable) throws IOException
    {
        List<String> users = getAllUsers(conn);
        FileWriter writer = new FileWriter("output.txt");
        for (String str : users)
        {
            writer.write(str + "\n");
        }
        System.out.println("MICHAEL GO TO CHECK!!!");
        writer.close();
        Map<String, String> convertTable = getConvertTable(invertedTable, users);
        createLoginsTable(conn, convertTable);
        try
        {
            DBUtils.renameTable(conn, "click_table", "click_table_old");
            DBUtils.renameTable(conn, "buy_table", "buy_table_old");
            DBUtils.renameTable(conn, "temp_click_table", "click_table");
            DBUtils.renameTable(conn, "temp_buy_table", "buy_table");
            DBUtils.runDDLQuery(conn, "ALTER TABLE buy_table RENAME COLUMN  useridreal to userid");
            DBUtils.runDDLQuery(conn, "ALTER TABLE click_table RENAME COLUMN  useridreal to userid");
            DBUtils.runDDLQuery(conn, "CREATE INDEX new_buy_index_userid  on buy_table (userid)");
            DBUtils.runDDLQuery(conn, "CREATE INDEX new_buy_index_timestamp on buy_table (timestamp)");
            DBUtils.runDDLQuery(conn, "CREATE INDEX new_buy_index_session on buy_table (userid,timestamp)");
            DBUtils.runDDLQuery(conn, "CREATE INDEX new_buy_index_buy on buy_table (userid,timestamp)");
            DBUtils.runDDLQuery(conn, "CREATE INDEX new_buy_index_price on buy_table (price)");
//			DBUtils.runDDLQuery(conn, "CREATE INDEX new_buy_index_sessionid on buy_table (sessionid)");

            DBUtils.runDDLQuery(conn, "CREATE INDEX new_click_userid  on click_table (userid)");
            DBUtils.runDDLQuery(conn, "CREATE INDEX new_click_timestamp on click_table (timestamp)");
            DBUtils.runDDLQuery(conn, "CREATE INDEX new_click_itemid on click_table (itemid)");
            DBUtils.runDDLQuery(conn, "CREATE INDEX new_click_session on click_table (userid,timestamp,itemid)");
//			DBUtils.runDDLQuery(conn, "CREATE INDEX new_click_sessionid on click_table (sessionid)");

        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//		reIndexes(conn);
    }

    private static void createLoginsTable(Connection conn, Map<String, String> convertTable)
    {
        dropWithoutError(conn, "temp_click_table");
        dropWithoutError(conn, "temp_buy_table");
        dropWithoutError(conn, "templogin_table");
        try
        {
            DBUtils.runDDLQuery(conn, "CREATE TABLE templogin_table (useridreal varchar(255),useridtemp varchar(255))");
            String path = "logins_temp.txt";
            FileUtils.writeToFile(path, "useridreal,useridtemp\n");
            for (Entry<String, String> usersPair : convertTable.entrySet())
            {
                FileUtils.writeToFile(path, usersPair.getValue() + "," + usersPair.getKey() + "\n", true);
            }
            DBUtils.copyCSVToPostgre(conn, "templogin_table", path);
            DBUtils.runDDLQuery(conn, "CREATE INDEX templogin_table_index on templogin_table (useridreal,useridtemp)");
            DBUtils.runDDLQuery(conn, "CREATE INDEX templogin_table_useridreal_index on templogin_table (useridtemp)");
            DBUtils.runDDLQuery(conn, "CREATE TABLE temp_click_table AS SELECT templogin_table.useridreal,click_table.timestamp,click_table.itemId,click_table.recommended,click_table.sessionid FROM click_table INNER JOIN templogin_table ON templogin_table.useridtemp = click_table.userid");
            DBUtils.runDDLQuery(conn, "CREATE TABLE temp_buy_table AS SELECT templogin_table.useridreal,buy_table.timestamp,buy_table.itemId,buy_table.quantity,buy_table.price,buy_table.sessionid FROM buy_table INNER JOIN templogin_table ON templogin_table.useridtemp = buy_table.userid");
            DBUtils.runDDLQuery(conn, "DROP INDEX templogin_table_index");
            DBUtils.runDDLQuery(conn, "DROP INDEX templogin_table_useridreal_index");

            DBUtils.dropTable(conn, "templogin_table");

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

    private static void dropWithoutError(Connection conn, String tableName)
    {
        try
        {
            DBUtils.dropTable(conn, tableName);
        } catch (SQLException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }

    private static List<String> getAllUsers(Connection conn)
    {
        List<String> allUsers = null;
        try
        {
            allUsers = DBUtils.getAllUsers(conn);
        } catch (SQLException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        return allUsers;
    }

    private static Map<String, String> getConvertTable(HashMap<Integer, List<String>> invertedTable, List<String> users)
    {
        Map<String, String> convertTable = new HashMap<String, String>();
        for (String user : users)
        {
            convertTable.put(user, user);
        }
        for (List<String> usersGroup : invertedTable.values())
        {
            String first = usersGroup.get(0);
            for (String user : usersGroup)
            {
                convertTable.put(user, first);
            }
        }
        return convertTable;
    }


    private static HashMap<Integer, List<String>> invertTable(HashMap<String, Integer> users)
    {
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
        return invertedTable;
    }

    private static HashMap<String, Integer> fromDBToTable(Connection conn)
    {
        HashMap<String, Integer> users = new HashMap<String, Integer>();
        int count = 0;
        try
        {
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
        } catch (SQLException e)
        {
            e.printStackTrace();
        }
        return users;
    }

    private static List<String> sortArrayList(List<String> value)
    {
        Collections.sort(value, new Comparator<String>()
        {
            @Override
            public int compare(String o1, String o2)
            {
                if ((o2.equals("null")) || (o1.indexOf('-') == -1 && o2.indexOf('-') != -1))
                {
                    return -1;
                }
                if ((o1.equals("null")) || (o1.indexOf('-') != -1 && o2.indexOf('-') == -1))
                {
                    return 1;
                }
                return o1.compareTo(o2);
            }
        });
        return value;
    }

    public static void run(Connection conn) throws IOException
    {
        updateLogins(conn);

    }

    private static void updateLogins(Connection conn) throws IOException
    {
//		reIndexes(conn);
        HashMap<Pair<String, LocalDate>, Integer> users = getUserTable(conn);
        HashMap<Integer, List<String>> invertedTable = invertedTable(users);
        FileUtils.writeToFile("logins.txt", "");
        updateLoginsFromTable(conn, invertedTable);
    }


    private static void reIndexes(Connection conn)
    {

        ITableAction a1 = new TableActionBuy();
        ITableAction a2 = new TableActionClick();
        ITableAction[] arr = new ITableAction[]{a1, a2};
        for (ITableAction iTableAction : arr)
        {
            try
            {
                iTableAction.createIndex(conn);
            } catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
    }

    private static void createTempTables(Connection conn)
    {
        try
        {
            ITableAction a1 = new TableActionBuy();
            ITableAction a2 = new TableActionAbstractClick();
            ITableAction[] arr = new ITableAction[]{a1, a2};
            for (ITableAction iTableAction : arr)
            {
                iTableAction.dropTempTable(conn);
                iTableAction.createTempTable(conn);
            }
        } catch (SQLException e)
        {
            e.printStackTrace();
        }


    }

    private static HashMap<Integer, List<String>> invertedTable(HashMap<Pair<String, LocalDate>, Integer> users)
    {
        HashMap<Integer, List<String>> invertedTable = new HashMap<Integer, List<String>>();
        for (Entry<Pair<String, LocalDate>, Integer> user : users.entrySet())
        {
            Pair<String, LocalDate> key = user.getKey();
            Integer value = user.getValue();
            if (!invertedTable.containsKey(value))
            {
                invertedTable.put(value, new ArrayList<String>());
            }
            invertedTable.get(value).add(key.first);
        }
        return invertedTable;
    }

    private static HashMap<Pair<String, LocalDate>, Integer> getUserTable(Connection conn)
    {
        HashMap<Pair<String, LocalDate>, Integer> users = new HashMap<Pair<String, LocalDate>, Integer>();
        int count = 0;
        try
        {
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
                Pair<String, LocalDate> p1 = new Pair<String, LocalDate>(userid, ldt.toLocalDate());
                Pair<String, LocalDate> p2 = new Pair<String, LocalDate>(newuserid, ldt.toLocalDate());
                if (users.containsKey(p1) || users.containsKey(p2))
                {
                    if (!users.containsKey(p1))
                    {
                        found = true;
                        users.put(p1, users.get(p2));
                    }
                    if (!users.containsKey(p2))
                    {
                        found = true;
                        users.put(p2, users.get(p1));
                    }
                }
                if (!found)
                {
                    users.put(p1, count);
                    users.put(p2, count);
                    count++;
                }
            }
            executeQuery.close();
        } catch (SQLException e)
        {
            e.printStackTrace();
        }
        return users;
    }
}
