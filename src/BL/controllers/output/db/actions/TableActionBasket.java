package BL.controllers.output.db.actions;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.ActionBasket;
import BL.dataobjects.actions.Action.ActionType;
import BL.dataobjects.actions.ActionClick;
import DAL.utils.DBUtils;

public class TableActionBasket implements ITableAction
{

    @Override
    public ActionType getActionType()
    {
        // TODO Auto-generated method stub
        return ActionType.basket;
    }

    private static final int MAX_BATCH = DBUtils.MAX_BATCH_COMMANDS;
    private String CREATE_BASKET_TABLE_SQL;
    private String INSERT_BASKET_SQL;
    private String DROP_BASKET_TABLE_SQL;

    private String INDEX_BASKET_TABLE_SQL1;
    private String INDEX_BASKET_TABLE_SQL2;
    private String INDEX_BASKET_TABLE_SQL3;
    private String INDEX_BASKET_TABLE_SQL4;
    private String INDEX_BASKET_TABLE_SQL5;

    private String DROP_INDEX_BASKET_TABLE_SQL1;
    private String DROP_INDEX_BASKET_TABLE_SQL2;
    private String DROP_INDEX_BASKET_TABLE_SQL3;
    private String DROP_INDEX_BASKET_TABLE_SQL4;
    private String DROP_INDEX_BASKET_TABLE_SQL5;

    private String FIND_DUPLICATE_BASKET_TABLE_SQL;
    private String DELETE_BASKET_TABLE_SQL;

    private Set<Integer> checkDup;
    private List<Action> actions;


    public TableActionBasket()
    {
        super();
        initSQLStrings();
        actions = new ArrayList<Action>(MAX_BATCH);
        checkDup = new HashSet<Integer>();
    }

    private void initSQLStrings()
    {
        CREATE_BASKET_TABLE_SQL = "CREATE TABLE basket_table ( timestamp TIMESTAMP,userid varchar(255), itemid varchar(255), sessionid int)";
        INSERT_BASKET_SQL = "insert into basket_table (userid,timestamp,itemId) values(?,?,?)";
        DROP_BASKET_TABLE_SQL = "drop TABLE basket_table";

        INDEX_BASKET_TABLE_SQL1 = "CREATE INDEX basket_userid on basket_table (userid)";
        INDEX_BASKET_TABLE_SQL2 = "CREATE INDEX basket_timestamp on basket_table (timestamp)";
        INDEX_BASKET_TABLE_SQL3 = "CREATE INDEX basket_itemid on basket_table (itemid)";
        INDEX_BASKET_TABLE_SQL4 = "CREATE INDEX basket_session on basket_table (userid,timestamp,itemid)";
        INDEX_BASKET_TABLE_SQL4 = "CREATE INDEX basket_sessionid on basket_table (sessionid)";

        DROP_INDEX_BASKET_TABLE_SQL1 = "DROP INDEX basket_userid";
        DROP_INDEX_BASKET_TABLE_SQL2 = "DROP INDEX basket_timestamp";
        DROP_INDEX_BASKET_TABLE_SQL3 = "DROP INDEX basket_itemid";
        DROP_INDEX_BASKET_TABLE_SQL4 = "DROP INDEX basket_session";
        DROP_INDEX_BASKET_TABLE_SQL5 = "DROP INDEX basket_sessionid";

        FIND_DUPLICATE_BASKET_TABLE_SQL =
                "select distinct b.USERID,b.timestamp,b.ITEMID "
                        + "from basket_table b "
                        + "inner join ("
                        + "SELECT b0.USERID,b0.timestamp,b0.ITEMID,COUNT(*) AS cnt "
                        + "FROM basket_TABLE b0 "
                        + "GROUP BY b0.USERID,b0.timestamp,b0.ITEMID"
                        + "HAVING COUNT(*) > 1 "
                        + ") b2"
                        + " on (b.USERID = b2.USERID and b.timestamp = b2.timestamp and b.ITEMID = b2.ITEMID)";
        DELETE_BASKET_TABLE_SQL = "delete from basket_table where userid=? and timestamp=? and itemId=? and sessionid=?";

    }

    @Override
    public void createTable(Connection conn) throws SQLException
    {
        DBUtils.runDDLQuery(conn, CREATE_BASKET_TABLE_SQL);

    }

    @Override
    public void insert(Connection conn, Action a) throws SQLException
    {
        if (a.getActionType() != getActionType())
            return;
        ActionBasket ba = (ActionBasket) a;
        ba.setActionType(getActionType());
        PreparedStatement ps = conn.prepareStatement(INSERT_BASKET_SQL);
        actionBasketToPS(ba, ps);
        ps.executeUpdate();
        ps.close();

    }

    private void actionBasketToPS(ActionBasket ba, PreparedStatement ps) throws SQLException
    {
        ps.setString(1, ba.getUserID());
        ps.setTimestamp(2, Timestamp.valueOf(ba.getTimestamp()));
        ps.setString(3, ba.getItemid());
        int recommended = 0;
        if (ba.getActionType() == ActionType.clickrecommended)
        {
            recommended = 1;
        }
        ps.setInt(3, recommended);

    }

    @Override
    public void dropTable(Connection conn) throws SQLException
    {
        // TODO Auto-generated method stub
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL1);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL2);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL3);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL4);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL5);
        runDLLWithTryAndCatch(conn, DROP_BASKET_TABLE_SQL);
    }

    private void runDLLWithTryAndCatch(Connection conn, String sql)
    {
        try
        {
            DBUtils.runDDLQuery(conn, sql);
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
        }
    }

    @Override
    public void createIndex(Connection conn) throws SQLException
    {
        removeIndex(conn);
        DBUtils.runDDLQuery(conn, INDEX_BASKET_TABLE_SQL1);
        DBUtils.runDDLQuery(conn, INDEX_BASKET_TABLE_SQL2);
        DBUtils.runDDLQuery(conn, INDEX_BASKET_TABLE_SQL3);
        DBUtils.runDDLQuery(conn, INDEX_BASKET_TABLE_SQL4);
        DBUtils.runDDLQuery(conn, INDEX_BASKET_TABLE_SQL5);
    }

    @Override
    public void insertBatch(Connection conn, Action a) throws SQLException
    {
        if (a.getActionType() != this.getActionType() && checkDup.contains(a.hashCode()))
        {
            return;
        }
        checkDup.add(a.hashCode());
        actions.add(a);
        if (actions.size() + 2 == MAX_BATCH)
        {
            writeBatchListToDB(conn);
        }
    }

    private void writeBatchListToDB(Connection conn) throws SQLException
    {
        PreparedStatement ps = conn.prepareStatement(INSERT_BASKET_SQL);
        for (Action an : actions)
        {

            ActionBasket ba = (ActionBasket) an;
            ba.setActionType(getActionType());
            actionBasketToPS(ba, ps);
            ps.addBatch();
            ps.clearParameters();
        }
        ps.executeBatch();
        ps.clearBatch();
        actions.clear();
        System.gc();
    }

    @Override
    public void finishBatch(Connection conn) throws SQLException
    {
        writeBatchListToDB(conn);
    }

    @Override
    public void removeDuplicate(Connection conn) throws SQLException
    {
        List<Action> duplicate = getDuplicateActions(conn);
        deleteBatch(conn, duplicate);
        insertBatch(conn, duplicate);
        writeToFile(duplicate);

    }

    private void insertBatch(Connection conn, List<Action> duplicate) throws SQLException
    {
        for (Action action : duplicate)
        {
            this.insertBatch(conn, action);
        }
        finishBatch(conn);

    }

    private void deleteBatch(Connection conn, List<Action> duplicate) throws SQLException
    {
        PreparedStatement ps = conn.prepareStatement(DELETE_BASKET_TABLE_SQL);
        int count = 0;
        for (Action an : duplicate)
        {
            ActionBasket ba = (ActionBasket) an;
            ba.setActionType(getActionType());
            actionBasketToPS(ba, ps);
            ps.addBatch();
            ps.clearParameters();
            count++;
            if (count == MAX_BATCH)
            {
                ps.executeBatch();
                ps.clearBatch();
                count = 0;
            }
        }
        ps.executeBatch();
        ps.clearBatch();
        ps.close();
    }

    private List<Action> getDuplicateActions(Connection conn) throws SQLException
    {
        List<Action> ans = new ArrayList<Action>();
        PreparedStatement ps = conn.prepareStatement(FIND_DUPLICATE_BASKET_TABLE_SQL);
        ResultSet executeQuery = ps.executeQuery();
        while (executeQuery.next())
        {
            LocalDateTime ldt = executeQuery.getTimestamp(2).toLocalDateTime();
            ActionClick ab = new ActionClick(ldt, "461", executeQuery.getString(1), executeQuery.getString(3));
            ab.setActionType(getActionType());
            ans.add(ab);
        }
        return ans;
    }

    private void writeToFile(List<Action> lst)
    {
        FileOutputStream fos = null;
        ObjectOutputStream oos = null;
        try
        {
            fos = new FileOutputStream(getActionType() + ".txt");
            oos = new ObjectOutputStream(fos);
            oos.writeObject(lst);
        } catch (FileNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try
        {
            if (oos != null)
            {
                oos.close();
            }
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try
        {
            if (fos != null)
            {
                fos.close();
            }
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void removeIndex(Connection conn) throws SQLException
    {
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL1);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL2);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL3);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL4);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL5);

    }

    @Override
    public void removeIndexToLogins(Connection conn) throws SQLException
    {
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL1);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL4);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BASKET_TABLE_SQL5);
    }

    @Override
    public void resumeIndexToLogins(Connection conn) throws SQLException
    {
        DBUtils.runDDLQuery(conn, INDEX_BASKET_TABLE_SQL1);
        DBUtils.runDDLQuery(conn, INDEX_BASKET_TABLE_SQL3);
        DBUtils.runDDLQuery(conn, INDEX_BASKET_TABLE_SQL4);
        DBUtils.runDDLQuery(conn, INDEX_BASKET_TABLE_SQL5);

    }

    @Override
    public void createTempTable(Connection conn) throws SQLException
    {
        DBUtils.runDDLQuery(conn, CREATE_BASKET_TABLE_SQL.replace("basket", "newbasket"));

    }

    @Override
    public void dropTempTable(Connection conn)
    {
        // TODO Auto-generated method stub

    }

}
