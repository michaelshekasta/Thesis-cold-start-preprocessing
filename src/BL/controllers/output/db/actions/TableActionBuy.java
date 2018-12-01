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
import java.util.List;

import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.Action.ActionType;
import BL.dataobjects.actions.ActionBuy;
import DAL.utils.DBUtils;

public class TableActionBuy implements ITableAction
{

    private static final String CREATE_BUY_TABLE_SQL = "CREATE TABLE buy_table ( timestamp TIMESTAMP,userid varchar(255), itemid varchar(255),price REAL,quantity int,sessionid int)";
    private static final String INSERT_BUY_SQL = "insert into buy_table (userid,timestamp,itemId,quantity,price) values(?,?,?,?,?)";
    private static final String DROP_BUY_TABLE_SQL = "drop TABLE buy_table";

    private static final String INDEX_BUY_TABLE_SQL1 = "CREATE INDEX buy_index_userid  on buy_table (userid)";
    private static final String INDEX_BUY_TABLE_SQL2 = "CREATE INDEX buy_index_timestamp on buy_table (timestamp)";
    private static final String INDEX_BUY_TABLE_SQL3 = "CREATE INDEX buy_index_session on buy_table (userid,timestamp)";
    private static final String INDEX_BUY_TABLE_SQL4 = "CREATE INDEX buy_index_buy on buy_table (userid,timestamp,itemId)";
    private static final String INDEX_BUY_TABLE_SQL5 = "CREATE INDEX buy_index_price on buy_table (price)";
    private static final String INDEX_BUY_TABLE_SQL6 = "CREATE INDEX buy_index_sessionid on buy_table (sessionid)";

    private static final String DROP_INDEX_BUY_TABLE_SQL1 = "DROP INDEX buy_index_userid";
    private static final String DROP_INDEX_BUY_TABLE_SQL2 = "DROP INDEX buy_index_timestamp";
    private static final String DROP_INDEX_BUY_TABLE_SQL3 = "DROP INDEX buy_index_session";
    private static final String DROP_INDEX_BUY_TABLE_SQL4 = "DROP INDEX buy_index_buy";
    private static final String DROP_INDEX_BUY_TABLE_SQL5 = "DROP INDEX buy_index_price";
    private static final String DROP_INDEX_BUY_TABLE_SQL6 = "DROP INDEX buy_index_price";

    private static final String FIND_DUPLICATE_BUY_TABLE_SQL = "select distinct b.USERID,b.timestamp,b.ITEMID,b.QUANTITY,b.PRICE "
            + "from BUY_TABLE b " + "inner join ("
            + "SELECT b0.USERID,b0.timestamp,b0.ITEMID,b0.QUANTITY,b0.PRICE, COUNT(*) AS cnt " + "FROM BUY_TABLE b0 "
            + "GROUP BY b0.USERID,b0.timestamp,b0.ITEMID,b0.QUANTITY,b0.PRICE " + "HAVING COUNT(*) > 1 " + ") b2"
            + " on (b.USERID = b2.USERID and b.timestamp = b2.timestamp and b.ITEMID = b2.ITEMID and b.QUANTITY = b2.QUANTITY and b.PRICE = b2.PRICE)";
    private static final String DELETE_BUY_TABLE_SQL = "delete from buy_table where userid=? and timestamp=? and itemId=? and quantity=? and price=?";

    private List<Action> actions;

    public TableActionBuy()
    {
        super();
        actions = new ArrayList<Action>(DBUtils.MAX_BATCH_COMMANDS);
    }

    @Override
    public ActionType getActionType()
    {
        return ActionType.buy;
    }

    @Override
    public void createTable(Connection conn) throws SQLException
    {
        DBUtils.runDDLQuery(conn, CREATE_BUY_TABLE_SQL);

    }

    @Override
    public void insert(Connection conn, Action a) throws SQLException
    {
        if (a.getActionType() != ActionType.buy)
            return;
        ActionBuy ba = (ActionBuy) a;
        PreparedStatement ps = conn.prepareStatement(INSERT_BUY_SQL);
        actionBuyToPS(ba, ps);
        ps.executeUpdate();
        ps.close();

    }

    private void actionBuyToPS(ActionBuy ba, PreparedStatement ps) throws SQLException
    {
        ps.setString(1, ba.getUserID());
        ps.setTimestamp(2, Timestamp.valueOf(ba.getTimestamp()));
        ps.setString(3, ba.getItemid());
        ps.setInt(4, ba.getQuantity());
        ps.setDouble(5, ba.getPrice());

    }

    @Override
    public void dropTable(Connection conn) throws SQLException
    {
        // TODO Auto-generated method stub
        removeIndex(conn);
        runDLLWithTryAndCatch(conn, DROP_BUY_TABLE_SQL);
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
        DBUtils.runDDLQuery(conn, INDEX_BUY_TABLE_SQL1);
        DBUtils.runDDLQuery(conn, INDEX_BUY_TABLE_SQL2);
        DBUtils.runDDLQuery(conn, INDEX_BUY_TABLE_SQL3);
        DBUtils.runDDLQuery(conn, INDEX_BUY_TABLE_SQL4);
        DBUtils.runDDLQuery(conn, INDEX_BUY_TABLE_SQL5);
        DBUtils.runDDLQuery(conn, INDEX_BUY_TABLE_SQL6);
    }

    @Override
    public void insertBatch(Connection conn, Action a) throws SQLException
    {
        if (a.getActionType() != this.getActionType())
        {
            return;
        }
        actions.add(a);
        if (actions.size() + 2 == DBUtils.MAX_BATCH_COMMANDS)
        {
            writeBatchListToDB(conn);
        }
    }

    private void writeBatchListToDB(Connection conn) throws SQLException
    {
        PreparedStatement ps = conn.prepareStatement(INSERT_BUY_SQL);
        for (Action an : actions)
        {

            ActionBuy ba = (ActionBuy) an;
            actionBuyToPS(ba, ps);
            ps.addBatch();
            ps.clearParameters();
        }
        ps.executeBatch();
        ps.clearBatch();
        actions.clear();
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
        PreparedStatement ps = conn.prepareStatement(DELETE_BUY_TABLE_SQL);
        int count = 0;
        for (Action an : duplicate)
        {
            ActionBuy ba = (ActionBuy) an;
            actionBuyToPS(ba, ps);
            ps.addBatch();
            ps.clearParameters();
            count++;
            if (count == DBUtils.MAX_BATCH_COMMANDS)
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
        // b.USERID,b.timestamp,b.ITEMID,b.QUANTITY,b.PRIC
        List<Action> ans = new ArrayList<Action>();
        PreparedStatement ps = conn.prepareStatement(FIND_DUPLICATE_BUY_TABLE_SQL);
        ResultSet executeQuery = ps.executeQuery();
        while (executeQuery.next())
        {
            LocalDateTime ldt = executeQuery.getTimestamp(2).toLocalDateTime();
            ActionBuy ab = new ActionBuy(ldt, "461", executeQuery.getString(1), executeQuery.getString(3),
                    executeQuery.getDouble(5), executeQuery.getInt(4));
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
            fos = new FileOutputStream("buys.txt");
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
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL1);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL2);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL3);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL4);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL5);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL6);
    }

    @Override
    public void removeIndexToLogins(Connection conn) throws SQLException
    {
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL1);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL3);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL4);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL5);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL6);
    }

    @Override
    public void resumeIndexToLogins(Connection conn) throws SQLException
    {
        DBUtils.runDDLQuery(conn, INDEX_BUY_TABLE_SQL1);
        DBUtils.runDDLQuery(conn, INDEX_BUY_TABLE_SQL3);
        DBUtils.runDDLQuery(conn, INDEX_BUY_TABLE_SQL4);
        DBUtils.runDDLQuery(conn, INDEX_BUY_TABLE_SQL5);
        DBUtils.runDDLQuery(conn, INDEX_BUY_TABLE_SQL6);

    }

    @Override
    public void createTempTable(Connection conn) throws SQLException
    {
        DBUtils.runDDLQuery(conn, CREATE_BUY_TABLE_SQL.replace("buy", "tempbuy"));
    }

    @Override
    public void dropTempTable(Connection conn)
    {
        try
        {
            DBUtils.dropTable(conn, "tempbuy_table");
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
