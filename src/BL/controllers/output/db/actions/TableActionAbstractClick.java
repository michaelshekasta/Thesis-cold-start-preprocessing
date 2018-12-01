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
import BL.dataobjects.actions.Action.ActionType;
import BL.dataobjects.actions.ActionClick;
import DAL.utils.DBUtils;

public class TableActionAbstractClick implements ITableAction
{

    private static final int MAX_BATCH = DBUtils.MAX_BATCH_COMMANDS;
    private String CREATE_CLICK_TABLE_SQL;
    private String INSERT_CLICK_SQL;
    private String DROP_CLICK_TABLE_SQL;

    private String INDEX_CLICK_TABLE_SQL1;
    private String INDEX_CLICK_TABLE_SQL2;
    private String INDEX_CLICK_TABLE_SQL3;
    private String INDEX_CLICK_TABLE_SQL4;
    private String INDEX_CLICK_TABLE_SQL5;

    private String DROP_INDEX_BUY_TABLE_SQL1;
    private String DROP_INDEX_BUY_TABLE_SQL2;
    private String DROP_INDEX_BUY_TABLE_SQL3;
    private String DROP_INDEX_BUY_TABLE_SQL4;
    private String DROP_INDEX_BUY_TABLE_SQL5;

    private String FIND_DUPLICATE_CLICK_TABLE_SQL;
    private String DELETE_CLICK_TABLE_SQL;

    private Set<Integer> checkDup;
    private List<Action> actions;


    public TableActionAbstractClick()
    {
        super();
        initSQLStrings();
        actions = new ArrayList<Action>(MAX_BATCH);
        checkDup = new HashSet<Integer>();
    }

    private void initSQLStrings()
    {
        CREATE_CLICK_TABLE_SQL = "CREATE TABLE click_table ( timestamp TIMESTAMP,userid varchar(255), itemid varchar(255),recommended int, sessionid int)";
        INSERT_CLICK_SQL = "insert into click_table (userid,timestamp,itemId,recommended) values(?,?,?,?)";
        DROP_CLICK_TABLE_SQL = "drop TABLE click_table";

        INDEX_CLICK_TABLE_SQL1 = "CREATE INDEX click_userid  on click_table (userid)";
        INDEX_CLICK_TABLE_SQL2 = "CREATE INDEX click_timestamp on click_table (timestamp)";
        INDEX_CLICK_TABLE_SQL3 = "CREATE INDEX click_itemid on click_table (itemid)";
        INDEX_CLICK_TABLE_SQL4 = "CREATE INDEX click_session on click_table (userid,timestamp,itemid)";
        INDEX_CLICK_TABLE_SQL5 = "CREATE INDEX click_sessionid on click_table (sessionid)";

        DROP_INDEX_BUY_TABLE_SQL1 = "DROP INDEX click_userid";
        DROP_INDEX_BUY_TABLE_SQL2 = "DROP INDEX click_timestamp";
        DROP_INDEX_BUY_TABLE_SQL3 = "DROP INDEX click_itemid";
        DROP_INDEX_BUY_TABLE_SQL4 = "DROP INDEX click_session";
        DROP_INDEX_BUY_TABLE_SQL5 = "DROP INDEX click_sessionid";

        FIND_DUPLICATE_CLICK_TABLE_SQL =
                "select distinct c.USERID,c.timestamp,c.ITEMID,c.recommended "
                        + "from CLICK_TABLE c "
                        + "inner join ("
                        + "SELECT c0.USERID,c0.timestamp,c0.ITEMID,c0.recommended, COUNT(*) AS cnt "
                        + "FROM CLICK_TABLE c0 "
                        + "GROUP BY c0.USERID,c0.timestamp,c0.ITEMID,c0.recommended "
                        + "HAVING COUNT(*) > 1 "
                        + ") c2"
                        + " on (c.USERID = c2.USERID and c.timestamp =c2.timestamp and c.ITEMID=c2.ITEMID and c.recommended=c2.recommended)";

/*		FIND_DUPLICATE_CLICK_TABLE_SQL = 
				"select distinct c.USERID,c.timestamp,c.ITEMID,c.recommended "
				+ "from "+getActionType()+"_table c "
				+ "inner join ("
				+ "SELECT c0.USERID,c0.timestamp,c0.ITEMID,c0.recommended,COUNT(*) AS cnt "
				+ "FROM "+getActionType()+"_TABLE c0 "
				+ "GROUP BY c0.USERID,c0.timestamp,c0.ITEMID,c0.recommended"
				+ "HAVING COUNT(*) > 1 "
				+ ") c2"
				+ " on (c.USERID = c2.USERID and c.timestamp = c2.timestamp and c.ITEMID = c2.ITEMID  and c.recommended = c2.recommended)";
		*/
        DELETE_CLICK_TABLE_SQL = "delete from click_table where userid=? and timestamp=? and itemId=? and recommended=?";

    }

    @Override
    public ActionType getActionType()
    {
        return ActionType.click;
    }

    @Override
    public void createTable(Connection conn) throws SQLException
    {
        DBUtils.runDDLQuery(conn, CREATE_CLICK_TABLE_SQL);
    }

    @Override
    public void insert(Connection conn, Action a) throws SQLException
    {
        if (a.getActionType() != getActionType())
            return;
        ActionClick ba = (ActionClick) a;
        ba.setActionType(getActionType());
        PreparedStatement ps = conn.prepareStatement(INSERT_CLICK_SQL);
        actionClickToPS(ba, ps);
        ps.executeUpdate();
        ps.close();

    }

    private void actionClickToPS(ActionClick ba, PreparedStatement ps) throws SQLException
    {
        ps.setString(1, ba.getUserID());
        ps.setTimestamp(2, Timestamp.valueOf(ba.getTimestamp()));
        ps.setString(3, ba.getItemid());
        int recommended = 0;
        if (ba.getActionType() == ActionType.clickrecommended)
        {
            recommended = 1;
        }
        ps.setInt(4, recommended);

    }

    @Override
    public void dropTable(Connection conn) throws SQLException
    {
        // TODO Auto-generated method stub
        removeIndex(conn);
        runDLLWithTryAndCatch(conn, DROP_CLICK_TABLE_SQL);
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
        DBUtils.runDDLQuery(conn, INDEX_CLICK_TABLE_SQL1);
        DBUtils.runDDLQuery(conn, INDEX_CLICK_TABLE_SQL2);
        DBUtils.runDDLQuery(conn, INDEX_CLICK_TABLE_SQL3);
        DBUtils.runDDLQuery(conn, INDEX_CLICK_TABLE_SQL4);
        DBUtils.runDDLQuery(conn, INDEX_CLICK_TABLE_SQL5);
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
        PreparedStatement ps = conn.prepareStatement(INSERT_CLICK_SQL);
        for (Action an : actions)
        {

            ActionClick ba = (ActionClick) an;
            ba.setActionType(getActionType());
            actionClickToPS(ba, ps);
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
        PreparedStatement ps = conn.prepareStatement(DELETE_CLICK_TABLE_SQL);
        int count = 0;
        for (Action an : duplicate)
        {
            ActionClick ba = (ActionClick) an;
            actionClickToPS(ba, ps);
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
        PreparedStatement ps = conn.prepareStatement(FIND_DUPLICATE_CLICK_TABLE_SQL);
        ResultSet executeQuery = ps.executeQuery();
        while (executeQuery.next())
        {
            LocalDateTime ldt = executeQuery.getTimestamp(2).toLocalDateTime();
            ActionClick ab = new ActionClick(ldt, "461", executeQuery.getString(1), executeQuery.getString(3));
            if (executeQuery.getString(4).equals("1"))
            {
                ab.setActionType(ActionType.clickrecommended);
            }
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
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL1);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL2);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL3);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL4);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL5);
    }

    @Override
    public void removeIndexToLogins(Connection conn) throws SQLException
    {
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL1);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL3);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL4);
        runDLLWithTryAndCatch(conn, DROP_INDEX_BUY_TABLE_SQL5);
    }

    @Override
    public void resumeIndexToLogins(Connection conn) throws SQLException
    {
        DBUtils.runDDLQuery(conn, INDEX_CLICK_TABLE_SQL1);
        DBUtils.runDDLQuery(conn, INDEX_CLICK_TABLE_SQL3);
        DBUtils.runDDLQuery(conn, INDEX_CLICK_TABLE_SQL4);
        DBUtils.runDDLQuery(conn, INDEX_CLICK_TABLE_SQL5);

    }

    @Override
    public void createTempTable(Connection conn) throws SQLException
    {
        DBUtils.runDDLQuery(conn, CREATE_CLICK_TABLE_SQL.replace("click", "tempclick"));
    }

    @Override
    public void dropTempTable(Connection conn)
    {
        try
        {
            DBUtils.dropTable(conn, "tempclick_table");
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
