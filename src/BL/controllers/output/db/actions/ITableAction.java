package BL.controllers.output.db.actions;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.Action.ActionType;

public interface ITableAction
{
    public ActionType getActionType();

    public void createTable(Connection conn) throws SQLException;

    public void createTempTable(Connection conn) throws SQLException;

    void dropTable(Connection conn) throws SQLException;

    void insert(Connection conn, Action a) throws SQLException;

    public void createIndex(Connection conn) throws SQLException;

    public void removeIndex(Connection conn) throws SQLException;

    public void removeIndexToLogins(Connection conn) throws SQLException;

    public void resumeIndexToLogins(Connection conn) throws SQLException;

    public void insertBatch(Connection conn, Action a) throws SQLException;

    public void finishBatch(Connection conn) throws SQLException;

    public void removeDuplicate(Connection conn) throws SQLException;

    public static Action[] filterBuyActions(Action[] a, ActionType at)
    {
        List<Action> ans = new ArrayList<Action>();
        for (Action action : a)
        {
            if (action.getActionType() == at)
            {
                ans.add(action);
            }
        }
        return ans.toArray(a);
    }

    public void dropTempTable(Connection conn);


}
