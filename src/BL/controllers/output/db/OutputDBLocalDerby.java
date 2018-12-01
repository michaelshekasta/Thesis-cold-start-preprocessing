package BL.controllers.output.db;

import java.sql.SQLException;

import BL.controllers.output.IOutput;
import BL.controllers.output.db.actions.ITableAction;
import BL.dataobjects.actions.Action;

public class OutputDBLocalDerby implements IOutput
{

    private OutputDB db;

    public OutputDBLocalDerby(String dbname, String username, String password, ITableAction[] tables, boolean create)
    {
        super();
        db = new OutputDB("jdbc:derby://localhost:1527/", dbname, username, password, tables, create);
    }

    public OutputDBLocalDerby(String dbname, String username, String password, ITableAction[] tables)
    {
        this(dbname, username, password, tables, false);
    }


    @Override
    public void writeAction(Action a) throws SQLException
    {
        db.writeAction(a);
        // TODO Auto-generated method stub

    }

    @Override
    public void writeBatch(Action a) throws SQLException
    {
        db.writeBatch(a);
        // TODO Auto-generated method stub
    }

    @Override
    public void finishBatch() throws SQLException
    {
        // TODO Auto-generated method stub
        db.finishBatch();
    }

}
