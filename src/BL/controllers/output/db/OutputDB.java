package BL.controllers.output.db;

import java.sql.Connection;
import java.sql.SQLException;

import BL.controllers.output.IOutput;
import BL.controllers.output.db.actions.ITableAction;
import BL.dataobjects.actions.Action;
import DAL.utils.DBUtils;

public class OutputDB implements IOutput
{
    private String dbname;
    private String username;
    private String password;
    private String connectionString;
    private Connection connection;
    private ITableAction[] tables;
    private boolean firstBatch;

    public OutputDB(String connectionString, String dbname, String username, String password, ITableAction[] tables, boolean create)
    {
        super();
        this.dbname = dbname;
        this.username = username;
        this.password = password;
        this.connectionString = connectionString;
        this.tables = tables;
        firstBatch = true;
        if (create)
        {
            try
            {
                connect();
                dropAndCreateTables();
                close();
            } catch (SQLException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private void dropAndCreateTables()
    {
        for (int i = 0; i < tables.length; i++)
        {
            if (tables[i] != null)
            {
                try
                {
                    tables[i].dropTable(connection);
                } catch (SQLException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                try
                {
                    tables[i].createTable(connection);
                } catch (SQLException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
        }
    }

    public OutputDB(String connectionString, String dbname, String username, String password, ITableAction[] tables)
    {
        this(connectionString, dbname, username, password, tables, false);
    }

    protected void connect() throws SQLException
    {
        connection = DBUtils.getConnection(connectionString, dbname, username, password, true);
    }

    private void close() throws SQLException
    {
        connection.close();
        connection = null;
    }

    @Override
    public void writeAction(Action a) throws SQLException
    {
        connect();
        for (ITableAction iTableAction : tables)
        {
            if (iTableAction.getActionType() == a.getActionType())
            {
                iTableAction.insert(this.connection, a);
                return;
            }
        }
        close();
        // TODO Auto-generated method stub
    }

    @Override
    public void writeBatch(Action a) throws SQLException
    {
        // TODO Auto-generated method stub
        if (firstBatch)
        {
            firstBatch = false;
            connect();
        }
        for (ITableAction iTableAction : tables)
        {
            if (iTableAction.getActionType() == a.getActionType())
            {
                iTableAction.insertBatch(this.connection, a);
                return;
            }
        }

    }

    @Override
    public void finishBatch() throws SQLException
    {
        // TODO Auto-generated method stub
        for (ITableAction iTableAction : tables)
        {
            iTableAction.finishBatch(connection);
            iTableAction.createIndex(connection);
            iTableAction.removeDuplicate(connection);
        }
        close();
    }


}
