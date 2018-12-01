package BL.controllers.output;

import java.sql.SQLException;

import BL.dataobjects.actions.Action;

public class OutputScreen implements IOutput
{

    @Override
    public void writeAction(Action a)
    {
        System.out.println(a);

    }

    @Override
    public void writeBatch(Action a)
    {
        // TODO Auto-generated method stub
        System.out.println(a);
    }

    @Override
    public void finishBatch() throws SQLException
    {
        // TODO Auto-generated method stub

    }

}
