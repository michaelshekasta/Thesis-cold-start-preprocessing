package BL.controllers.output;

import java.sql.SQLException;

import BL.dataobjects.actions.Action;

public interface IOutput
{

    void writeAction(Action a) throws SQLException;

    public void writeBatch(Action a) throws SQLException;

    public void finishBatch() throws SQLException;
}
