package BL.controllers.output.db.actions;

import java.sql.Connection;
import java.sql.SQLException;

import BL.dataobjects.actions.Action.ActionType;

public class TableActionClick extends TableActionAbstractClick
{

    @Override
    public ActionType getActionType()
    {
        // TODO Auto-generated method stub
        return ActionType.click;
    }


}
