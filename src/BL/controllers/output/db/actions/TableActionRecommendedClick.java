package BL.controllers.output.db.actions;

import java.sql.Connection;
import java.sql.SQLException;

import BL.dataobjects.actions.Action.ActionType;

@Deprecated
public class TableActionRecommendedClick extends TableActionAbstractClick
{

    @Override
    public ActionType getActionType()
    {
        // TODO Auto-generated method stub
        return ActionType.clickrecommended;
    }


}
