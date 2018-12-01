package BL.controllers.parser.action.raw;

import BL.controllers.parser.IActionParser;
import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.Action.ActionType;
import BL.dataobjects.actions.ActionClick;

public class ClickRecommendedParserFromRaw implements IActionParser
{
    ClickActionParserFromRaw car = new ClickActionParserFromRaw();

    @Override
    public Action getAction(String line)
    {
        // TODO Auto-generated method stub
        ActionClick a = (ActionClick) car.getAction(line);
        a.setActionType(ActionType.clickrecommended);
        return a;
    }

    @Override
    public ActionType getTypeAction()
    {
        // TODO Auto-generated method stub
        return ActionType.clickrecommended;
    }

}
