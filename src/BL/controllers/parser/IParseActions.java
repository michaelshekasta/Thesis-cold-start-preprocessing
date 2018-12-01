package BL.controllers.parser;

import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.Action.ActionType;

public interface IParseActions
{

    Action getAction(String line);

    ActionType[] getTypeActions();

}
