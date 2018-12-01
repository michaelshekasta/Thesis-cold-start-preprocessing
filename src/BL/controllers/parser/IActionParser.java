package BL.controllers.parser;

import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.Action.ActionType;

public interface IActionParser
{
    Action getAction(String line);

    ActionType getTypeAction();
}
