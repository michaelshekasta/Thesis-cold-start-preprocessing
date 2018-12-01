package BL.controllers.parser;

import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.Action.ActionType;

public class ParserBasic implements IParseActions
{

    private static final String BACKSLUSH = "/";
    IActionParser[] actionParsers;
    private static final String EMPTY_STRING = "";

    public ParserBasic(IActionParser[] actionParsers)
    {
        super();
        this.actionParsers = actionParsers;
    }

    @Override
    public Action getAction(String line)
    {
        try
        {
            String clearLine = line.replace("[", EMPTY_STRING);
            clearLine = clearLine.replace("]", EMPTY_STRING);
            String[] splitTimestamp = clearLine.split(";|,");
            String[] splitItemBig = splitTimestamp[1].trim().split(BACKSLUSH);
            if (!isYCUser(splitItemBig[2]))
            {
                return null;
            }
            String eventTypeStr = splitItemBig[3];
            if (!hasLetters(eventTypeStr)) //check valid action
            {
                return null;
            }
            if (eventTypeStr.equals("buy"))
            {
                int x = 3;
                x--;
            }
            for (IActionParser iActionParser : actionParsers)
            {
                if (iActionParser != null && checkType(eventTypeStr, iActionParser))
                {
                    return iActionParser.getAction(line);
                }
            }
        } catch (Exception e)
        {
            e.printStackTrace();
            System.out.println(line);
        }
        return null;
    }

    private boolean hasLetters(String eventTypeStr)
    {
        return eventTypeStr.matches(".*[a-zA-Z]+.*");
    }

    private boolean isYCUser(String customerId)
    {
        return customerId.equals("461");
    }

    private boolean checkType(String eventTypeStr, IActionParser iActionParser)
    {
        return ActionType.valueOf(eventTypeStr.toLowerCase()) == iActionParser.getTypeAction();
    }

    @Override
    public ActionType[] getTypeActions()
    {
        ActionType[] ans = new ActionType[this.actionParsers.length];
        for (int i = 0; i < this.actionParsers.length; i++)
        {
            ans[i] = this.actionParsers[i].getTypeAction();
        }
        return ans;
    }

}
