package BL.controllers.parser;

import java.util.HashSet;
import java.util.Set;

import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.Action.ActionType;
import DAL.utils.FileUtils;

public class ParserNoDup implements IParseActions
{

    private static final String BACKSLUSH = "/";
    IActionParser[] actionParsers;
    private static final String EMPTY_STRING = "";
    private static final int MAX_SIZE_HASH = 1000000;
    private Set<Integer>[] checkDup;
    private int count;
    private int line_count;


    public ParserNoDup(IActionParser[] actionParsers)
    {
        super();
        this.actionParsers = actionParsers;
        checkDup = new Set[actionParsers.length];
        for (int i = 0; i < actionParsers.length; i++)
        {
            checkDup[i] = new HashSet<Integer>();
        }
        count = 0;
        line_count = 0;
    }

    @Override
    public Action getAction(String line)
    {
        try
        {
            line_count++;
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
            for (int i = 0; i < actionParsers.length; i++)
            {
                IActionParser iActionParser = actionParsers[i];
                if (iActionParser != null && checkType(eventTypeStr, iActionParser))
                {
                    int hashCode = line.hashCode();
                    if (checkDup[i].contains(hashCode))
                    {
                        FileUtils.writeToFile("dup.txt", line + "\n", true);
                        FileUtils.writeToFile("dup_rows.txt", line_count + "\n", true);
                        return null;
                    }
                    checkDup[i].add(hashCode);
                    count++;
                    if (count > MAX_SIZE_HASH)
                    {
                        count = 0;
                        resetCheckDup();
                    }
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

    private void resetCheckDup()
    {
        for (Set<Integer> set : checkDup)
        {
            set.clear();
        }
    }
}
