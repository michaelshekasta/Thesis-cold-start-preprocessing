package BL.controllers.parser.action.raw;

import java.time.LocalDateTime;

import BL.controllers.parser.IActionParser;
import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.ActionTransfer;
import BL.dataobjects.actions.Action.ActionType;
import DAL.utils.DateUtils;

public class TransferActionParser implements IActionParser
{

    private static final String EMPTY_STRING = "";

    @Override
    public Action getAction(String line)
    {
        String clearLine = line.replace("[", EMPTY_STRING);
        clearLine = clearLine.replace("]", EMPTY_STRING);
        String[] splitTimestamp = clearLine.split(";|,");
        String timestampStr = splitTimestamp[0].trim();
        LocalDateTime ldt = DateUtils.getDateTimeFromString(timestampStr);
        String[] splitItemBig = splitTimestamp[1].trim().split("/");
        String customerIdStr = splitItemBig[2];
        String eventTypeStr = splitItemBig[3];
        String userIdStr = splitItemBig[4];
        String userIdStr2 = splitItemBig[5];
        String tuple = String.format("%s,%s,%s,%s,%s,%s,%s", customerIdStr, userIdStr, timestampStr, userIdStr2, "", "",
                "TRANSFER");
        Action a = new ActionTransfer(ldt, customerIdStr, userIdStr, userIdStr2);
        return a;
    }

    @Override
    public ActionType getTypeAction()
    {
        return ActionType.transfer;
    }

}
