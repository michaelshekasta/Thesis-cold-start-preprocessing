package BL.controllers.parser.action.raw;

import java.time.LocalDateTime;

import BL.controllers.parser.IActionParser;
import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.Action.ActionType;
import BL.dataobjects.actions.ActionBasket;
import DAL.utils.DateUtils;

public class BasketActionParserFromRaw implements IActionParser
{

    private static final String EMPTY_STRING = "";
    private static final String QUESTION_MARK = "\\?";

    @Override
    public Action getAction(String line)
    {
        String clearLine = line.replace(" +0000];", EMPTY_STRING);
        clearLine = line.replace("[", EMPTY_STRING);
        clearLine = clearLine.replace("]", EMPTY_STRING);
        String[] splitTimestamp = clearLine.split(";|,");
        String timestampStr = splitTimestamp[0].trim();
        // LocalDateTime ldt = Action.getDateFromString(timestampStr);
        String[] splitItemBig = splitTimestamp[1].trim().split("/");
        // String customerLabelStr = splitItemBig[0];
        String customerIdStr = splitItemBig[2];
        String userIdStr = splitItemBig[4];
        // String qtyStr = splitItemBig[5];
        String itemDetailsStr = splitItemBig[6];
        String[] splitItem = itemDetailsStr.split(QUESTION_MARK);
        String ItemId = splitItem[0];
        LocalDateTime timestamp = DateUtils.getDateTimeFromString(timestampStr);
        Action a = new ActionBasket(timestamp, customerIdStr, userIdStr, ItemId);
        return a;
    }


    @Override
    public ActionType getTypeAction()
    {
        // TODO Auto-generated method stub
        return ActionType.basket;
    }

}
