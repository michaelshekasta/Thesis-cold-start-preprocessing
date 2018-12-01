package BL.controllers.parser.action.raw;

import java.time.LocalDateTime;

import BL.controllers.parser.IActionParser;
import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.ActionClick;
import BL.dataobjects.actions.Action.ActionType;
import DAL.utils.DateUtils;

public class ClickActionParserFromRaw implements IActionParser
{
    private static final String DOTCOMMENT_STRING = ";";
    private static final String EMPTY_STRING = "";
    private static final String AMPERCENT_STRING = "&";
    private static final String EQUAL_MARK = "=";
    private static final String YOOCHOOSE_USERID = "461";
    private static final String EUR_STRING = "eur";
    private static final String CURRENCY_STRING = "currency";
    private static final String QUANTITY_STRING = "quantity";
    private static final String PRICE_STRING = "price";
    private static final String QUESTION_MARK = "\\?";
    private static final String BUY = "buy";
    private static final String NEWLINE = "\n";
    private static final String TITLE = "YooChoseCustomerId,userId,timestamp,ItemId,Qty,price";
    private static final Object CLICK = "click";
    private static final Object TRANSFER = "transfer";
    private static final Object CLICKRECOMMENED = "clickrecommended";

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
        String eventTypeStr = splitItemBig[3];
        String userIdStr = splitItemBig[4];
        // String qtyStr = splitItemBig[5];
        String itemDetailsStr = splitItemBig[6];
        String[] splitItem = itemDetailsStr.split(QUESTION_MARK);
        String ItemId = splitItem[0];
        LocalDateTime timestamp = DateUtils.getDateTimeFromString(timestampStr);
        Action a = new ActionClick(timestamp, customerIdStr, userIdStr, ItemId);
        return a;
    }

    @Override
    public ActionType getTypeAction()
    {
        // TODO Auto-generated method stub
        return ActionType.click;
    }

}
