package BL.controllers.parser.action.raw;

import java.time.LocalDateTime;

import BL.controllers.parser.IActionParser;
import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.Action.ActionType;
import DAL.utils.DateUtils;
import BL.dataobjects.actions.ActionBuy;

public class BuyActionParserFromRaw implements IActionParser
{
    private static final String DOTCOMMENT_STRING = ";";
    private static final String EMPTY_STRING = "";
    private static final String AMPERCENT_STRING = "&";
    private static final String EQUAL_MARK = "=";
    private static final String EUR_STRING = "eur";
    private static final String CURRENCY_STRING = "currency";
    private static final String QUANTITY_STRING = "quantity";
    private static final String PRICE_STRING = "price";
    private static final String QUESTION_MARK = "\\?";

    @Override
    public Action getAction(String line)
    {
        String clearLine = line.replace("[", EMPTY_STRING);
        clearLine = clearLine.replace("]", EMPTY_STRING);
        String[] splitTimestamp = clearLine.split(";|,");
        String timestampStr = splitTimestamp[0].trim();
        // LocalDateTime ldt = Action.getDateFromString(timestampStr);
        String[] splitItemBig = splitTimestamp[1].trim().split("/");
        // String customerLabelStr = splitItemBig[0];
        String customerIdStr = splitItemBig[2];
        // check which event
        String userIdStr = splitItemBig[4];
        // String qtyStr = splitItemBig[5];
        String itemDetailsStr = splitItemBig[6];
        String[] splitItem = itemDetailsStr.split(QUESTION_MARK);
        String ItemId = splitItem[0];
        String temp = splitItem[1];
        String itemParameters = temp;
        String[] splitItemParams = itemParameters.split(AMPERCENT_STRING);
        String quantity = EMPTY_STRING;
        String currency = EMPTY_STRING;
        String price = EMPTY_STRING;
        for (String parms : splitItemParams)
        {
            String[] keyValue = parms.split(EQUAL_MARK);
            if (keyValue[0].toLowerCase().equals(PRICE_STRING) || keyValue[0].toLowerCase().equals("fullprice"))
            {
                price = keyValue[1].replace(DOTCOMMENT_STRING, EMPTY_STRING);
            }
            if (keyValue[0].toLowerCase().equals(QUANTITY_STRING))
            {
                quantity = keyValue[1].replace(DOTCOMMENT_STRING, EMPTY_STRING);
            }
            if (keyValue[0].toLowerCase().equals(CURRENCY_STRING))
            {
                currency = keyValue[1].replace(DOTCOMMENT_STRING, EMPTY_STRING);
            }
        }
        price = price + currency;

        int indexEUR = price.toLowerCase().indexOf(EUR_STRING);
        String priceOnly = price;
        if (indexEUR != -1)
        {
            priceOnly = price.substring(0, indexEUR);
        }
        LocalDateTime timestamp = DateUtils.getDateTimeFromString(timestampStr);
        Action action = new ActionBuy(timestamp, customerIdStr, userIdStr, ItemId, Double.parseDouble(priceOnly),
                Integer.parseInt(quantity));
        return action;
    }

    @Override
    public ActionType getTypeAction()
    {
        // TODO Auto-generated method stub
        return ActionType.buy;
    }

}
