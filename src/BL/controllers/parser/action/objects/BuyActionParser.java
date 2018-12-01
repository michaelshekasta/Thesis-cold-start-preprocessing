package BL.controllers.parser.action.objects;

import java.time.LocalDateTime;

import BL.controllers.parser.IActionParser;
import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.Action.ActionType;
import BL.dataobjects.actions.ActionBuy;

@Deprecated
public class BuyActionParser implements IActionParser
{

    @Override
    public Action getAction(String line)
    {
        int whitespaceIndex = line.indexOf(" ");
        String action = line.substring(0, whitespaceIndex);
        if (!action.equals("Action" + this.getTypeAction()))
        {
            return null;
        }

        String onlyargs = line.substring(line.indexOf("[") + 1, line.indexOf("]"));
        String[] arguments = onlyargs.split(",");
        String itemid = "";
        double price = 0;
        int quantity = 0;
        LocalDateTime ldt = LocalDateTime.now();
        String customerId = "461";
        String userId = "";
        for (String field : arguments)
        {
            String key = field.substring(0, line.indexOf("=")).trim();
            String value = field.substring(line.indexOf("=")).trim();
            if (key.equals("userId"))
            {
                userId = value;
            }
            if (key.equals("customerId"))
            {
                customerId = value;
            }
            if (key.equals("itemid"))
            {
                itemid = value;
            }
            if (key.equals("price"))
            {
                price = Double.parseDouble(value);
            }
            if (key.equals("quantity"))
            {
                quantity = Integer.parseInt(value);
            }
            if (key.equals("timestamp"))
            {
                ldt = LocalDateTime.parse(value);
            }

        }
        ActionBuy ab = new ActionBuy(ldt, customerId, userId, itemid, price, quantity);
        return ab;
    }

    @Override
    public ActionType getTypeAction()
    {
        // TODO Auto-generated method stub
        return ActionType.buy;
    }

}
