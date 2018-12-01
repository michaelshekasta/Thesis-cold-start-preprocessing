package DAL;

import java.io.IOException;

import DAL.utils.FileUtils;

public class Parser
{
    // CONSTANTS
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

    // MEMBERS
    private String errorFilePath;
    private String inputFilePath;
    private String outputFilePath;

    public Parser(String errorFilePath, String inputFilePath, String outputFilePath)
    {
        super();
        this.errorFilePath = errorFilePath;
        this.inputFilePath = inputFilePath;
        this.outputFilePath = outputFilePath;
    }

    public void praseAndWrite() throws IOException
    {
        try
        {
            FileUtils.writeToFile(errorFilePath, EMPTY_STRING);
            FileUtils.writeToFile(outputFilePath, EMPTY_STRING);
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        CSVReader reader = new CSVReader(inputFilePath);
        FileUtils.writeToFile(outputFilePath, TITLE + NEWLINE);
        for (String line : reader)
        {
            try
            {
                parse(outputFilePath, line);
            } catch (Exception ex)
            {
                FileUtils.writeToFile(errorFilePath, line + NEWLINE, true);
                System.err.println("error :" + line);
            }
        }
    }

    private void parse(String outputFile, String line) throws IOException
    {
        {
            parse(outputFile, line, true);
        }
    }

    private void parse(String outputFile, String line, boolean toFile) throws IOException
    {
        // remove []
        String clearLine = line.replace("[", EMPTY_STRING);
        clearLine = clearLine.replace("]", EMPTY_STRING);
        String[] splitTimestamp = clearLine.split(";|,");
        String timestampStr = splitTimestamp[0].trim();
        // LocalDateTime ldt = Action.getDateFromString(timestampStr);
        String[] splitItemBig = splitTimestamp[1].trim().split("/");
        // String customerLabelStr = splitItemBig[0];
        String customerIdStr = splitItemBig[2];
        String eventTypeStr = splitItemBig[3];
        // check which event
        if (eventTypeStr.equals(BUY))
        {
            handleBuyEvent(outputFile, toFile, timestampStr, splitItemBig, customerIdStr);
        }
        if (eventTypeStr.equals(CLICK) || eventTypeStr.equals(CLICKRECOMMENED))
        {
            handleClickEvent(outputFile, toFile, timestampStr, splitItemBig, customerIdStr);
        }
        if (eventTypeStr.equals(TRANSFER))
        {
            handleTransferEvent(outputFile, toFile, timestampStr, splitItemBig, customerIdStr);
        }
    }

    private void handleTransferEvent(String outputFile, boolean toFile, String timestampStr, String[] splitItemBig,
                                     String customerIdStr) throws IOException
    {
        // TODO Auto-generated method stub
        String userIdStr = splitItemBig[4];
        String userIdStr2 = splitItemBig[5];
        if (toFile && customerIdStr.equals(YOOCHOOSE_USERID))
        {
            String tuple = String.format("%s,%s,%s,%s,%s,%s,%s", customerIdStr, userIdStr, timestampStr, userIdStr2, "",
                    "", "TRANSFER");
            FileUtils.writeToFile(outputFile, tuple + NEWLINE, true);
        }

    }

    private void handleClickEvent(String outputFile, boolean toFile, String timestampStr, String[] splitItemBig,
                                  String customerIdStr) throws IOException
    {
        // TODO Auto-generated method stub
        String userIdStr = splitItemBig[4];
        // String qtyStr = splitItemBig[5];
        String itemDetailsStr = splitItemBig[6];
        String[] splitItem = itemDetailsStr.split(QUESTION_MARK);
        String ItemId = splitItem[0];
        if (toFile && customerIdStr.equals(YOOCHOOSE_USERID))
        {
            String tuple = String.format("%s,%s,%s,%s,%s,%s,%s", customerIdStr, userIdStr, timestampStr, ItemId, "", "",
                    "CLICK");
            FileUtils.writeToFile(outputFile, tuple + NEWLINE, true);
        }

    }

    private void handleBuyEvent(String outputFile, boolean toFile, String timestampStr, String[] splitItemBig,
                                String customerIdStr) throws IOException
    {
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
        if (indexEUR != -1)
        {
            String priceOnly = price.substring(0, indexEUR);
            if (toFile && customerIdStr.equals(YOOCHOOSE_USERID))
            {
                String tuple = String.format("%s,%s,%s,%s,%s,%s,%s", customerIdStr, userIdStr, timestampStr, ItemId,
                        quantity, priceOnly, "BUY");
                FileUtils.writeToFile(outputFile, tuple + NEWLINE, true);
            }
        }
    }

    private void printLines(String line, String... str)
    {
        for (String string : str)
        {
            printLine(line, string);
        }
    }

    private void printLine(String line, String str)
    {
        if (line.indexOf(str) != -1)
        {
            System.out.println(line);
        }
    }

	/**/

}

/*
 * String[] firstCut = line.split(";"); String timeStampStr =
 * firstCut[0].substring(1); LocalDateTime timestamp =
 * LocalDateTime.parse(timeStampStr,Action.patternDate); String[] secondCut =
 * firstCut[1].split("\\?"); StringTokenizer stringTokenizer = new
 * StringTokenizer(secondCut[0].trim(),"/"); String
 * customerLabel=stringTokenizer.nextToken();
 * if(customerLabel.equals("robots.txt")) { continue; } String
 * customerId=stringTokenizer.nextToken(); Action.ActionType
 * actionType=Action.ActionType.valueOf(stringTokenizer.nextToken());
 * if(actionType == Action.ActionType.buy) { String thirdCut = secondCut[1]; int
 * quantityLoc = thirdCut.indexOf("quantity"); int nextAps =
 * thirdCut.indexOf("&",quantityLoc); int nextEq =
 * thirdCut.indexOf("=",quantityLoc); int quantity =
 * Integer.parseInt(thirdCut.substring(nextEq+1,nextAps)); int priceLoc =
 * thirdCut.indexOf("quantity"); nextAps = thirdCut.indexOf("&",priceLoc);
 * nextEq = thirdCut.indexOf("=",priceLoc); int
 * price=Integer.parseInt(thirdCut.substring(nextEq+1,nextAps)); ActionBuy
 * actionBuy = new ActionBuy(timestamp, customerLabel, customerId, actionType,
 * price , quantity); FileUtils.writeToFile(outputFile , actionBuy.toCsv()+"\n",
 * true); } //if(line.indexOf("buy") != -1) //{ //
 * FileUtils.writeToFile(outputFile, line+"\n", true); //} ------
 * 
 * // printLine(line,"02/Feb/2016:23:38:22"); //
 * printLine(line,"02/Feb/2016:23:38:22"); //
 * printLine(line,"04/Feb/2016:18:51:35"); //
 * printLine(line,"04/Feb/2016:19:07:16"); //
 * printLine(line,"04/Feb/2016:19:38:27"); //
 * printLine(line,"04/Feb/2016:19:38:28"); //
 * printLine(line,"04/Feb/2016:19:39:14"); //
 * printLine(line,"04/Feb/2016:19:07:16"); //
 * printLine(line,"04/Feb/2016:19:38:27"); //
 * printLine(line,"04/Feb/2016:19:38:27"); //
 * printLine(line,"04/Feb/2016:19:38:28"); //
 * printLine(line,"04/Feb/2016:18:51:35"); //
 * printLine(line,"04/Feb/2016:19:07:16"); //
 * printLine(line,"04/Feb/2016:19:38:27"); //
 * printLine(line,"04/Feb/2016:19:38:27"); //
 * printLine(line,"04/Feb/2016:19:38:27"); //
 * printLine(line,"06/Feb/2016:13:16:53"); //
 * printLine(line,"18/Feb/2016:18:20:23"); //
 * printLine(line,"24/Feb/2016:13:08:18");
 */

/*
 * private String parseBuyAction(String line) { String clearLine =
 * line.replace("[", EMPTY_STRING); clearLine = clearLine.replace("]",
 * EMPTY_STRING); String[] splitTimestamp = clearLine.split(";|,"); String
 * timestampStr = splitTimestamp[0].trim(); LocalDateTime ldt =
 * Action.getDateFromString(timestampStr); StringTokenizer stk = new
 * StringTokenizer(splitTimestamp[1].trim(), "/"); String customerLabelStr =
 * stk.nextToken(); String customerIdStr = stk.nextToken(); String eventTypeStr
 * = stk.nextToken(); String userIdStr = stk.nextToken(); String qtyStr =
 * stk.nextToken(); String itemDetailsStr = stk.nextToken(); String[] splitItem
 * = itemDetailsStr.split(QUESTION_MARK); String ItemId = splitItem[0]; String
 * temp = splitItem[1]; String itemParameters = temp; String[] splitItemParams =
 * itemParameters.split(AMPERCENT_STRING); String quantity = EMPTY_STRING;
 * String currency = EMPTY_STRING; String price = EMPTY_STRING; int i = 0; for
 * (String parms : splitItemParams) { String[] keyValue =
 * parms.split(EQUAL_MARK); if (keyValue[0].toLowerCase().equals(PRICE_STRING)
 * || keyValue[0].toLowerCase().equals("fullprice")) { price =
 * keyValue[1].replace(DOTCOMMENT_STRING, EMPTY_STRING); } if
 * (keyValue[0].toLowerCase().equals(QUANTITY_STRING)) { quantity =
 * keyValue[1].replace(DOTCOMMENT_STRING, EMPTY_STRING); } if
 * (keyValue[0].toLowerCase().equals(CURRENCY_STRING)) { currency =
 * keyValue[1].replace(DOTCOMMENT_STRING, EMPTY_STRING); } } price = price +
 * currency; String tuple = String.format("%s,%s,%s,%s,%s", userIdStr,
 * timestampStr, ItemId, quantity, price); return tuple; }
 */