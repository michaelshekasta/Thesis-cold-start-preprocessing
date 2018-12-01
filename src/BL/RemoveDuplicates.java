package BL;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import BL.controllers.input.IInputActions;
import BL.controllers.input.raw.InputActionFromFile;
import BL.controllers.output.IOutput;
import BL.controllers.output.OutputCSV;
import BL.controllers.output.db.OutputDBLocalDerby;
import BL.controllers.output.db.actions.ITableAction;
import BL.controllers.output.db.actions.TableActionBasket;
import BL.controllers.output.db.actions.TableActionBuy;
import BL.controllers.output.db.actions.TableActionClick;
import BL.controllers.output.db.actions.TableActionRecommendedClick;
import BL.controllers.output.db.actions.TableActionTransfer;
import BL.controllers.parser.IActionParser;
import BL.controllers.parser.IParseActions;
import BL.controllers.parser.ParserBasic;
import BL.controllers.parser.action.raw.BuyActionParserFromRaw;
import BL.controllers.parser.action.raw.ClickActionParserFromRaw;
import BL.controllers.parser.action.raw.ClickRecommendedParserFromRaw;
import BL.controllers.parser.action.raw.TransferActionParser;
import BL.controllers.preprocessing.IYCReader;
import BL.controllers.preprocessing.YCReaderBatch;
import BL.dataobjects.actions.Action;
import DAL.utils.EmailUtils;
import DAL.utils.FileUtils;

public class RemoveDuplicates
{

    public static void runAll(Connection conn)
    {
        try
        {
            FileUtils.writeToFile("log.log", "starting");
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        removeDuplicates(conn);
//		try
//		{
//			writeLinesBuys();
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//		try
//		{
//			writeLinesClicks();
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//		try
//		{
//			writeLinesRecommendedClicks();
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//		try
//		{
//			writeLinesTransfer();
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//		
//		//send mails
//		
//		sendEmail("buy feb buys","buy_line_feb.txt");
//		sendEmail("buy mar buys","buy_line_mar.txt");
//			
//		sendEmail("buy feb clicks","click_line_feb.txt");
//		sendEmail("buy mar clicks","click_line_mar.txt");
//
//		sendEmail("buy feb recommended click","recommended_click_line_feb.txt");
//		sendEmail("buy mar recommended click","recommended_click_line_mar.txt");
//		
//		sendEmail("transfer feb recommended click","transfer_line_feb.txt");
//		sendEmail("transfer mar recommended click","transfer_line_mar.txt");
    }


    private static void writeLinesTransfer() throws Exception
    {
        Map<Action, List<Integer>> duplicateMap = loadActions("clickrecommended.txt");

        String feb = "C:\\Users\\Michael\\Documents\\לימודים\\תואר שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-feb\\461-tracking-feb.out";
        String mar = "C:\\Users\\Michael\\Documents\\לימודים\\תואר שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-mar\\461-tracking-mar.out";
        writeDuplicateTransfer(duplicateMap, feb, "transfer_line_feb.txt");
        writeDuplicateTransfer(duplicateMap, mar, "transfer_line_mar.txt");

    }

    private static void writeDuplicateTransfer(Map<Action, List<Integer>> duplicateMap, String month,
                                               String outputFile) throws IOException
    {
        String[] filenames = new String[]{month};

        // parsers
        IActionParser[] p1 = new IActionParser[]{new TransferActionParser()};
        IParseActions parser = new ParserBasic(p1);

        // input
        IInputActions input = new InputActionFromFile(filenames);
        int count = 0;
        for (String line : input)
        {
            count++;
            Action action = parser.getAction(line);
            if (action != null && duplicateMap.containsKey(action))
            {
                List<Integer> list = duplicateMap.get(action);
                list.add(count);
            }
        }
        FileUtils.writeToFile(outputFile, "", false);
        for (Entry<Action, List<Integer>> dupBuy : duplicateMap.entrySet())
        {
            List<Integer> value = dupBuy.getValue();
            if (value.size() == 0)
            {
                continue;
            }
            for (Integer integer : value)
            {
                FileUtils.writeToFile(outputFile, integer + " ", true);
            }
            FileUtils.writeToFile(outputFile, "\n", true);
        }
    }


    private static void sendEmail(String subject, String filepath)
    {
        try
        {
            String username = "shadars003@gmail.com";
            String password = "123456ABC";
            String toEmail = "shkasta@post.bgu.ac.il";
            EmailUtils.sendEmail(username, password, toEmail, subject, filepath, "automate email");
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private static void writeLinesRecommendedClicks() throws Exception
    {
        Map<Action, List<Integer>> duplicateMap = loadActions("clickrecommended.txt");

        String feb = "C:\\Users\\Michael\\Documents\\לימודים\\תואר שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-feb\\461-tracking-feb.out";
        String mar = "C:\\Users\\Michael\\Documents\\לימודים\\תואר שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-mar\\461-tracking-mar.out";
        writeDuplicateRecommendedClicks(duplicateMap, feb, "recommended_click_line_feb.txt");
        writeDuplicateRecommendedClicks(duplicateMap, mar, "recommended_click_line_mar.txt");

    }

    private static void writeDuplicateRecommendedClicks(Map<Action, List<Integer>> duplicateMap, String month,
                                                        String outputFile) throws IOException
    {
        String[] filenames = new String[]{month};

        // parsers
        IActionParser[] p1 = new IActionParser[]{new ClickRecommendedParserFromRaw()};
        IParseActions parser = new ParserBasic(p1);

        // input
        IInputActions input = new InputActionFromFile(filenames);
//		IInputActions input = new InputActionFromFileBounded(filenames,MAX_RECORDS);
        int count = 0;
        for (String line : input)
        {
            count++;
            Action action = parser.getAction(line);
            if (action != null && duplicateMap.containsKey(action))
            {
                List<Integer> list = duplicateMap.get(action);
                list.add(count);
            }
        }
        FileUtils.writeToFile(outputFile, "", false);
        for (Entry<Action, List<Integer>> dupBuy : duplicateMap.entrySet())
        {
            List<Integer> value = dupBuy.getValue();
            if (value.size() == 0)
            {
                continue;
            }
            for (Integer integer : value)
            {
                FileUtils.writeToFile(outputFile, integer + " ", true);
            }
            FileUtils.writeToFile(outputFile, "\n", true);
        }
    }

    private static void writeLinesClicks() throws Exception
    {
        Map<Action, List<Integer>> duplicateMap = loadActions("click.txt");

        String feb = "C:\\Users\\Michael\\Documents\\לימודים\\תואר שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-feb\\461-tracking-feb.out";
        String mar = "C:\\Users\\Michael\\Documents\\לימודים\\תואר שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-mar\\461-tracking-mar.out";
        writeDuplicateClicks(duplicateMap, feb, "click_line_feb.txt");
        writeDuplicateClicks(duplicateMap, mar, "click_line_mar.txt");

    }

    private static void writeDuplicateClicks(Map<Action, List<Integer>> duplicateMap, String month, String outputFile)
            throws IOException
    {
        String[] filenames = new String[]{month};

        // parsers
        IActionParser[] p1 = new IActionParser[]{new ClickActionParserFromRaw()};
        IParseActions parser = new ParserBasic(p1);

        // input
        IInputActions input = new InputActionFromFile(filenames);
//		IInputActions input = new InputActionFromFileBounded(filenames,MAX_RECORDS);
        int count = 0;
        for (String line : input)
        {
            count++;
            Action action = parser.getAction(line);
            if (action != null && duplicateMap.containsKey(action))
            {
                List<Integer> list = duplicateMap.get(action);
                list.add(count);
            }
        }
        FileUtils.writeToFile(outputFile, "", false);
        for (Entry<Action, List<Integer>> dupBuy : duplicateMap.entrySet())
        {
            List<Integer> value = dupBuy.getValue();
            if (value.size() == 0)
            {
                continue;
            }
            for (Integer integer : value)
            {
                FileUtils.writeToFile(outputFile, integer + " ", true);
            }
            FileUtils.writeToFile(outputFile, "\n", true);
        }
    }

    private static void writeLinesBuys() throws FileNotFoundException, IOException, ClassNotFoundException
    {
        String filename = "buys.txt";
        Map<Action, List<Integer>> duplicateList = loadActions(filename);

        String feb = "C:\\Users\\Michael\\Documents\\לימודים\\תואר שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-feb\\461-tracking-feb.out";
        String mar = "C:\\Users\\Michael\\Documents\\לימודים\\תואר שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-mar\\461-tracking-mar.out";
        writeBuysPerMonth(duplicateList, feb, "buy_line_feb.txt");
        writeBuysPerMonth(duplicateList, mar, "buy_line_mar.txt");
    }

    private static Map<Action, List<Integer>> loadActions(String filename)
            throws FileNotFoundException, IOException, ClassNotFoundException
    {
        Map<Action, List<Integer>> duplicateList = new HashMap<>();
        List<Action> lst;
        FileInputStream fis = new FileInputStream(filename);
        ObjectInputStream ois = new ObjectInputStream(fis);
        List<Action> duplicate = (List<Action>) ois.readObject();
        ois.close();
        for (Action action : duplicate)
        {
            duplicateList.put(action, new ArrayList<>());
        }
        return duplicateList;
    }

    private static void writeBuysPerMonth(Map<Action, List<Integer>> buy, String month, String outputName)
            throws IOException
    {
        String[] filenames = new String[]{month};

        // parsers
        IActionParser[] p1 = new IActionParser[]{new BuyActionParserFromRaw()};
        IParseActions parser = new ParserBasic(p1);

        // input
        IInputActions input = new InputActionFromFile(filenames);
//		IInputActions input = new InputActionFromFileBounded(filenames,MAX_RECORDS);
        int count = 0;
        for (String line : input)
        {
            count++;
            Action action = parser.getAction(line);
            if (action != null && buy.containsKey(action))
            {
                List<Integer> list = buy.get(action);
                list.add(count);
            }
        }
        FileUtils.writeToFile(outputName, "", false);
        for (Entry<Action, List<Integer>> dupBuy : buy.entrySet())
        {
            List<Integer> value = dupBuy.getValue();
            if (value.size() == 0)
            {
                continue;
            }
            for (Integer integer : value)
            {
                FileUtils.writeToFile(outputName, integer + " ", true);
            }
            FileUtils.writeToFile(outputName, "\n", true);
        }
    }

    private static void run2() throws IOException
    {
        // TODO Auto-generated method stub
        ITableAction[] t1 = new ITableAction[]{new TableActionBuy(), new TableActionClick(), new TableActionRecommendedClick()};
        IActionParser[] p1 = new IActionParser[]{new BuyActionParserFromRaw(), new ClickActionParserFromRaw(), new ClickRecommendedParserFromRaw()};
        run(t1, p1);

    }

    private static void run() throws IOException
    {
        ITableAction[] t1 = new ITableAction[]{new TableActionBuy()};
        IActionParser[] p1 = new IActionParser[]{new BuyActionParserFromRaw()};

        ITableAction[] t2 = new ITableAction[]{new TableActionClick()};
        IActionParser[] p2 = new IActionParser[]{new ClickActionParserFromRaw()};

        ITableAction[] t3 = new ITableAction[]{new TableActionRecommendedClick()};
        IActionParser[] p3 = new IActionParser[]{new ClickRecommendedParserFromRaw()};

        run(t1, p1);
        System.out.println("finish load buys");
        run(t2, p2);
        System.out.println("finish load clicks");
        run(t3, p3);
        System.out.println("finish load recommended clicks");
    }

    private static void run(ITableAction[] tables, IActionParser[] parsers) throws IOException
    {
        String feb = "C:\\Users\\Michael\\Documents\\לימודים\\תואר שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-feb\\461-tracking-feb.out";
        String mar = "C:\\Users\\Michael\\Documents\\לימודים\\תואר שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-mar\\461-tracking-mar.out";
        String[] filenames = new String[]{feb, mar};

        // input
        IInputActions input = new InputActionFromFile(filenames);
//		IInputActions input = new InputActionFromFileBounded(filenames, MAX_RECORDS);
        // parsers
        IParseActions parser = new ParserBasic(parsers);

        // output
        IOutput output = new OutputDBLocalDerby("yoochose", "user2", "user2", tables, true);

        // reader
        IYCReader reader = new YCReaderBatch(input, output, parser);
        reader.parse();
    }

    private static void removeDuplicates(Connection conn)
    {
        try
        {
            ITableAction a1 = new TableActionBuy();
            ITableAction a2 = new TableActionClick();
            ITableAction a4 = new TableActionTransfer();
            ITableAction a5 = new TableActionBasket();
            ITableAction[] arr = new ITableAction[]{a1, a2, a4};
//			ITableAction[] arr = new ITableAction[]{a2};
            try
            {
                for (ITableAction iTableAction : arr)
                {
                    iTableAction.createIndex(conn);
                }
            } catch (SQLException e)
            {
                e.printStackTrace();
            }
            try
            {
                FileUtils.writeToFile("log.log", "finish create indexing", true);
            } catch (IOException e)
            {
                e.printStackTrace();
            }
            for (ITableAction iTableAction : arr)
            {
                iTableAction.removeDuplicate(conn);
            }
        } catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    private static void fromFileToCSV() throws IOException
    {
        String feb = "C:\\Users\\Michael\\Documents\\לימודים\\תואר שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-feb\\461-tracking-feb.out";
        String mar = "C:\\Users\\Michael\\Documents\\לימודים\\תואר שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-mar\\461-tracking-mar.out";
        String[] filenames = new String[]{feb, mar};
        IInputActions input = new InputActionFromFile(filenames);

        IActionParser[] actionParsers = new IActionParser[]{new BuyActionParserFromRaw(), new ClickActionParserFromRaw(), new TransferActionParser(), new ClickRecommendedParserFromRaw()};
        IParseActions parser = new ParserBasic(actionParsers);

        IOutput output = new OutputCSV("buy.txt", "click.txt", "rclick.txt", "trans.txt");

        IYCReader reader = new YCReaderBatch(input, output, parser);
        reader.parse();
        EmailUtils.sendEmail("shadars003@gmail.com", "123456ABC", "shkasta@post.bgu.ac.il", "finish run", "m.txt", "awesome!!");
    }

    private static void removeIndexs(Connection conn)
    {
        try
        {
            ITableAction a1 = new TableActionBuy();
            ITableAction a2 = new TableActionClick();
            ITableAction a4 = new TableActionTransfer();
            ITableAction a5 = new TableActionBasket();
            ITableAction[] arr = new ITableAction[]{a1, a2, a4};
//			ITableAction[] arr = new ITableAction[]{a2};
            try
            {
                for (ITableAction iTableAction : arr)
                {
                    iTableAction.createIndex(conn);
                }
            } catch (SQLException e)
            {
                e.printStackTrace();
            }
            try
            {
                FileUtils.writeToFile("log.log", "finish create indexing", true);
            } catch (IOException e)
            {
                e.printStackTrace();
            }
            for (ITableAction iTableAction : arr)
            {
                iTableAction.removeDuplicate(conn);
            }
        } catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

}
