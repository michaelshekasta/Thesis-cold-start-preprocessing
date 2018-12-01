package BL.controllers;

import BL.PredictionsToFiles;
import BL.RemoveDuplicates;
import BL.controllers.flow.CreateSquenceDataSession;
import BL.controllers.flow.UpdateSessionState;
import BL.controllers.flow.UpdateSessions;
import BL.controllers.input.IInputActions;
import BL.controllers.input.raw.InputActionFromFile;
import BL.controllers.output.IOutput;
import BL.controllers.output.OutputCSV;
import BL.controllers.output.db.actions.ITableAction;
import BL.controllers.output.db.actions.TableActionBuy;
import BL.controllers.output.db.actions.TableActionClick;
import BL.controllers.output.db.actions.TableActionTransfer;
import BL.controllers.parser.IActionParser;
import BL.controllers.parser.IParseActions;
import BL.controllers.parser.ParserBasic;
import BL.controllers.parser.action.raw.*;
import BL.controllers.preprocessing.IYCReader;
import BL.controllers.preprocessing.YCReaderBatch;
import DAL.utils.DBUtils;
import DAL.utils.DateUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

public class CreateDataToExpirement
{
    private static final String SQL_CREATE_SESSION_TABLE = "CREATE TABLE session_table ( dayofsession date,userid varchar(255), sessionid int,clicks int, buy int)";

    public static void run() throws IOException, ClassNotFoundException, SQLException, Exception
    {
//        Connection conn = DBUtils.getConnectionLocalPostgre("yoochoseaug", "yoochose", "1234");
//        System.out.println("Starting wish me luck....");
//        createDataFromFile();
//        System.out.println("done creating files");
//        Connection conn = null;
//        Connection conn = DBUtils.getConnectionRemotePostgre("localhost","128","yoochoseaug", "yoochose", "1234");
//        createTables(conn);
//        System.out.println("done creating tables");
//        insertCSVToDB(conn);
//        System.out.println("done insert csv to db");
//        removeDuplicate(conn);
//        System.out.println("done remove dup");
//        removeNull(conn);
//        System.out.println("done change null to unknown");
//        UpdateWithLogins.run(conn);
//        fixInput(conn);
//        System.out.println("done update logins");
//        System.out.println("done fix all long itemids");
//        updateSession(conn);
//        System.out.println("done update sessions");
//        createSquenceDataSession(conn);
//        System.out.println("done create sequence session");
//        updateSessionState(conn);
//        System.out.println("done update session stat");
//        printSessionFile(conn, "list session.csv", "0", "44553440");
//        printSessionFile(conn, "list session.csv", "44553440", "90000000", true);
//        System.out.println("done create session file");
//        createStat(conn);
//        System.out.println("done createStat");
//        UpdateSessionAvgClicks.run(conn);
//        System.out.println("done update session table stats");
//        addHourAndDay(conn);
//        insertCatalog(conn,"user.dir");
//        insertCatalog(conn, "/home/ise/Desktop/augData/catalog");
//        insertCatalog(conn, "C:\\Users\\Michael\\Documents\\study\\Thesis\\1 - data\\Yoochose Data\\content_data_export_2016-08-19_22-01-00-026\\");
//        insertCatalog(conn, "C:\\Users\\Michael\\Documents\\study\\Thesis\\1 - data\\Yoochose Data\\export-2016-04-04T22_00_01.000Z.csv\\");
//        DBUtils.writeQueryToCSV(conn,"Select clicks as length, count(*) as count from session_table group by clicks","2a.csv");
//        DBUtils.writeQueryToCSV(conn,"Select clicks as length, count(*) as count from session_table where buy >0 group by clicks","2b.csv");
//        DBUtils.writeQueryToCSV(conn,"Select clicks as length, count(*) as count from session_table where buy = 0 group by clicks","2c.csv");
//        DBUtils.writeQueryToCSV(conn,"select category1,count(*) cnt from catalog_table as ct join buy_table as b on (ct.itemid = b.itemid) group by category1 order by cnt desc","3c1.csv");
//        DBUtils.writeQueryToCSV(conn,"select category2,count(*) cnt from catalog_table as ct join buy_table as b on (ct.itemid = b.itemid) group by category2 order by cnt desc","3c2.csv");
//        DBUtils.writeQueryToCSV(conn,"select category3,count(*) cnt from catalog_table as ct join buy_table as b on (ct.itemid = b.itemid) group by category3 order by cnt desc","3c3.csv");
//        DBUtils.writeQueryToCSV(conn,"select lastcategory,count(*) cnt from catalog_table as ct join buy_table as b on (ct.itemid = b.itemid) group by lastcategory order by cnt desc","3c4.csv");
//        DBUtils.writeQueryToCSV(conn,"select ss.clicks, count(*) as cnt FROM session_table as ss join ( SELECT DISTINCT sessionid FROM session_table EXCEPT SELECT DISTINCT sessionid FROM ( SELECT DISTINCT sessionid FROM ((SELECT sessionid, itemid FROM click_table) AS T1 JOIN (SELECT itemid FROM (SELECT DISTINCT itemid FROM catalog_table WHERE htmlcategory IS NULL EXCEPT SELECT DISTINCT itemid FROM catalog_table WHERE htmlcategory IS NOT NULL) AS T22) AS T2 ON (T1.itemid = T2.itemid)) AS T5 UNION SELECT DISTINCT sessionid FROM ((SELECT sessionid, itemid FROM buy_table) AS T1 JOIN (SELECT itemid FROM (SELECT DISTINCT itemid FROM catalog_table WHERE htmlcategory IS NULL EXCEPT SELECT DISTINCT itemid FROM catalog_table WHERE htmlcategory IS NOT NULL) AS T22) AS T2 ON (T1.itemid = T2.itemid)) AS T52 ) T6 ) T7 on ss.sessionid = T7.sessionid group by clicks order by cnt desc","3c5.csv");
//        DBUtils.writeQueryToCSV(conn,"select ss.clicks, count(*) as cnt FROM session_table as ss join ( SELECT DISTINCT sessionid FROM session_table EXCEPT SELECT DISTINCT sessionid FROM ( SELECT DISTINCT sessionid FROM ((SELECT sessionid, itemid FROM click_table) AS T1 JOIN (SELECT itemid FROM (SELECT DISTINCT itemid FROM catalog_table WHERE htmlcategory IS NULL EXCEPT SELECT DISTINCT itemid FROM catalog_table WHERE htmlcategory IS NOT NULL) AS T22) AS T2 ON (T1.itemid = T2.itemid)) AS T5 UNION SELECT DISTINCT sessionid FROM ((SELECT sessionid, itemid FROM buy_table) AS T1 JOIN (SELECT itemid FROM (SELECT DISTINCT itemid FROM catalog_table WHERE htmlcategory IS NULL EXCEPT SELECT DISTINCT itemid FROM catalog_table WHERE htmlcategory IS NOT NULL) AS T22) AS T2 ON (T1.itemid = T2.itemid)) AS T52 ) T6 ) T7 on ss.sessionid = T7.sessionid where ss.buy > 0 group by clicks order by cnt desc","3c6.csv");
//        DBUtils.writeQueryToCSV(conn,"select ss.clicks, count(*) as cnt FROM session_table as ss join ( SELECT DISTINCT sessionid FROM session_table EXCEPT SELECT DISTINCT sessionid FROM ( SELECT DISTINCT sessionid FROM ((SELECT sessionid, itemid FROM click_table) AS T1 JOIN (SELECT itemid FROM (SELECT DISTINCT itemid FROM catalog_table WHERE htmlcategory IS NULL EXCEPT SELECT DISTINCT itemid FROM catalog_table WHERE htmlcategory IS NOT NULL) AS T22) AS T2 ON (T1.itemid = T2.itemid)) AS T5 UNION SELECT DISTINCT sessionid FROM ((SELECT sessionid, itemid FROM buy_table) AS T1 JOIN (SELECT itemid FROM (SELECT DISTINCT itemid FROM catalog_table WHERE htmlcategory IS NULL EXCEPT SELECT DISTINCT itemid FROM catalog_table WHERE htmlcategory IS NOT NULL) AS T22) AS T2 ON (T1.itemid = T2.itemid)) AS T52 ) T6 ) T7 on ss.sessionid = T7.sessionid where ss.buy = 0 group by clicks order by cnt desc","3c7.csv");
//        insertCatalog(conn, "C:\\Users\\Michael\\Documents\\study\\Thesis\\1 - data\\Yoochose Data\\export-2016-04-04T22_00_01.000Z.csv\\");
//        PredictionsToFiles.run("C:\\Users\\Michael\\Documents\\study\\Thesis\\4 - results\\temp\\Test\\input\\","C:\\Users\\Michael\\Documents\\study\\Thesis\\4 - results\\temp\\Test\\output\\");
        PredictionsToFiles.run("/home/ise/Desktop/michael/thesis/db_regular_input/baseline/","/home/ise/Desktop/michael/thesis/db_regular_output/baseline/",true);
        PredictionsToFiles.run("/home/ise/Desktop/michael/thesis/db_regular_input/textmodel/","/home/ise/Desktop/michael/thesis/db_regular_output/textmodel/");
        PredictionsToFiles.run("/home/ise/Desktop/michael/thesis/db_regular_input/integration/","/home/ise/Desktop/michael/thesis/db_regular_output/integration/");
//        insertTrainTestSet(conn, "train.csv", "test.csv");
//        insertPredictionsRegularExp(conn, "prediction.csv",);
//        insertPredictionsRegularExp(conn, "integrated_prediction.csv",false);
//        insertPredictionsColdStart(conn, "predictions_cold.csv");

//        DBUtils.runDDLQuery(conn, "drop index prediction_index_sample");

//        runQueryWithTryAndCatch(conn, "drop index prediction_index_model");
//
//        runQueryWithTryAndCatch(conn, "drop index prediction_index_epoch");
//
//        runQueryWithTryAndCatch(conn, "drop index prediction_cold_index_expname");
//
//        runQueryWithTryAndCatch(conn, "drop index prediction_cold_index_type");
//
//        runQueryWithTryAndCatch(conn, "drop index prediction_cold_index_sample");
//
//        runQueryWithTryAndCatch(conn, "drop table prediction_cold_table");//        System.out.println("done deleting");
//
//        runQueryWithTryAndCatch(conn, "create table prediction_cold_table(	exp_name varchar(300),	percent integer,	type varchar(300),	epoch integer,	model varchar(300),	count integer,	x_test varchar(10000),	y_test real,	y_pred real)");

//        insertPredictionsColdStart(conn, "predictions_baseline.csv", false);
////        insertPredictionsColdStart(conn, "predictions_textmodel.csv", false);
////        insertPredictionsColdStart(conn, "predictions_integration.csv", false);
//
//        insertPredictionsColdStart(conn, "predictions_cold.csv", false);
//        runQueryWithTryAndCatch(conn, "create index prediction_index_sample	on prediction_cold_table (exp_name, percent, model, count)");
//        runQueryWithTryAndCatch(conn, "create index prediction_index_model	on prediction_cold_table (type)");
//        runQueryWithTryAndCatch(conn, "create index prediction_index_epoch	on prediction_cold_table (epoch)");
//        runQueryWithTryAndCatch(conn, "create index prediction_cold_index_expname	on prediction_cold_table (exp_name)");
//        runQueryWithTryAndCatch(conn, "create index prediction_cold_index_type	on prediction_cold_table (model)");
//        runQueryWithTryAndCatch(conn, "create index prediction_cold_index_sample	on prediction_cold_table (exp_name, percent, type, count)");

//
// (conn, "" , false);

//        insertRemovedItems(conn,"remove_items_to_db.csv");
//        DBUtils.writeQueryToCSV(conn, "select distinct dayofsession, userid, sessiont.sessionid, clicks,buy, y_baseline, y_ourmodel, gap  from session_table as sessiont join    (      select sessionid, big_gap.y_baseline ,big_gap.y_ourmodel, big_gap.gap      from test_table as testt        join (               select t1.indexloc, t1.y_pred as y_baseline ,t2.y_pred as y_ourmodel, abs(t1.y_pred - t2.y_pred) as gap               from prediction_table as t1                 join prediction_table as t2                   on (t1.indexloc = t2.indexloc)               where t1.exp_name = 'baseline_no_cold_start_0_1' and t2.exp_name ='ourmodel_no_cold_start_0_1'                     and t1.epoch = 1 and t2.epoch =11               order by gap desc               ) big_gap on (testt.indexloc = big_gap.indexloc)    ) as test_gap      on (sessiont.sessionid = test_gap.sessionid)  order by gap desc  ", "results-q1.csv");
//        DBUtils.writeQueryToCSV(conn, "select distinct dayofsession, userid, sessiont.sessionid, clicks,buy, y_baseline, y_ourmodel, gap  from session_table as sessiont join    (      select sessionid, big_gap.y_baseline ,big_gap.y_ourmodel, big_gap.gap      from test_table as testt        join (               select t1.indexloc, t1.y_pred as y_baseline ,t2.y_pred as y_ourmodel, abs(0.5 - t2.y_pred) as gap               from prediction_table as t1                 join prediction_table as t2                   on (t1.indexloc = t2.indexloc)               where t1.exp_name = 'baseline_no_cold_start_0_1' and t2.exp_name ='ourmodel_no_cold_start_0_1'                     and t1.epoch = 1 and t2.epoch =11               order by gap desc               ) big_gap on (testt.indexloc = big_gap.indexloc)    ) as test_gap      on (sessiont.sessionid = test_gap.sessionid)  order by gap ", "results-q2.csv");
//        DBUtils.writeQueryToCSV(conn, "select    clicks,    buy,    avg(y_baseline) as avg_baseline,    avg(y_ourmodel) as avg_ourmodel,    count(y_baseline) as cnt_baseline,    count(y_ourmodel) as cnt_ourmodel  from session_table as sessiont    join (select            sessionid,            big_gap.y_baseline,            big_gap.y_ourmodel          from test_table as testt            join (select                    t1.indexloc,                    t1.y_pred                  as y_baseline,                    t2.y_pred                  as y_ourmodel                  from prediction_table as t1                    join prediction_table as t2 on (t1.indexloc = t2.indexloc)                  where t1.exp_name = 'baseline_no_cold_start_0_1' and t2.exp_name = 'ourmodel_no_cold_start_0_1' and                        t1.epoch = 1 and t2.epoch = 11                  ) big_gap on (testt.indexloc = big_gap.indexloc)) as test_gap      on (sessiont.sessionid = test_gap.sessionid)    group by clicks,buy  order by clicks,buy", "results-q3.csv");
//        DBUtils.writeQueryToCSV(conn, "select predtest.dayofsession, timestamp_, userid_, sessionid_, y_baseline, y_ourmodel, itemid_, recommended_, hours_, day_, title, manufacturer, generalcategory, category1, category, category3, deepcategory, lastcategory, categorygerman, price, englishcatgory from ( select * from test_table as test join ( select t1.y_pred as y_baseline, t2.y_pred as y_ourmodel, t1.indexloc as indexloc from prediction_table as t1 join prediction_table as t2 on (t1.indexloc = t2.indexloc) where t1.exp_name = 'baseline_no_cold_start_0_1' and t2.exp_name = 'ourmodel_no_cold_start_0_1' and t1.epoch = 1 and t2.epoch = 11 ) predt on (test.indexloc = predt.indexloc) ) predtest join ( select click.timestamp as timestamp_, click.userid as userid_, click.sessionid as sessionid_, click.recommended as recommended_, click.hours as hours_, click.day as day_, click.itemid as itemid_, click.sessionid, title, manufacturer, generalcategory, category1, category, category3, deepcategory, lastcategory, categorygerman, price, englishcatgory from click_table as click left outer join (select * from catalog_table where filename = 'content_data_export_2016-08-31_01-01-00-015[items-0].csv' UNION select filename,timestamp,catfile2.itemid,image_url,title,manufacturer,generalcategory, category1,category,category3,deepcategory,lastcategory,shortdescription,categorygerman, price,englishcatgory,htmlcategory from catalog_table as catfile2 join (select itemid from catalog_table where filename = 'content_data_export_2016-08-19_22-01-00-026[items-0].csv' EXCEPT select itemid from catalog_table where filename = 'content_data_export_2016-08-31_01-01-00-015[items-0].csv') as itemsfile2 on (catfile2.itemid = itemsfile2.itemid) ) as catelog on (click.itemid = catelog.itemid) ) clickcat on (predtest.sessionid = clickcat.sessionid) order by clickcat.sessionid, timestamp_ ", "results-q4.csv", false, ";");
//        DBUtils.writeQueryToCSV(conn, "select dayofsession,userid,sessionid,clicks,buys,exp_name,epoch,y_test,y_pred  from test_table as test    join prediction_table as predt on (test.indexloc = predt.indexloc)  where predt.exp_name = 'baseline_no_cold_start_0_1' or predt.exp_name = 'ourmodel_no_cold_start_0_1'  order by sessionid, exp_name, epoch", "results-q5.csv", false, ";");
//        DBUtils.runDDLQuery(conn,"update prediction_cold_table set model='baseline_old' where model='baseline'");
//        DBUtils.runDDLQuery(conn,"update prediction_cold_table set model='baseline_old' where model='integrated'");
//        DBUtils.runDDLQuery(conn, "update prediction_cold_table set x_test='NaN' where model='ourmodel'");
//        DBUtils.runDDLQuery(conn, "update prediction_cold_table set x_test='NaN' where model='ourmodel_emb5'");
//
//        DBUtils.runDDLQuery(conn, "update prediction_cold_table set model='baseline_itr1' where model='baseline'");
//        DBUtils.runDDLQuery(conn, "update prediction_cold_table set model='integrated_itr1' where model='integrated'");
//        DBUtils.runDDLQuery(conn, "update prediction_cold_table set model='ourmodel_itr1' where model='ourmodel'");
//        DBUtils.runDDLQuery(conn, "update items_removed_table set exp_name = 'remove_items_old' where exp_name='remove_items'");
//        DBUtils.runDDLQuery(conn, "update items_removed_table set exp_name = 'remove_sessions_old' where exp_name='remove_sessions'");
//        DBUtils.runDDLQuery(conn, "update test_cold_table set exp_name = 'remove_items_old' where exp_name='remove_items'");
//        DBUtils.runDDLQuery(conn, "update test_cold_table set exp_name = 'remove_sessions_old' where exp_name='remove_sessions'");
//        writeColdStartPredictionsToCsv(conn);
//        conn.close();
        System.out.println("done all");
        System.out.println("done all");

    }

    private static void writeColdStartPredictionsToCsv(Connection conn)
    {
        String[][] epochs = new String[][]
                {
                        {"1", "remove_items", "1", "1", "12"},
                        {"2", "remove_items", "1", "1", "12"},
                        {"3", "remove_items", "0", "1", "17"},
                        {"4", "remove_items", "0", "0", "8"},
                        {"5", "remove_items", "0", "0", "8"},
                        {"6", "remove_items", "0", "0", "6"},
                        {"7", "remove_items", "0", "0", "6"},
                        {"8", "remove_items", "1", "1", "5"},
                        {"1", "remove_sessions", "1", "1", "8"},
                        {"2", "remove_sessions", "1", "1", "8"},
                        {"3", "remove_sessions", "1", "1", "9"},
                        {"4", "remove_sessions", "1", "0", "8"},
                        {"5", "remove_sessions", "1", "1", "8"},
                        {"6", "remove_sessions", "0", "0", "4"},
                        {"7", "remove_sessions", "0", "0", "12"},
                        {"8", "remove_sessions", "0", "0", "4"}
                };
        for (int percent = 1; percent < 9; percent++)
        {
            String[] exps = new String[]{"remove_items", "remove_sessions"};
            for (String exp : exps)
            {
                int epoch1 = -1;
                int epoch2 = -1;
                int epoch3 = -1;
                for (String[] tuple : epochs)
                {
                    if (Integer.parseInt(tuple[0]) == percent &&
                            tuple[1].equals(exp))
                    {
                        epoch1 = Integer.parseInt(tuple[2]);
                        epoch2 = Integer.parseInt(tuple[3]);
                        epoch3 = Integer.parseInt(tuple[4]);
                    }
                }
                writeColdPredictionToFile(conn, exp, percent, epoch1, epoch3, epoch2);
            }
        }
    }

    private static void writeColdPredictionToFile(Connection conn, String model, String expname, int precent, int epoch)
    {
        System.out.println("writing model=" + model + " expname=" + expname + " precent=" + precent + " epoch=" + epoch);
        String filename = expname + "_" + precent + "_" + model + ".csv";
        try
        {
            DBUtils.writeQueryToCSV(conn, "select t1.exp_name, t1.percent, t1.type, t1.cold_start_items, t1.count, t1.indexloc, t1.dayofsession, t1.userid, t1.sessionid, t1.clicks, t1.buy,t2.y_test, t2.y_pred from test_cold_table as t1 join prediction_cold_table as t2 on (t1.exp_name = t2.exp_name and t1.percent = t2.percent and t1.type = t2.type and t1.count = t2.count) where t2.model = '" + model + "' and t2.epoch=" + epoch + " and t2.exp_name='" + expname + "' and t2.percent=" + precent + "order by sessionid", "outputcold" + filename);
        } catch (SQLException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static void writeColdPredictionToFile(Connection conn, String expname, int precent, int epoch1, int epoch2, int epoch3)
    {
        String filename = expname + "_" + precent + ".csv";
        try
        {
            String sql = "select t1.exp_name, t1.percent, t1.type, t1.cold_start_items, t1.count, t1.indexloc, t1.dayofsession, t1.userid, t1.sessionid, t1.clicks, t1.buy,t2.y_test, t2.y_baseline, t2.y_textmodel, t2.y_integrated from test_cold_table as t1 join ( select t3.exp_name, t3.percent, t3.type, t3.count,t3.y_test, t3.y_pred as y_baseline, t4.y_textmodel, t4.y_integrated from prediction_cold_table t3 join ( select t5.exp_name, t5.percent, t5.type, t5.count, t5.y_pred as y_textmodel, t6.y_pred as y_integrated from prediction_cold_table t5 join prediction_cold_table t6 on ( (t5.exp_name = t6.exp_name and t5.percent = t6.percent and t5.type = t6.type and t5.count = t6.count) ) where t5.model = 'ourmodel' and t6.model = 'integrated' and t5.epoch = " + epoch2 + " and t6.epoch = " + epoch3 + " and t5.exp_name = '" + expname + "' and t5.percent = " + precent + " ) t4 on (t3.exp_name = t4.exp_name and t3.percent = t4.percent and t3.type = t4.type and t3.count = t4.count) where t3.model = 'baseline' and t3.epoch = " + epoch1 + " ) t2 on (t1.exp_name = t2.exp_name and t1.percent = t2.percent and t1.type = t2.type and t1.count = t2.count) order by sessionid;";
            System.out.println(sql);
            DBUtils.writeQueryToCSV(conn, sql, filename);
        } catch (SQLException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static void insertPredictionsRegularExp(Connection conn, String prediction_path)
    {
        System.out.println("running with file " + prediction_path);
        String tableName = "prediction";
        insertPredictionsRegularExp(conn, prediction_path, true);
        System.out.println("done file " + prediction_path);
    }

    private static void insertPredictionsRegularExp(Connection conn, String prediction_path, boolean createNew)
    {
        System.out.println("running with file " + prediction_path);
        String tableName = "prediction";
        String create_train_table = "CREATE TABLE " + tableName + "_table (exp_name varchar (300),epoch int,indexLoc int,x_test varchar (10000) ,y_test real ,y_pred real,trash05 real,rightpred real)";
        createTablePredictions(conn, prediction_path, tableName, create_train_table, createNew);
        System.out.println("done with file " + prediction_path);

    }

    private static void insertRemovedItems(Connection conn, String prediction_path)
    {
        String tableName = "items_removed";
        String create_train_table = "CREATE TABLE " + tableName + "_table (exp_name varchar (300),precent int,itemid varchar(255))";
        String filename = tableName + "_table.csv";
        System.out.println(create_train_table);
        if (conn != null)
        {
            runQueryWithTryAndCatch(conn, create_train_table);
        }
        removeFirstLineFromFile(prediction_path, filename);
        csvInsertToDb(conn, tableName, filename);
        System.out.println("done");

    }

    private static void createTablePredictions(Connection conn, String prediction_path, String tableName, String create_train_table, boolean createNew)
    {
        String filename = tableName + "_table.csv";
        if (createNew)
        {
            System.out.println(create_train_table);
            String drop_index_train_table_sessionid = "DROP INDEX " + tableName + "_table_indexloc";
            String drop_table_train_table = "DROP TABLE " + tableName + "_table";
            if (conn != null)
            {
                runQueryWithTryAndCatch(conn, drop_index_train_table_sessionid);
                runQueryWithTryAndCatch(conn, drop_table_train_table);
                runQueryWithTryAndCatch(conn, create_train_table);
            }
            System.out.println("done");
        }
        removeFirstLineFromFile(prediction_path, filename);
        csvInsertToDb(conn, tableName, filename);
    }

    private static void insertPredictionsColdStart(Connection conn, String prediction_path)
    {
        System.out.println("running with file " + prediction_path);
        insertPredictionsColdStart(conn, prediction_path, true);
        System.out.println("done with file " + prediction_path);
    }

    private static void insertPredictionsColdStart(Connection conn, String prediction_path, boolean createNew)
    {
//        String table1 = "items_removed";
//        String table2 = "test_cold";
        String table3 = "prediction_cold";
//        String create_items_removed = "CREATE TABLE " + table1 + "_table (exp_name varchar (300),percent int, count int,item varchar (30))";
//        String create_test_cold = "CREATE TABLE " + table2 + "_table (exp_name varchar (300),percent int, type varchar (300),cold_start_items int, count int, indexLoc int,dayofsession date,userid varchar(255), sessionid int,clicks int, buy int, actions varchar(10000))";
//        String create_prediction_cold = "CREATE TABLE " + table3 + "_table (exp_name varchar (300),percent int, model varchar (300), epoch int, type varchar (300), count int,x_test varchar (10000) ,y_test real ,y_pred real)";
//        if (createNew)
//        {
//            if (conn != null)
//            {
//                runQueryWithTryAndCatch(conn, "drop table "+table1+"_table");
//                runQueryWithTryAndCatch(conn, "drop table " + table2 + "_table");
//                runQueryWithTryAndCatch(conn, "drop table "+table3+"_table");
//                runQueryWithTryAndCatch(conn, create_items_removed);
//                runQueryWithTryAndCatch(conn, create_test_cold);
//                runQueryWithTryAndCatch(conn, create_prediction_cold);
//            }
//        }
//        csvInsertToDb(conn, table1, "item_removed.csv",';');
//        csvInsertToDb(conn, table2, "test_cold.csv", ';');
        System.out.println("insert predictions");
        csvInsertToDb(conn, table3, "predictions_baseline.csv", ';');
        System.out.println("done baseline");
        csvInsertToDb(conn, table3, "predictions_textmodel.csv", ';');
        System.out.println("done text model");
        csvInsertToDb(conn, table3, "predictions_integration.csv", ';');
        System.out.println("done integration");
    }

    private static void csvInsertToDb(Connection conn, String tableName, String filename)
    {
        csvInsertToDb(conn, tableName, filename, ',');
    }

    private static void csvInsertToDb(Connection conn, String tableName, String filename, char delimiter)
    {
        System.out.println("insert file=" + filename + " to table " + tableName);
        if (conn != null)
        {
            try
            {
                DBUtils.copyCSVToPostgre(conn, tableName + "_table", filename, delimiter);
//                DBUtils.runDDLQuery(conn, "CREATE INDEX " + tableName + "_index_indexloc on " + tableName + "_table (indexloc)");
            } catch (SQLException e)
            {
                e.printStackTrace();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    private static void removeFirstLineFromFile(String prediction_path, String filename)
    {
        try (BufferedWriter writer = new BufferedWriter(new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(filename), "UTF-8"))))
        {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(prediction_path), "UTF8")))
            {
                String line;
                boolean first = true;
                while ((line = br.readLine()) != null)
                {
                    if (first)
                    {
                        first = false;
                        continue;
                    }
                    if (line.length() > 0)
                    {
                        writer.write(line);
                        writer.write("\n");
                    }
                }
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private static void insertTrainTestSet(Connection conn, String trainPath, String testPath)
    {
        insertSetToDB(conn, trainPath, "train");
        insertSetToDB(conn, testPath, "test");
    }

    private static void insertSetToDB(Connection conn, String trainPath, String tableName)
    {
        String create_train_table = "CREATE TABLE " + tableName + "_table (indexloc int,dayofsession date, userid varchar(255),sessionid int, clicks int , buys int, actions varchar(10000))";
        String drop_index_train_table_sessionid = "DROP INDEX " + tableName + "_table_sessionid";
        String drop_index_train_table_indexloc = "DROP INDEX " + tableName + "_table_sessionid";
        String drop_table_train_table = "DROP TABLE " + tableName + "_table";
        String filename = tableName + "_table.csv";
        if (conn != null)
        {
            runQueryWithTryAndCatch(conn, drop_index_train_table_sessionid);
            runQueryWithTryAndCatch(conn, drop_index_train_table_indexloc);
            runQueryWithTryAndCatch(conn, drop_table_train_table);
            runQueryWithTryAndCatch(conn, create_train_table);
        }
        System.out.println("done");
        removeFirstLineFromFile(trainPath, filename);
        if (conn != null)
        {
            try
            {
                DBUtils.copyCSVToPostgre(conn, tableName + "_table", filename);
                DBUtils.runDDLQuery(conn, "CREATE INDEX " + tableName + "_index_sessionid on " + tableName + "_table (sessionid)");
                DBUtils.runDDLQuery(conn, "CREATE INDEX " + tableName + "_index_indexloc on " + tableName + "_table (indexloc)");
            } catch (SQLException e)
            {
                e.printStackTrace();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    private static void insertCatalog(Connection conn, String path) throws IOException, SQLException
    {
        insertCatalog(conn, path, false);
    }

    private static void insertCatalog(Connection conn, String path, boolean newCatalog) throws IOException, SQLException
    {
        //Create table
        ////        "filename0,timestamp1,2itemid,3image_url,4title,5manufacturer,6generalcategory,7category1,8category2,9category3,10deepcategory,11lastcategory,12shortdescription,13categoryGerman,14price,15englishcatgory,16htmlcategory"
        String catalog_table = "catalog_table";
        if (newCatalog)
        {
            catalog_table = "catalog_table_new";
        }
        String createCatalogTable = "CREATE TABLE " + catalog_table + " (filename varchar(255),timestamp TIMESTAMP, itemid varchar(255), " +
                "image_url varchar(1000), title varchar(255), manufacturer varchar(255), generalcategory varchar(255), category1 varchar(255), category varchar(255)," +
                "category3 varchar(255), deepcategory int, lastcategory varchar(255), shortdescription varchar(2000), categoryGerman varchar(255)," +
                "price REAL, englishcatgory varchar(255), htmlcategory varchar(50000))";
        System.out.println("creating catalog table");
        if (conn != null)
        {
            runQueryWithTryAndCatch(conn, "DROP INDEX catalog_index_itemid");
            runQueryWithTryAndCatch(conn, "DROP INDEX catalog_index_timestamp");
            runQueryWithTryAndCatch(conn, "DROP TABLE catalog_table");
            runQueryWithTryAndCatch(conn, createCatalogTable);
        }
        System.out.println("done");
        try (BufferedWriter writer = new BufferedWriter(new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream("catalog.csv"), "UTF-8"))))
        {
//            writer.write("filename,timestamp,itemid,image_url,title,manufacturer,generalcategory,category1,category2,category3,deepcategory,lastcategory,shortdescription,categoryGerman,price,englishcatgory,htmlcategory\n");
//            String[] allFilesInDict = getAllFilesInDict(System.getProperty("user.dir") + "/generalCatalog/", ".csv");
            String[] allFilesInDict = getAllFilesInDict(path, ".csv");
            Arrays.sort(allFilesInDict);
            System.out.println("files=" + allFilesInDict.length);
            int i = 1;
            for (String filename : allFilesInDict)
            {
                System.out.print(i + " " + filename + ":");
                String file = path + "/" + filename;
                String timestamp = "";
                try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF8")))
                {
                    boolean first = true;
                    String line;
                    boolean hasHtml = false;
                    while ((line = br.readLine()) != null)
                    {
                        if (line.length() == 0)
                        {
                            continue;
                        }
                        if (first)
                        {
                            first = false;
                            hasHtml = line.indexOf("Volltext-Beschreibung") > -1;
                            int exportLength = "export_".length();
                            int exportLoc = filename.indexOf("export");
                            int startExport = exportLoc > -1 ? exportLoc + exportLength : exportLength;
                            int bracesLoc = filename.indexOf("[");
                            int endExport = bracesLoc > -1 ? bracesLoc : filename.length();
                            String onlyTimestamp = filename.substring(startExport, endExport).replace(".csv", "");
                            String pattern = "";
                            if (hasHtml)
                            {
                                //2016-08-19_22-01-00-026

                                pattern = "yyyy-MM-dd_HH-mm-ss-SSS";
                            } else
                            {

//                                timestamp = DateUtils.getDateToDB("2016-04-01T22_00_01.000Z","yyyy-MM-dd'T'HH:mm:ss.'Z");
                                //2016-10-12T22:00:01.000Z
                                //2016-10-12T22:00:01.000Z
                                pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
                            }
                            timestamp = DateUtils.getDateToDB(onlyTimestamp, pattern);
                            continue;
                        }
                        try
                        {
                            writer.write(handleLine(line, hasHtml, filename, timestamp));
                            writer.write("\n");
                        } catch (Exception ex)
                        {
                            System.out.println("line=" + line);
                            ex.printStackTrace();
                        }
                    }
                    System.out.println("done");
                } catch (Exception ex)
                {
                    System.out.println("error in file" + filename);
                    ex.printStackTrace();
                }
                i++;
            }
        } catch (Exception ex)
        {
            ex.printStackTrace();
        }
        if (conn != null)
        {
            DBUtils.copyCSVToPostgre(conn, "catalog_table", "catalog.csv", ';');
            DBUtils.runDDLQuery(conn, "CREATE INDEX catalog_index_itemid on catalog_table (itemid)");
            DBUtils.runDDLQuery(conn, "CREATE INDEX catalog_index_timestamp on catalog_table (timestamp)");
        }
    }

    private static String[] split(String line)
    {
        ArrayList<String> ans = new ArrayList<>();
        int startPos = 0;
        boolean qoutes = false;
        for (int i = 0; i < line.length(); i++)
        {
            if (line.charAt(i) == '"')
            {
                qoutes ^= true;
            }
            if (line.charAt(i) == ';')
            {
                ans.add(line.substring(startPos, i));
                startPos = i + 1;
            }
        }
        return ans.toArray(new String[ans.size()]);
    }

    private static String handleLine(String line, boolean isHtml, String filename, String timestamp)
    {
//        "0itemid,1image_url,2title,3manufacturer,4generalcategory,5category1,6category2,7category3,8deepcategory,9lastcategory,10shortdescription,11categoryGerman,12price,13englishcatgory,14htmlcategory"
        String[] fields = line.replaceAll("(?!\");(?!\")", "").split(";");
//        if(fields.length > 13)
//        {
//            fields = split(line);
//        }
        String[] ans = new String[17];
        ans[0] = filename;
        ans[1] = timestamp;
        if (fields[0].indexOf("100169542") > -1)
        {
            int x = 3;
        }
        for (int i = 0; i < 5; i++)
        {
            ans[i + 2] = fields[i].replace("\"", "");
        }
        String category = fields[4].replace("\"", "").trim();
        ans[6] = category;
        String[] categories = category.split(">");
        ans[7] = categories[0].replace("\"", "").trim();
        ans[8] = categories.length > 1 ? categories[1].replace("\"", "").trim() : "";
        ans[9] = categories.length > 2 ? categories[2].replace("\"", "").trim() : "";
        ans[10] = categories.length + "";
        ans[11] = categories[categories.length - 1].trim();
        ans[12] = fields[5].replace("\"", "");
        ans[13] = fields[7].replace("\"", "");
        ans[14] = fields[6].replace("\"", "");
        ans[15] = fields[9].replace("\"", "");
        if (isHtml)
        {
//            "0product_id;1image_url;2title;3manufacturer;4Google-Produkttyp;5description;6price;7Kategorie 1;8categorypath_id;9categorie;10wt_start;11wt_end;12Neu im Sortiment;13Volltext-Beschreibung"
            ans[16] = fields[13].replace("\"", "");
        } else
        {
            ans[16] = "";
        }

        return String.join(";", ans);
    }

    private static void addHourAndDay(Connection conn) throws SQLException
    {
        ITableAction a1 = new TableActionBuy();
        ITableAction a2 = new TableActionClick();
        ITableAction a4 = new TableActionTransfer();
        ITableAction[] arr = new ITableAction[]{a1, a2, a4};
//			ITableAction[] arr = new ITableAction[]{a2};
        try
        {
            for (ITableAction iTableAction : arr)
            {
                iTableAction.removeIndex(conn);
            }
        } catch (SQLException e)
        {
            e.printStackTrace();
        }
        removeSessionIndexes(conn);
        System.out.println("delete all sessions");
//        DBUtils.runDDLQuery(conn,"ALTER TABLE click_table ADD hours time");
//        DBUtils.runDDLQuery(conn,"ALTER TABLE click_table ADD day date");
        System.out.println("update click table");
        DBUtils.runDDLQuery(conn, "UPDATE click_table set day=date(timestamp) , hours=timestamp::time");
        DBUtils.runDDLQuery(conn, "ALTER TABLE buy_table ADD hours time");
        DBUtils.runDDLQuery(conn, "ALTER TABLE buy_table ADD day date");
        System.out.println("update buy table");
        DBUtils.runDDLQuery(conn, "UPDATE buy_table set day=date(timestamp) , hours=timestamp::time");
        DBUtils.runDDLQuery(conn, "ALTER TABLE transfer_table ADD hours time");
        DBUtils.runDDLQuery(conn, "ALTER TABLE transfer_table ADD day date");
        System.out.println("update transfer table");
        DBUtils.runDDLQuery(conn, "UPDATE transfer_table set day=date(timestamp) , hours=timestamp::time");
        System.out.println("create indexes again");

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
        createSessionIndexes(conn);
    }

    private static void fixInput(Connection conn)
    {
        updateLongItemId(conn, "000000000100188913", "000000000100188912f4fff");
        updateLongItemId(conn, "000000000100168657", "0000000001001686a68b8f8");
        updateLongItemId(conn, "000000000100167142", "0000000001001671a68b8f8");
        updateLongItemId(conn, "000000000100189016", "0000000001001890a68b8f8");
        updateLongItemId(conn, "000000000100192386", "0000000001001923a68b8f8");
        updateLongItemId(conn, "000000000100179742", "0000000001001797a68b8f8");
        updateLongItemId(conn, "000000000100195784", "00000000010019576450232");
        updateLongItemId(conn, "000000000100185826", "0000000001001858a68b8f8");
    }

    private static void updateLongItemId(Connection conn, String newValue, String oldValue)
    {
        String query = "UPDATE click_table SET itemid = '" + newValue + "' " + "WHERE itemid ='" + oldValue + "'";
        runQueryWithTryAndCatch(conn, query);
    }

    private static void runQueryWithTryAndCatch(Connection conn, String query)
    {
        try
        {
            DBUtils.runDDLQuery(conn, query);
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void createStat(Connection conn)
    {
        writeStat(
                conn, "" + "select userid,date(timestamp),count(*) " + "from buy_table "
                        + "group by userid,date(timestamp) " + "order by date(timestamp),userid",
                "user_session_stat.csv");
        writeStat(
                conn, "" + "select itemid,date(timestamp),count(*) " + "from buy_table "
                        + "group by itemid,date(timestamp) " + "order by date(timestamp),itemid",
                "item_session_stat.csv");
    }

    private static void writeStat(Connection conn, String sql, String path)
    {
        try
        {
            DBUtils.writeQueryToCSV(conn, sql, path);
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void updateSessionState(Connection conn)
    {
        UpdateSessionState.run(conn);
    }

    private static void removeNull(Connection conn)
    {
        changeNullToUnknown(conn, "buy_table");
        changeNullToUnknown(conn, "click_table");
        changeNullToUnknown(conn, "transfer_table");
    }

    private static void changeNullToUnknown(Connection conn, String table)
    {
        String query = "delete from " + table + " where userid='null' or userid is null";
        runQueryWithTryAndCatch(conn, query);
    }

    private static void removeDuplicate(Connection conn)
    {
        RemoveDuplicates.runAll(conn);
    }

    private static void printSessionFile(Connection conn, String path, String from, String to)
    {
        printSessionFile(conn, path, from, to, false);
    }

    private static void printSessionFile(Connection conn, String path, String from, String to, boolean keepFile)
    {
        String sql = "select * from session_table where sessionid >= " + from + "and  sessionid <= " + to
                + " order by sessionid";
        try
        {
            DBUtils.writeQueryToCSV(conn, sql, path, keepFile);
        } catch (SQLException | IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void createSquenceDataSession(Connection conn)
    {
        try
        {
            CreateSquenceDataSession.run(conn, "eventsquance.txt");
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private static void createTables(Connection conn) throws SQLException
    {
        // CREATE TABLE session_table ( dayofsession date,userid varchar(255),
        // sessionid varchar(30));
        TableActionBuy b = new TableActionBuy();
        b.dropTable(conn);
        b.createTable(conn);
        TableActionClick c = new TableActionClick();
        c.dropTable(conn);
        c.createTable(conn);
        TableActionTransfer t = new TableActionTransfer();
        t.dropTable(conn);
        t.createTable(conn);
        // TableActionBasket ba = new TableActionBasket();
        // ba.createTable(conn);
        try
        {
            DBUtils.dropTable(conn, "session_table");
        } catch (Exception ex)
        {
            ex.printStackTrace();
        }
        try
        {
            DBUtils.dropTable(conn, "buy_table_old");
        } catch (Exception ex)
        {
            ex.printStackTrace();
        }
        try
        {
            DBUtils.dropTable(conn, "click_table_old");
        } catch (Exception ex)
        {
            ex.printStackTrace();
        }
        try
        {
            DBUtils.runDDLQuery(conn, SQL_CREATE_SESSION_TABLE);
        } catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    private static void insertCSVToDB(Connection conn) throws ClassNotFoundException, SQLException, IOException
    {
        DBUtils.copyCSVToPostgre(conn, "buy_table", "buy.csv");
        DBUtils.copyCSVToPostgre(conn, "click_table", "click.csv");
        DBUtils.copyCSVToPostgre(conn, "click_table", "rclick.csv");
        DBUtils.copyCSVToPostgre(conn, "transfer_table", "transfer.csv");
        // DBUtils.copyCSVToPostgre(conn, "basket_table", "basket.csv");
    }

    private static void updateSession(Connection conn)
    {
        try
        {
            UpdateSessions.run(conn);
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        createSessionIndexes(conn);
    }

    private static void createSessionIndexes(Connection conn)
    {
        try
        {
            DBUtils.runDDLQuery(conn, "CREATE INDEX new_buy_index_sessionid on buy_table (sessionid)");
            DBUtils.runDDLQuery(conn, "CREATE INDEX new_click_sessionid on click_table (sessionid)");
        } catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    private static void removeSessionIndexes(Connection conn)
    {
        try
        {
            DBUtils.runDDLQuery(conn, "DROP INDEX new_buy_index_sessionid");
            DBUtils.runDDLQuery(conn, "DROP INDEX new_click_sessionid");
        } catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    private static void createDataFromFile() throws Exception
    {
        IInputActions input = new InputActionFromFile(getAllFilesInDict(System.getProperty("user.dir")));
//        IInputActions input = new InputActionFromFile(getAllFilesInDict(System.getProperty("/data/")));
        // IInputActions input = new InputActionFromFile(new String[] {
        // "C:\\Users\\Michael\\Documents\\לימודים\\תואר
        // שני\\Thesis\\data\\Yoochose\\CleanData\\461-tracking-august\\461-tracking-august.out"});
        IActionParser p1 = new BuyActionParserFromRaw();
        IActionParser p2 = new ClickActionParserFromRaw();
        IActionParser p3 = new ClickRecommendedParserFromRaw();
        IActionParser p4 = new TransferActionParser();
        IActionParser p5 = new BasketActionParserFromRaw();
        IParseActions parser = new ParserBasic(new IActionParser[]{p1, p2, p3, p4});
        IOutput output = new OutputCSV("buy.csv", "click.csv", "rclick.csv", "transfer.csv");
        IYCReader reader = new YCReaderBatch(input, output, parser);
        reader.parse();
    }

    private static String[] getAllFilesInDict(String path)
    {
        return getAllFilesInDict(path, ".out");
    }

    private static String[] getAllFilesInDict(String path, String suffix)
    {
        File folder = new File(path);
        ArrayList<String> ans = new ArrayList<>();
        File[] listOfFiles = folder.listFiles();
        for (int i = 0; i < listOfFiles.length; i++)
        {
            if (listOfFiles[i].isFile() && listOfFiles[i].getName().endsWith(suffix))
            {
                // System.out.println("File " + listOfFiles[i].getName());
                ans.add(listOfFiles[i].getName());
            } else if (listOfFiles[i].isDirectory())
            {
                // System.out.println("Directory " + listOfFiles[i].getName());
            }
        }
        return ans.toArray(new String[ans.size()]);
    }
}
