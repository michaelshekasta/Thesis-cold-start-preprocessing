package BL;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PredictionsToFiles
{
    public static String getNameOfDir(File file)
    {
        String name = file.getName();
        int pos = name.lastIndexOf(".");
        if (pos > 0)
        {
            name = name.substring(0, pos);
        }
        return name;
    }

    public static void run(String inputFolder, String outputFolder)
    {
        run(inputFolder,outputFolder,false);
    }
    public static void run(String inputFolder, String outputFolder, boolean runAll)
    {
        File[] files = initHeaders(outputFolder);
        File[] directories = new File(inputFolder).listFiles(File::isDirectory);
        Arrays.sort(directories);
        if(runAll)
        {
            System.out.println("running all");
            for (int percent = 1; percent < 9; percent++)
            {
                for (String expName : new String[]{"remove_items","remove_sessions"})
                {
                    handleGeneralFiles(files, expName, percent + "", inputFolder + expName + "_0" + percent+"/data_before_encode/");
                }
            }
        }
        for (File directory : directories)
        {
            if(!getNameOfDir(directory).startsWith("remove"))
            {
                handleExpirement(directory, files);
            }
        }
    }

    private static void handleExpirement(File directory, File[] files)
    {
        String[] fullname = getNameOfDir(directory).split("_");
//        System.out.println("fullname=" + Arrays.toString(fullname));
        String model = fullname[0];
        String expName = fullname[1] + "_" + fullname[2];
        String percent = "" + Integer.parseInt(fullname[3]);
        String beforeEncode = directory + "/data_before_encode/";

        for (int epoch = 0; epoch < 20; epoch++)
        {
            for (String type : new String[]{"cold_start", "regular"})
            {
                String filePath = directory + "/detailed_prediction/model_predict_" + type + "_" + epoch + ".csv";
                System.out.print("working on file "+filePath+":");
                HandlePredictions(filePath, expName, percent, epoch, type, model, files[2]);
                System.out.println("done");
            }
        }
    }

    private static void handleGeneralFiles(File[] files, String expName, String percent, String beforeEncode)
    {
        System.out.println("running on "+expName+" "+percent);
        List<String> itemsRemoved = HandleItemsRemove(beforeEncode + "items_removed.csv", expName, percent, files[0]);
        if(expName.equals("remove_items"))
        {
            String filePath1 = beforeEncode + "test_new.csv";
            System.out.print("working on file "+filePath1+" :");
//            HandleTestTable(filePath1, expName, percent, files[1], "regular", itemsRemoved);
            System.out.println("done");
            String filePath2 = beforeEncode + "new_test_only_new_items.csv";
            System.out.print("working on file "+filePath1+" :");
            HandleTestTable(filePath2, expName, percent, files[1], "cold_start", itemsRemoved);
            System.out.println("done");
        }
        else
        {
            String filePath1 = beforeEncode + "non_new_item_test_set.csv";
            System.out.print("working on file "+filePath1+" :");
//            HandleTestTable(filePath1, expName, percent, files[1], "regular", itemsRemoved);
            System.out.println("done");
            String filePath2 = beforeEncode + "new_item_test_set.csv";
            System.out.print("working on file "+filePath2+" :");
            HandleTestTable(filePath2, expName, percent, files[1], "cold_start", itemsRemoved);
            System.out.println("done");
        }
    }

    private static void printFile(String filePath)
    {
        System.out.println("file " + filePath + " is exist:" + Files.exists(Paths.get(filePath)));
    }

    private static void HandlePredictions(String filePath, String expName, String percent, int epoch, String model, String type, File file)
    {
        try (FileWriter fw = new FileWriter(file, true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw))
        {
            try (BufferedReader br = new BufferedReader(new FileReader(filePath)))
            {
                String line = br.readLine();
                int count = 0;
                while (line != null)
                {
                    line = br.readLine();
                    if (line != null && line.length() > 0)
                    {
                        fw.write(expName + ";" + percent + ";" + model + ";" + epoch + ";" + type + ";" + count + ";");
                        fw.write(line.replace(",", ";"));
                        fw.write("\n");
                        count++;
                    }
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        } catch (IOException e)
        {
            //exception handling left as an exercise for the reader
            e.printStackTrace();
        }
    }

    private static List<String> HandleItemsRemove(String filePath, String expName, String percent, File file)
    {
        List<String> itemsRemoved = new ArrayList<String>();
        try (FileWriter fw = new FileWriter(file, true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw))
        {
            try (BufferedReader br = new BufferedReader(new FileReader(filePath)))
            {
                String line = br.readLine();
                int count = 0;
                while (line != null)
                {
                    line = br.readLine();
                    if (line != null && line.length() > 0)
                    {
                        fw.write(expName + ";" + percent + ";");
                        fw.write(line.replace(",", ";"));
                        fw.write("\n");
                        itemsRemoved.add(line.split(",")[1].trim());
                    }
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        } catch (IOException e)
        {
            //exception handling left as an exercise for the reader
            e.printStackTrace();
        }
        return itemsRemoved;
    }

    private static void HandleTestTable(String filePath, String expName, String percent, File file, String type, List<String> itemsRemoved)
    {
        try (FileWriter fw = new FileWriter(file, true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw))
        {

            try (BufferedReader br = new BufferedReader(new FileReader(filePath)))
            {
                String line = br.readLine();
                int count = 0;
                while (line != null)
                {
                    line = br.readLine();
                    if (line != null && line.length() > 0)
                    {
                        int cold_start_items = 0;
                        String[] splitByQuotes = line.split("\"");
                        String[] itemsInSession = splitByQuotes[1]
                                .replace("[", "")
                                .replace("]", "")
                                .replace("'", "")
                                .split(",");
                        for (int i = 0; i < itemsInSession.length; i++)
                        {
                            itemsInSession[i] = itemsInSession[i].trim();
                        }
                        for (String item : itemsInSession)
                        {
                            if (itemsRemoved.contains(item))
                            {
                                cold_start_items += 1;
                            }
                        }
                        fw.write(expName + ";" + percent + ";" + type + ";" + cold_start_items + ";" + count + ";");
                        fw.write(splitByQuotes[0].replace(",", ";"));
                        fw.write(splitByQuotes[1]);
                        fw.write("\n");
                        count++;
                    }
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        } catch (IOException e)
        {
            //exception handling left as an exercise for the reader
            e.printStackTrace();
        }
    }

    private static File[] initHeaders(String outputFolder)
    {
        File[] files = new File[3];
        files[0] = new File(outputFolder + "/item_removed.csv");
        files[1] = new File(outputFolder + "/test.csv");
        files[2] = new File(outputFolder + "/predictions.csv");
        for (int i = 0; i < files.length; i++)
        {
            try
            {
                FileWriter fw = new FileWriter(files[i], false);
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        return files;
    }
}
