package DAL.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FileUtils
{
    public static void writeToFile(String path, String data, boolean append) throws IOException
    {
        File f = new File(path);
        FileWriter fw = new FileWriter(f, append);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(data);
        bw.close();
        fw.close();
    }

    public static void writeToFile(String path, String data) throws IOException
    {
        writeToFile(path, data, false);
    }

    public static String readFromFile(String path) throws IOException
    {
        StringBuilder sb = new StringBuilder();
        File f = new File(path);
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        while (line != null)
        {
            sb.append(line);
            line = br.readLine();
        }
        br.close();
        fr.close();
        return sb.toString();
    }

    public static String getLineInFile(String path, int numOfLine) throws IOException
    {
        File f = new File(path);
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        String ans = null;
        for (int i = 0; i < numOfLine - 2 && line != null; i++)
        {
            line = br.readLine();
        }
        if (line != null)
        {
            ans = br.readLine();
        }
        br.close();
        fr.close();
        return ans;
    }

    public static int[] getLineIncludeText(String path, String text) throws IOException
    {
        ArrayList<Integer> locations = new ArrayList<Integer>();
        File f = new File(path);
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        int numOfline = 1;
        while (line != null)
        {
            if (line.indexOf(text) != -1)
            {
                locations.add(numOfline);
            }
            line = br.readLine();
            numOfline++;
        }
        br.close();
        fr.close();
        int[] ints = locations.stream().mapToInt(i -> i).toArray();
        return ints;
    }

    public static String[] getLinesInFile(String path, int[] numOfLine) throws IOException
    {
        // or if you need a HashSet specifically
        HashSet<Integer> hashset = IntStream.of(numOfLine).boxed().collect(Collectors.toCollection(HashSet::new));
        Arrays.sort(numOfLine);
        File f = new File(path);
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        ArrayList<String> ans = new ArrayList<String>();
        int i = 1;
        while (line != null)
        {
            if (hashset.contains(i))
            {
                ans.add(line);
            }
            i++;
            line = br.readLine();
        }
        br.close();
        fr.close();
        return ans.stream().toArray(String[]::new);
    }

    public static void writeLinesToFile(String path, String[] arr) throws IOException
    {
        for (String string : arr)
        {
            writeToFile(path, string + "\n", true);
        }
    }

    public static String[] getFirstNLinesFromFile(String path, int num) throws IOException
    {
        String[] ans = new String[num];
        File f = new File(path);
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        int numOfline = 0;
        while (line != null && numOfline < num)
        {
            line = br.readLine();
            ans[numOfline++] = line;
        }
        br.close();
        fr.close();
        return ans;
    }
}
