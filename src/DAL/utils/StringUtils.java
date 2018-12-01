package DAL.utils;

public class StringUtils
{
    public static void printLines(String line, String... str)
    {
        for (String string : str)
        {
            printLine(line, string);
        }
    }

    public static void printLine(String line, String str)
    {
        if (line.indexOf(str) != -1)
        {
            System.out.println(line);
        }
    }
}
