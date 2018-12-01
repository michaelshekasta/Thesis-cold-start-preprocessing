package BL.controllers.input.raw;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import BL.controllers.input.IInputActions;
import DAL.utils.FileUtils;

public class InputActionFromFileBounded implements IInputActions
{
    private String[] filenames;
    private String nextLine;
    private File f;
    private FileReader fr;
    private BufferedReader br;
    private int currFile;
    private int maxRecords;
    private int currentCount;

    public InputActionFromFileBounded(String[] filenames, int maxRecords) throws IOException
    {
        super();
        this.filenames = filenames;
        initNewFile(0);
        currFile = 0;
        this.maxRecords = maxRecords;
        this.currentCount = 0;
    }

    private void initNewFile(int numFile) throws IOException
    {
        if (filenames.length <= numFile)
        {
            nextLine = null;
        } else
        {
            f = new File(filenames[numFile]);
            fr = new FileReader(f);
            br = new BufferedReader(fr);
            nextLine = br.readLine();
            FileUtils.writeToFile("dup_rows.txt", "new file\n", true);
        }
    }

    @Override
    public Iterator<String> iterator()
    {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public boolean hasNext()
    {
        return nextLine != null;
    }

    @Override
    public String next()
    {
        try
        {
            if (hasNext())
            {
                String currentLine = nextLine;
                nextLine = br.readLine();
                if (nextLine == null)
                {
                    currFile++;
                    initNewFile(currFile);
                }
                currentCount++;
                if (currentCount == maxRecords)
                {
                    currFile++;
                    this.currentCount = 0;
                    initNewFile(currFile);
                }
                return currentLine;
            }
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

}
