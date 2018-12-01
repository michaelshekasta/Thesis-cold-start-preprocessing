package BL.controllers.input.raw;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import BL.controllers.input.IInputActions;
import DAL.utils.FileUtils;

public class InputActionFromFile implements IInputActions
{
    private String[] filenames;
    private String nextLine;
    private File f;
    private FileReader fr;
    private BufferedReader br;
    private int currFile;

    public InputActionFromFile(String[] filenames) throws IOException
    {
        super();
        this.filenames = filenames;
        System.out.println("debugging - start reading all file " + Arrays.toString(filenames));
        initNewFile(0);
        currFile = 0;
    }

    private void initNewFile(int numFile) throws IOException
    {
        if (filenames.length <= numFile)
        {
            System.out.println("debugging - end to read with numFile=" + numFile);
            nextLine = null;
        } else
        {
            f = new File(filenames[numFile]);
            System.out.println("debugging - starting using file " + filenames[numFile] + " numFile=" + numFile);
            fr = new FileReader(f);
            br = new BufferedReader(fr);
            nextLine = br.readLine();
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
