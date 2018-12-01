package DAL;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class CSVReader implements Iterable<String>
{
    private String m_filename;
    private ReadCSVIterable m_iter = null;

    public CSVReader(String filename)
    {
        this(filename, 1);
    }

    public CSVReader(String filename, int size)
    {
        this(filename, size, 1);
    }

    public CSVReader(String filename, int size, int lowerBound)
    {
        super();
        this.m_filename = filename;
    }

    @Override
    public Iterator<String> iterator()
    {
        m_iter = new ReadCSVIterable();
        return m_iter;
    }

    public void close()
    {
        m_iter.close();
    }

    private class ReadCSVIterable implements Iterator<String>
    {
        private FileReader m_fr = null;
        private BufferedReader m_br = null;
        private String m_nextLine;

        public ReadCSVIterable()
        {
            super();
            File f = new File(CSVReader.this.m_filename);
            try
            {
                m_fr = new FileReader(f);
                m_br = new BufferedReader(m_fr);
                m_nextLine = m_br.readLine();
            } catch (FileNotFoundException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        @Override
        public boolean hasNext()
        {
            // TODO Auto-generated method stub
            return m_nextLine != null;
        }

        @Override
        public String next()
        {
            // TODO Auto-generated method stub
            if (m_nextLine != null)
            {
                String temp = m_nextLine;
                try
                {
                    m_nextLine = m_br.readLine();
                    if (m_nextLine == null)
                    {
                        closeResources();
                    }
                } catch (IOException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    closeResources();
                }
                return temp;
            }
            return null;
        }

        private void closeResources()
        {
            try
            {
                if (m_br != null)
                {
                    m_br.close();
                }
                if (m_fr != null)
                {
                    m_fr.close();
                }
            } catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        public void close()
        {
            closeResources();
            m_nextLine = null;
        }
    }

}