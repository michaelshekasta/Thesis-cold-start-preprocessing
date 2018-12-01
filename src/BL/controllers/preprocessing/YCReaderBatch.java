package BL.controllers.preprocessing;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import BL.controllers.input.IInputActions;
import BL.controllers.output.IOutput;
import BL.controllers.parser.IParseActions;
import BL.dataobjects.actions.Action;
import DAL.utils.FileUtils;

public class YCReaderBatch implements IYCReader
{

    private IInputActions input;
    private IOutput output;
    private IParseActions parser;

    public YCReaderBatch(IInputActions input, IOutput output, IParseActions parser)
    {
        super();
        this.input = input;
        this.output = output;
        this.parser = parser;
    }

    public void parse()
    {
        for (String record : input)
        {
            try
            {
                Action a = parser.getAction(record);
                if (a != null)
                {
                    try
                    {
                        output.writeBatch(a);
                    } catch (SQLException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                } else
                {
                    FileUtils.writeToFile("waste events.txt", record, true);
                }
            } catch (RuntimeException e)
            {
                System.out.println("line that do problem:" + record);
                e.printStackTrace();
                throw e;
            } catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        try
        {
            output.finishBatch();
        } catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
