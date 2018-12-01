package BL.controllers.preprocessing;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

import BL.controllers.input.IInputActions;
import BL.controllers.output.IOutput;
import BL.controllers.parser.IParseActions;
import BL.dataobjects.actions.Action;
import DAL.utils.FileUtils;

public class YCReaderBatchNoDup implements IYCReader
{

    private IInputActions input;
    private IOutput output;
    private IParseActions parser;

    public YCReaderBatchNoDup(IInputActions input, IOutput output, IParseActions parser)
    {
        super();
        this.input = input;
        this.output = output;
        this.parser = parser;
    }

    public void parse()
    {
        try
        {
            FileUtils.writeToFile("dup.txt", "");
        } catch (IOException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        for (String record : input)
        {
            Action a = parser.getAction(record);
            if (a == null)
            {
                continue;
            }
            try
            {
                output.writeBatch(a);
            } catch (SQLException e)
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
