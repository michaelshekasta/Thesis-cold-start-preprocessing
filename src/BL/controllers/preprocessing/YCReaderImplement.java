package BL.controllers.preprocessing;

import java.sql.SQLException;

import BL.controllers.input.IInputActions;
import BL.controllers.output.IOutput;
import BL.controllers.parser.IParseActions;
import BL.dataobjects.actions.Action;

public class YCReaderImplement implements IYCReader
{

    private IInputActions input;
    private IOutput output;
    private IParseActions parser;

    public YCReaderImplement(IInputActions input, IOutput output, IParseActions parser)
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
            Action a = parser.getAction(record);
            if (a != null)
            {
                try
                {
                    output.writeAction(a);
                } catch (SQLException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

}
