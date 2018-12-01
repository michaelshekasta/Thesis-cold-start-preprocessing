package BL.controllers.output;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;

import BL.dataobjects.actions.Action;
import BL.dataobjects.actions.Action.ActionType;

public class OutputCSV implements IOutput
{

    private String buyFile;
    private String clickFile;
    private String recommendClickFile;
    private String trasnferFile;
    private String basketFile;

    private PrintWriter buyBW;
    private PrintWriter clickBW;
    private PrintWriter rclickBW;
    private PrintWriter transferBW;
    private PrintWriter basketBW;

    public OutputCSV(String buyFile, String clickFile, String recommendClickFile, String trasnferFile)
    {
        super();
        this.buyFile = buyFile;
        this.clickFile = clickFile;
        this.recommendClickFile = recommendClickFile;
        this.trasnferFile = trasnferFile;
        open();

    }

    public OutputCSV(String buyFile, String clickFile, String recommendClickFile, String trasnferFile, String basketFile)
    {
        super();
        this.buyFile = buyFile;
        this.clickFile = clickFile;
        this.recommendClickFile = recommendClickFile;
        this.trasnferFile = trasnferFile;
        this.basketFile = basketFile;
        open();

    }

    private void open()
    {
        try
        {
            buyBW = new PrintWriter(this.buyFile);
            clickBW = new PrintWriter(clickFile);
            rclickBW = new PrintWriter(recommendClickFile);
            transferBW = new PrintWriter(trasnferFile);
//			basketBW = new PrintWriter(basketFile);
//			buyBW.write("timestamp,userId,itemid,price,quantity\n");
//			clickBW.write("timestamp,userId,itemid\n");
//			rclickBW.write("timestamp,userId,itemid\n");
//			transferBW.write("timestamp,userId,newUserId\n");
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void writeAction(Action a)
    {
        // TODO Auto-generated method stub
        if (a == null)
        {
            return;
        }
        if (a.getActionType() == ActionType.buy)
        {
            buyBW.write(a.toCsv() + "\n");
        }
        if (a.getActionType() == ActionType.click)
        {
            clickBW.write(a.toCsv() + "\n");
        }
        if (a.getActionType() == ActionType.clickrecommended)
        {
            rclickBW.write(a.toCsv() + "\n");
        }
        if (a.getActionType() == ActionType.transfer)
        {
            transferBW.write(a.toCsv() + "\n");
        }
        if (a.getActionType() == ActionType.basket)
        {
            basketBW.write(a.toCsv() + "\n");
        }
    }

    @Override
    public void writeBatch(Action a) throws SQLException
    {
        // TODO Auto-generated method stub
        writeAction(a);
    }

    @Override
    public void finishBatch() throws SQLException
    {
        // TODO Auto-generated method stub
        close();
    }

    public void close()
    {
        buyBW.close();
        clickBW.close();
        rclickBW.close();
        transferBW.close();
//		basketBW.close();
    }

}
