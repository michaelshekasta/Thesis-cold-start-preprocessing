package BL.dataobjects.actions;

import java.time.LocalDateTime;

import DAL.utils.DateUtils;

public class ActionTransfer extends Action
{

    private String newUserId;

    public ActionTransfer(LocalDateTime timestamp, String customerLabel, String customerId, String newUserId)
    {
        super(timestamp, customerLabel, customerId, ActionType.transfer);
        this.newUserId = newUserId.trim();
    }

    public String getNewUserId()
    {
        return newUserId;
    }

    public void setNewUserId(String newUserId)
    {
        this.newUserId = newUserId;
    }

    @Override
    public String toCsv()
    {
        String ans = DateUtils.getDateToDB(timestamp) + "," + userId + "," + newUserId;
        return ans;
    }

    @Override
    public String toString()
    {
        return "ActionTransfer [newUserId=" + newUserId + ", timestamp=" + timestamp + ", customerLabel="
                + customerLabel + ", userId=" + userId + ", actionType=" + actionType + "]";
    }

}
