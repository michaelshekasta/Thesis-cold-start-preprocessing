package BL.dataobjects.actions;

import java.time.LocalDateTime;

import DAL.utils.DateUtils;

public class ActionClick extends Action
{

    private String itemid;

    public ActionClick(LocalDateTime timestamp, String customerLabel, String customerId, String itemid)
    {
        super(timestamp, customerLabel, customerId, ActionType.click);
        this.itemid = itemid.trim();
    }

    public String getItemid()
    {
        return itemid;
    }

    public void setItemid(String itemid)
    {
        this.itemid = itemid;
    }

    @Override
    public String toCsv()
    {
        int type = 1;
        if (this.actionType == ActionType.click)
        {
            type = 0;
        }
        String ans = DateUtils.getDateToDB(timestamp) + "," + userId + "," + itemid + "," + type + "," + this.sessionId;
        return ans;
    }

    @Override
    public String toString()
    {
        return "ActionClick [itemid=" + itemid + ", timestamp=" + timestamp + ", customerLabel=" + customerLabel
                + ", userId=" + userId + ", actionType=" + actionType + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((actionType == null) ? 0 : actionType.hashCode());
        result = prime * result + ((customerLabel == null) ? 0 : customerLabel.hashCode());
        result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
        result = prime * result + ((userId == null) ? 0 : userId.hashCode());
        result = prime * result + ((itemid == null) ? 0 : itemid.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        ActionClick other = (ActionClick) obj;
        if (actionType != other.actionType)
            return false;
        if (customerLabel == null)
        {
            if (other.customerLabel != null)
                return false;
        } else if (!customerLabel.equals(other.customerLabel))
            return false;
        if (timestamp == null)
        {
            if (other.timestamp != null)
                return false;
        } else if (!timestamp.equals(other.timestamp))
            return false;
        if (userId == null)
        {
            if (other.userId != null)
                return false;
        } else if (!userId.equals(other.userId))
            return false;

        if (itemid == null)
        {
            if (other.itemid != null)
                return false;
        } else if (!itemid.equals(other.itemid))
            return false;
        return true;
    }

}
