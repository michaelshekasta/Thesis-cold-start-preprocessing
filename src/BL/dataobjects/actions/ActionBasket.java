package BL.dataobjects.actions;

import java.time.LocalDateTime;

import DAL.utils.DateUtils;

public class ActionBasket extends Action
{
    private String itemid;


    public ActionBasket(LocalDateTime timestamp, String customerLabel, String userId, String itemid,
                        String sessionId)
    {
        super(timestamp, customerLabel, userId, ActionType.basket, sessionId);
        this.itemid = itemid.trim();
    }

    public ActionBasket(LocalDateTime timestamp, String customerLabel, String userId, String itemid)
    {
        this(timestamp, customerLabel, userId, itemid, "-1");
        // TODO Auto-generated constructor stub
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
        String ans = DateUtils.getDateToDB(timestamp) + "," + userId + "," + itemid + "," + this.sessionId;
        return ans;
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
        ActionBasket other = (ActionBasket) obj;
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
