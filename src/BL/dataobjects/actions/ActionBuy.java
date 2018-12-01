package BL.dataobjects.actions;

import java.io.Serializable;
import java.time.LocalDateTime;

import DAL.utils.DateUtils;

public class ActionBuy extends Action implements Serializable
{
    private String itemid;
    private double price;
    private int quantity;

    public ActionBuy(LocalDateTime timestamp, String customerLabel, String userId, String itemid, double price,
                     int quantity)
    {
        super(timestamp, customerLabel, userId, ActionType.buy);
        this.itemid = itemid.trim();
        this.price = price;
        this.quantity = quantity;
    }

    public double getPrice()
    {
        return price;
    }

    public void setPrice(double price)
    {
        this.price = price;
    }

    public int getQuantity()
    {
        return quantity;
    }

    public void setQuantity(int quantity)
    {
        this.quantity = quantity;
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
        String ans = DateUtils.getDateToDB(timestamp) + "," + userId + "," + itemid + "," + price
                + "," + quantity + "," + sessionId;
        return ans;
    }

    @Override
    public String toString()
    {
        return "ActionBuy [itemid=" + itemid + ", price=" + price + ", quantity=" + quantity + ", timestamp="
                + timestamp + ", customerLabel=" + customerLabel + ", userId=" + userId + ", actionType=" + actionType
                + "]";
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
        ActionBuy other = (ActionBuy) obj;
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
        if (Double.doubleToLongBits(price) != Double.doubleToLongBits(other.price))
            return false;
        if (quantity != other.quantity)
            return false;
        return true;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((actionType == null) ? 0 : actionType.hashCode());
        result = prime * result + ((customerLabel == null) ? 0 : customerLabel.hashCode());
        result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
        result = prime * result + ((userId == null) ? 0 : userId.hashCode());
        result = prime * result + ((itemid == null) ? 0 : itemid.hashCode());
        long temp;
        temp = Double.doubleToLongBits(price);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + quantity;
        return result;
    }

}
