package BL.dataobjects.actions;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

import BL.dataobjects.Tuple;

public abstract class Action implements Serializable
{

    public enum ActionType
    {
        click, basket, clickrecommended, buy, rendered, transfer, consume,
    }

    protected LocalDateTime timestamp;
    protected String customerLabel;
    protected String userId;
    protected ActionType actionType;
    protected String sessionId;

    public Action(LocalDateTime timestamp, String customerLabel, String userId, ActionType actionType)
    {
        this(timestamp, customerLabel, userId, actionType, "-1");
    }


    public Action(LocalDateTime timestamp, String customerLabel, String userId, ActionType actionType, String sessionId)
    {
        super();
        this.timestamp = timestamp;
        this.customerLabel = customerLabel;
        this.userId = userId.trim();
        this.actionType = actionType;
        this.sessionId = sessionId;
    }


    public LocalDateTime getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp)
    {
        this.timestamp = timestamp;
    }

    public String getCustomerLabel()
    {
        return customerLabel;
    }

    public void setCustomerLabel(String customerLabel)
    {
        this.customerLabel = customerLabel;
    }

    public String getUserID()
    {
        return userId;
    }

    public void setUserID(String userId)
    {
        this.userId = userId;
    }

    public ActionType getActionType()
    {
        return actionType;
    }

    public void setActionType(ActionType actionType)
    {
        this.actionType = actionType;
    }

    @Override
    public String toString()
    {
        return "Action [timestamp=" + timestamp + ", customerLabel=" + customerLabel + ", userId=" + userId
                + ", actionType=" + actionType + "]";
    }

    public List<Tuple> getMapFields()
    {
        List<Tuple> ans = null;
        // TODO: complete it on every action
        // implements OutputDB
        return ans;
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
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Action other = (Action) obj;
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
        return true;
    }

    public abstract String toCsv();

    public String getSessionId()
    {
        return sessionId;
    }

    public void setSessionId(String sessionId)
    {
        this.sessionId = sessionId;
    }


}