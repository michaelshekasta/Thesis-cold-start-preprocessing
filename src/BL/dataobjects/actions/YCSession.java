package BL.dataobjects.actions;

import java.time.LocalDate;

public class YCSession
{

    public String getUserid()
    {
        return userid;
    }

    public void setUserid(String userid)
    {
        this.userid = userid;
    }

    public LocalDate getDate()
    {
        return date;
    }

    public void setDate(LocalDate date)
    {
        this.date = date;
    }

    private String userid;
    private LocalDate date;

    public YCSession(String userid, LocalDate date)
    {
        super();
        this.userid = userid;
        this.date = date;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((date == null) ? 0 : date.hashCode());
        result = prime * result + ((userid == null) ? 0 : userid.hashCode());
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
        YCSession other = (YCSession) obj;
        if (date == null)
        {
            if (other.date != null)
                return false;
        } else if (!date.equals(other.date))
            return false;
        if (userid == null)
        {
            if (other.userid != null)
                return false;
        } else if (!userid.equals(other.userid))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "YOSession [userid=" + userid + ", date=" + date + "]";
    }

}
