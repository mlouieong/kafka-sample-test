package model;

public class PageViewEvent {

    public String itemId;
    public String itemTitle;
    public int scrollRange;
    public long stayTerm;
    public int scrollUpDownCount;

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getItemTitle() {
        return itemTitle;
    }

    public void setItemTitle(String itemTitle) {
        this.itemTitle = itemTitle;
    }

    public int getScrollRange() {
        return scrollRange;
    }

    public void setScrollRange(int scrollRange) {
        this.scrollRange = scrollRange;
    }

    public long getStayTerm() {
        return stayTerm;
    }

    public void setStayTerm(long stayTerm) {
        this.stayTerm = stayTerm;
    }

    public int getScrollUpDownCount() {
        return scrollUpDownCount;
    }

    public void setScrollUpDownCount(int scrollUpDownCount) {
        this.scrollUpDownCount = scrollUpDownCount;
    }

    @Override
    public String toString() {
        return "PageViewEvent{" +
                "itemId='" + itemId + '\'' +
                ", itemTitle='" + itemTitle + '\'' +
                ", scrollRange=" + scrollRange +
                ", stayTerm=" + stayTerm +
                ", scrollUpDownCount=" + scrollUpDownCount +
                '}';
    }
}
