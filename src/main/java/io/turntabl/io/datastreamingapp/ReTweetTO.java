package io.turntabl.io.datastreamingapp;

public class ReTweetTO {
    private String screenName;
    private Long retweetCount;

    public ReTweetTO() {
    }

    public String getScreenName() {
        return screenName;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }

    public Long getRetweetCount() {
        return retweetCount;
    }

    public void setRetweetCount(Long retweetCount) {
        this.retweetCount = retweetCount;
    }

    @Override
    public String toString() {
        return "ReTweetTO{" +
                "screenName='" + screenName + '\'' +
                ", retweetCount=" + retweetCount +
                '}';
    }
}
