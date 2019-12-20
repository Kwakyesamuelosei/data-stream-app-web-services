package io.turntabl.io.datastreamingapp;

public class TweeTO {
    private String screenName;
    private Long tweetCount;

    public TweeTO() {
    }

    public String getScreenName() {
        return screenName;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }

    public Long getTweetCount() {
        return tweetCount;
    }

    public void setTweetCount(Long tweetCount) {
        this.tweetCount = tweetCount;
    }

    @Override
    public String toString() {
        return "TweeTO{" +
                "screenName='" + screenName + '\'' +
                ", tweetCount=" + tweetCount +
                '}';
    }


}
