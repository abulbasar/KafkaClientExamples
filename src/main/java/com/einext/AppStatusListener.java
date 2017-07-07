package com.einext;

import org.apache.log4j.Logger;
import twitter4j.*;

public class AppStatusListener implements StatusListener {

    private Logger logger = Logger.getLogger(AppStatusListener.class);
    private static boolean skipKafka = false;

    public static void setSkipKafka(boolean value){
        skipKafka = value;
    }

    public void onStatus(Status status) {
        System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
        String rawJson = TwitterObjectFactory.getRawJSON(status);
        logger.info(rawJson);
        KafkaSource.send(null, rawJson);
        if(!skipKafka) {

        }
    }

    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        //System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
    }

    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        //System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
    }

    public void onScrubGeo(long userId, long upToStatusId) {
        //System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
    }

    public void onStallWarning(StallWarning warning) {
        //System.out.println("Got stall warning:" + warning);
    }

    public void onException(Exception ex) {
        //ex.printStackTrace();
    }
}