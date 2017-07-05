package com.einext;

import twitter4j.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Stream {

    private static Logger logger = Logger.getLogger(Stream.class);

    private static Twitter twitter = TwitterFactory.getSingleton();


    public static void main(String[] args) throws Exception {


        boolean skipKafka = Arrays.asList(args).contains("skip-kafka");
        System.out.println("skip-kafka: " + skipKafka);
        AppStatusListener.setSkipKafka(skipKafka);


        System.out.println("Twitter screen name of current user: " + twitter.getScreenName());

        long[] friends = twitter.friendsFollowers().getFriendsIDs(twitter.getId(), -1).getIDs();

        System.out.println("No of friends: " + friends.length);

        List<String> terms = new ArrayList<String>();

        File f = new File("terms.txt");
        if(f.isFile() && f.canRead()){
            System.out.println("Tracking terms found in " + f.getAbsolutePath());
            BufferedReader br = new BufferedReader(new FileReader(f.getAbsolutePath()));
            String term;
            while((term = br.readLine()) != null){
                terms.add(term);
                System.out.println(term);
            }
        }

        if(terms.size() == 0){
            System.out.println("Specify the terms to track in terms.txt file");
            System.exit(0);
        }

        String[] searchTerms = terms.toArray(new String[0]);

        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(new AppStatusListener());

        FilterQuery filterQuery = new FilterQuery();
        //filterQuery.follow(friends);
        filterQuery.language("en");
        filterQuery.track(searchTerms);
        twitterStream.filter(filterQuery);

        logger.info("Simple message");

    }
}
