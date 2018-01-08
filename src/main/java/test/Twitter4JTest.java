package test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class Twitter4JTest {

    public static void main(String[] args)
            throws TwitterException, IOException {

        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                System.out.println(
                        status.getUser().getName() + " : " + status.getText());

            }

            @Override
            public void onDeletionNotice(
                    StatusDeletionNotice statusDeletionNotice) {}

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }

            @Override
            public void onScrubGeo(long aArg0, long aArg1) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onStallWarning(StallWarning aArg0) {
                // TODO Auto-generated method stub

            }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("w005HES0qRC80dSzISfcXWuYA")
                .setOAuthConsumerSecret(
                        "WWmbn1USHfGUbFYqGEoP1Zo771MZT8YQD9aYhCCQW2i5uEpjmJ")
                .setOAuthAccessToken(
                        "119367092-XTMgigkWeuTOrnP7N4WkKl3jsZtbuu5o7woFerpJ")
                .setOAuthAccessTokenSecret(
                        "mDeunQONPSMhqYwGcLdYZZiTq28TorWFNCeXK8ZNJzExh");

        FilterQuery query = new FilterQuery();

        List<String> ids = Arrays.asList(
                "119367092,69779983,1429602151,2904107388,69779983,1429602151,2904107388"
                        .replaceAll("(\\r|\\n|\\r\\n)", "").split(","));

        List<Long> longIds =
                ids.stream().map(Long::parseLong).collect(Collectors.toList());

        long[] followings = longIds.stream().mapToLong(i -> i).toArray();

        // long[] followings = {1L, 119367092L};
        query.follow(followings);

        TwitterStream twitterStream =
                new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(listener);

        twitterStream.filter(query);
        // twitterStream.sample();
    }

}
