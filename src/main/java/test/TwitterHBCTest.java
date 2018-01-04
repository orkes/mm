package test;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

// import twitter4j.JSONException;
// import twitter4j.JSONObject;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class TwitterHBCTest {

    public static void main(String[] args) throws ParseException {
        try {
            TwitterHBCTest.run();
        } catch (InterruptedException e) {
            System.out.println(e);
        }

    }

    public static void run() throws InterruptedException, ParseException {

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        // endpoint.followings(Lists.newArrayList(119367092L, 1L));
        endpoint.trackTerms(Lists.newArrayList("news", "text"));

        Authentication auth = new OAuth1("w005HES0qRC80dSzISfcXWuYA",
                "WWmbn1USHfGUbFYqGEoP1Zo771MZT8YQD9aYhCCQW2i5uEpjmJ",
                "119367092-XTMgigkWeuTOrnP7N4WkKl3jsZtbuu5o7woFerpJ",
                "mDeunQONPSMhqYwGcLdYZZiTq28TorWFNCeXK8ZNJzExh");
        // Authentication auth = new BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();

        // Establish a connection
        client.connect();
        System.out.println("connection establiched!");

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            String msg = queue.take();
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(msg);
            // get a String from the JSON object

            String text = (String) jsonObject.get("user.name");
            System.out.println(text);
        }

        client.stop();

    }

}
