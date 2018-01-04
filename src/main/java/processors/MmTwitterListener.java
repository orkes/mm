package processors;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

/**
 * Custom listener for receiving tweets.
 */
public class MmTwitterListener implements StatusListener {

    @Override
    public void onStatus(Status status) {
        System.out
                .println(status.getUser().getName() + " : " + status.getText());

    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

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

}
