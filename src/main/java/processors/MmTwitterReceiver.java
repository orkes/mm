package processors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.buffer.CircularFifoBuffer;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * A processor for retrieving tweets from specific users.
 */
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"twitter", "retrieve", "source", "MM"})
@CapabilityDescription("This processor retrieves tweets from specific users.")
@TriggerSerially
public class MmTwitterReceiver extends AbstractProcessor {

    /** Processor property. */
    public static final PropertyDescriptor USER_IDS =
            new PropertyDescriptor.Builder().name("Twitter user IDs")
                    .description(
                            "Specifies comma-separated relevant Twitter users/accounts.")
                    .defaultValue("119367092").required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    /** Relationship "Success". */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description(
                    "This is where flow files are sent if the processor execution went well.")
            .build();

    /** List of processor properties. */
    private List<PropertyDescriptor> properties;

    /** List of processor relationships. */
    private Set<Relationship> relationships;

    /**
     * Twitter stream.
     */
    private TwitterStream twitterStream;

    /**
     * A circular buffer to keep of track status IDs and omit duplicates.
     */
    private CircularFifoBuffer buffer;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void init(final ProcessorInitializationContext context) {

        final Set<Relationship> procRels = new HashSet<Relationship>();
        procRels.add(REL_SUCCESS);
        setRelationships(Collections.unmodifiableSet(procRels));

        final List<PropertyDescriptor> supDescriptors =
                new ArrayList<PropertyDescriptor>();
        supDescriptors.add(USER_IDS);
        setProperties(Collections.unmodifiableList(supDescriptors));

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("w005HES0qRC80dSzISfcXWuYA")
                .setOAuthConsumerSecret(
                        "WWmbn1USHfGUbFYqGEoP1Zo771MZT8YQD9aYhCCQW2i5uEpjmJ")
                .setOAuthAccessToken(
                        "119367092-XTMgigkWeuTOrnP7N4WkKl3jsZtbuu5o7woFerpJ")
                .setOAuthAccessTokenSecret(
                        "mDeunQONPSMhqYwGcLdYZZiTq28TorWFNCeXK8ZNJzExh");

        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        buffer = new CircularFifoBuffer(100);

        getLogger()
                .info(this.getClass().getName() + ": Initialisation complete!");

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTrigger(final ProcessContext aContext,
            final ProcessSession aSession) throws ProcessException {

        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status aStatus) {

                if (filterStatus(aStatus)) {

                    buffer.add(aStatus.getId());

                    FlowFile flowFile = aSession.create();

                    // parse Twitter status into a flowfile
                    aSession.putAttribute(flowFile, "Text", aStatus.getText());
                    aSession.putAttribute(flowFile, "UserId",
                            String.valueOf(aStatus.getUser().getId()));
                    aSession.putAttribute(flowFile, "UserName",
                            aStatus.getUser().getName());
                    aSession.putAttribute(flowFile, "UserScreenName",
                            aStatus.getUser().getScreenName());
                    aSession.putAttribute(flowFile, "UserURL",
                            aStatus.getUser().getURL());
                    aSession.putAttribute(flowFile, "StatusURL",
                            "https://twitter.com/"
                                    + aStatus.getUser().getScreenName()
                                    + "/status/" + aStatus.getId());
                    aSession.putAttribute(flowFile, "FullStatus",
                            aStatus.toString());

                    aSession.transfer(flowFile, REL_SUCCESS);
                    aSession.commit();
                    getLogger().info("Received and sent forward a status: "
                            + aStatus.toString());
                }
            }

            @Override
            public void onDeletionNotice(
                    StatusDeletionNotice statusDeletionNotice) {}

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

            @Override
            public void onException(Exception ex) {
                getLogger().error(ex.getMessage());
            }

            @Override
            public void onScrubGeo(long aArg0, long aArg1) {}

            @Override
            public void onStallWarning(StallWarning aArg0) {}
        };
        twitterStream.addListener(listener);

        String stringIds = aContext.getProperty(USER_IDS).getValue();
        if (!stringIds.replaceAll("(\\r|\\n|\\r\\n)", "").trim().isEmpty()) {

            List<String> ids = Arrays.asList(stringIds.split(","));
            List<Long> longIds = ids.stream().map(Long::parseLong)
                    .collect(Collectors.toList());
            long[] followings = longIds.stream().mapToLong(i -> i).toArray();
            FilterQuery query = new FilterQuery();
            query.follow(followings);

            twitterStream.filter(query);
            // twitterStream.sample();
        }
    }

    /**
     * Filters out the noice - i.e. retweets, replies, quotes, etc.
     * 
     * @param aStatus incoming tweet
     * @return true - if it is a relevant primary tweet, otherwise - false
     */
    public boolean filterStatus(Status aStatus) {

        if (aStatus.getInReplyToStatusId() > 0
                || aStatus.getInReplyToUserId() > 0
                || aStatus.getInReplyToScreenName() != null
                || aStatus.getQuotedStatus() != null
                || aStatus.getQuotedStatusId() > 0 || aStatus.isRetweet()
                || buffer.contains(aStatus.getId())) {
            getLogger().info("Ignoring 'noisy' tweet: "
                    + aStatus.getUser().getScreenName() + " - "
                    + aStatus.getText());
            return false;
        }
        return true;
    }

    /**
     * Any cleanup code appears here.
     */
    @OnUnscheduled
    public void cleanUp() {
        twitterStream.cleanUp();
        twitterStream.shutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * Setter.
     *
     * @param aRelationships relationships
     */
    public void setRelationships(final Set<Relationship> aRelationships) {
        relationships = aRelationships;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * Getter.
     *
     * @return properties
     */
    public List<PropertyDescriptor> getProperties() {
        return properties;
    }

    /**
     * Setter.
     *
     * @param aProperties properties
     */
    public void setProperties(final List<PropertyDescriptor> aProperties) {
        properties = aProperties;
    }

}
