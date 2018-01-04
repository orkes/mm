package processors;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * A keyword matcher class.
 */
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"MM", "keyword", "match"})
@CapabilityDescription("This processor matches keywords in tweets.")
public class MmKeywordMatcher extends AbstractProcessor {

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

    /** Processor property. */
    public static final PropertyDescriptor KEYWORDS =
            new PropertyDescriptor.Builder().name("Key words").description(
                    "Specifies comma-separated key words to be matched.")
                    .defaultValue("test").required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

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
        supDescriptors.add(KEYWORDS);
        setProperties(Collections.unmodifiableList(supDescriptors));

        getLogger().info("Initialisation complete!");

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTrigger(final ProcessContext aContext,
            final ProcessSession aSession) throws ProcessException {

        FlowFile flowFile = aSession.get();
        if (null == flowFile) {
            return;
        }

        aSession.read(flowFile, new InputStreamCallback() {

            @Override
            public void process(final InputStream aStream) throws IOException {

                String keywordString =
                        aContext.getProperty(KEYWORDS).getValue();
                List<String> keywords = new ArrayList<String>();
                if (!keywordString.trim().isEmpty()) {
                    keywords = Arrays.asList(keywordString.split(","));
                }

                String message = new String(IOUtils.toByteArray(aStream));
                getLogger().info("Received: " + message);

                Gson gson = new Gson();
                String[] lemmas = gson.fromJson(message, String[].class);

                for (String lemma : lemmas) {
                    if (keywords.contains(lemma)) {
                        getLogger().warn("KEYWORD MATCHED!!!");
                        FlowFile result = aSession.create();
                        result = aSession.write(result,
                                new OutputStreamCallback() {

                                    @Override
                                    public void process(
                                            final OutputStream aStream)
                                            throws IOException {
                                        getLogger().info(
                                                "Sent a tweet with a keyword: "
                                                        + message);
                                        aStream.write(message.getBytes());
                                    }
                                });
                        aSession.transfer(result, REL_SUCCESS);
                        break;
                    }
                }
            }
        });

        aSession.remove(flowFile);
        aSession.commit();
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
