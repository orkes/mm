package processors;

import com.google.gson.Gson;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import opennlp.tools.lemmatizer.DictionaryLemmatizer;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

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

/**
 * A tokenizer/lemmatizer class.
 */
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"MM", "tokenize"})
@CapabilityDescription("This processor lemmatizes tweets.")
public class MmTokenizerLemmatizer extends AbstractProcessor {

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

    /** Tokenization model. */
    private Tokenizer tokenizer;

    /** POS tagging model. */
    private POSTaggerME posTagger;

    /** Lemmatization model. */
    private DictionaryLemmatizer lemmatizer;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void init(final ProcessorInitializationContext context) {

        final Set<Relationship> procRels = new HashSet<Relationship>();
        procRels.add(REL_SUCCESS);
        setRelationships(Collections.unmodifiableSet(procRels));

        File tokenModelFile = getFile("en-token.bin");
        File lemmaModelFile = getFile("en-lemmatizer.txt");
        File posModelFile = getFile("en-pos-maxent.bin");
        try {
            // load the tokenization model
            InputStream is = new FileInputStream(tokenModelFile);
            TokenizerModel model = new TokenizerModel(is);
            tokenizer = new TokenizerME(model);
            is.close();

            // load the POS tagging model
            is = new FileInputStream(posModelFile);
            POSModel posModel = new POSModel(is);
            // initializing the parts-of-speech tagger with model
            posTagger = new POSTaggerME(posModel);

            // load the lemmatization model
            is = new FileInputStream(lemmaModelFile);
            lemmatizer = new DictionaryLemmatizer(is);
            is.close();

        } catch (FileNotFoundException e) {
            getLogger().error(e.getMessage());
        } catch (IOException e) {
            getLogger().error(e.getMessage());
        }

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

        String text = flowFile.getAttribute("Text");
        if (!text.isEmpty()) {

            getLogger().info("Received string: " + text);
            String[] tokens = tokenizer.tokenize(text);
            for (String token : tokens) {
                getLogger().info("Token: " + token);
            }

            // tag POSs
            String[] tags = posTagger.tag(tokens);
            for (String tag : tags) {
                getLogger().info("Tag: " + tag);
            }

            // lemmatize
            String[] lemmas = lemmatizer.lemmatize(tokens, tags);
            for (String lemma : lemmas) {
                getLogger().info("Lemma: " + lemma);
            }

            Gson gson = new Gson();
            String jsonLemmas = gson.toJson(lemmas);

            getLogger().info(jsonLemmas);

            aSession.putAttribute(flowFile, "Lemmas", jsonLemmas);

            //
            // FlowFile result = aSession.create(flowFile);
            // // result = aSession.putAttribute(result, "parent",
            // // flowFile.getAttribute(CoreAttributes.UUID.key()));
            // result = aSession.write(result, new OutputStreamCallback() {
            //
            // @Override
            // public void process(final OutputStream aStream)
            // throws IOException {
            // aStream.write(jsonLemmas.getBytes());
            // }
            // });
            // // result = aSession.putAttribute(result,
            // // "received-for-detection",
            // // timestamp);
            aSession.transfer(flowFile, REL_SUCCESS);

            // aSession.read(flowFile, new InputStreamCallback() {
            //
            // @Override
            // public void process(final InputStream aStream) throws IOException
            // {
            //
            // // tokenize
            // String message = new String(IOUtils.toByteArray(aStream));
            // String[] tokens = tokenizer.tokenize(message);
            //
            // // tag POSs
            // String[] tags = posTagger.tag(tokens);
            //
            // // lemmatize
            // String[] lemmas = lemmatizer.lemmatize(tokens, tags);
            //
            // Gson gson = new Gson();
            // String jsonLemmas = gson.toJson(lemmas);
            //
            // getLogger().info(jsonLemmas);
            //
            // FlowFile result = aSession.create(flowFile);
            // // result = aSession.putAttribute(result, "parent",
            // // flowFile.getAttribute(CoreAttributes.UUID.key()));
            // result = aSession.write(result, new OutputStreamCallback() {
            //
            // @Override
            // public void process(final OutputStream aStream)
            // throws IOException {
            // aStream.write(jsonLemmas.getBytes());
            // }
            // });
            // // result = aSession.putAttribute(result,
            // // "received-for-detection",
            // // timestamp);
            // aSession.transfer(result, REL_SUCCESS);
            //
            // }
            // });

            aSession.commit();
            // aSession.remove(flowFile);
        }
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

    private File getFile(String fileName) {

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());
        return file;
    }

}
