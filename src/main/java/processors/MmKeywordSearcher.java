package processors;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

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
import org.apache.nifi.processor.util.StandardValidators;

/**
 * A Lucene-based keyword searcher class.
 */
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"MM", "keyword", "search"})
@CapabilityDescription("This processor searches for keywords/phrases in tweets.")
public class MmKeywordSearcher extends AbstractProcessor {

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
                    "Specifies comma-separated key words to search for.")
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

        String keywordString = aContext.getProperty(KEYWORDS).getValue();
        List<String> keywords = new ArrayList<String>();
        if (!keywordString.trim().isEmpty()) {
            keywords = Arrays.asList(keywordString.split(","));
        }

        String userId = flowFile.getAttribute("UserId");
        String text = flowFile.getAttribute("Text");
        if (!text.isEmpty() && !userId.isEmpty()) {
            // 0. Specify the analyzer for tokenizing text.
            // The same analyzer should be used for indexing and searching
            StandardAnalyzer analyzer = new StandardAnalyzer();

            // 1. create the index
            Directory index = new RAMDirectory();

            IndexWriterConfig config = new IndexWriterConfig(analyzer);

            try {
                IndexWriter w = new IndexWriter(index, config);
                Document doc = new Document();
                doc.add(new TextField("Text", text, Field.Store.YES));

                // use a string field for userId because we don't want it
                // tokenized
                doc.add(new StringField("UserId", userId, Field.Store.YES));
                w.addDocument(doc);
                w.close();

                // 2. query
                boolean hit = false;
                for (String keyword : keywords) {

                    // the "Text" arg specifies the default field to use
                    // when no field is explicitly specified in the query.
                    Query q = new QueryParser("Text", analyzer)
                            .parse("'" + keyword + "'");

                    // 3. search
                    int hitsPerPage = 10;
                    IndexReader reader = DirectoryReader.open(index);

                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs docs = searcher.search(q, hitsPerPage);

                    if (docs.totalHits > 0) {
                        // getLogger().info("Hits found!");
                        hit = true;
                    }
                    // reader can only be closed when there
                    // is no need to access the documents any more.
                    reader.close();
                }
                if (hit) {
                    aSession.transfer(flowFile, REL_SUCCESS);
                    getLogger()
                            .info("Flowfile with a keyword is sent forward!");
                } else {
                    aSession.remove(flowFile);
                    getLogger().info("No keyword found!");
                }
                aSession.commit();

            } catch (IOException | ParseException e) {
                getLogger().error(e.getMessage());
            }
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

}
