package test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import opennlp.tools.lemmatizer.DictionaryLemmatizer;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

public class OpenNLPTest {

    public static void main(String[] args) {

        try{

            //Tokenizer
            InputStream is = new FileInputStream("/home/orkes/Desktop/en-token.bin");

            TokenizerModel model = new TokenizerModel(is);

            Tokenizer tokenizer = new TokenizerME(model);

            String tokens[] = tokenizer.tokenize("Hi. How are you? This is Mike.");


            for (String a : tokens) {
                System.out.println(a);
            }

            is.close();



            // test sentence
            //String[] tokens = new String[]{"Most", "large", "cities", "in", "the", "US", "had",
            //       "morning", "and", "afternoon", "newspapers", "."};

            // Parts-Of-Speech Tagging
            // reading parts-of-speech model to a stream
            InputStream posModelIn = new FileInputStream("/home/orkes/Desktop/en-pos-maxent.bin");
            // loading the parts-of-speech model from stream
            POSModel posModel = new POSModel(posModelIn);
            // initializing the parts-of-speech tagger with model
            POSTaggerME posTagger = new POSTaggerME(posModel);
            // Tagger tagging the tokens
            String tags[] = posTagger.tag(tokens);

            // loading the dictionary to input stream
            InputStream dictLemmatizer = new FileInputStream("/home/orkes/Desktop/en-lemmatizer.dict.txt");
            // loading the lemmatizer with dictionary
            DictionaryLemmatizer lemmatizer = new DictionaryLemmatizer(dictLemmatizer);

            // finding the lemmas
            String[] lemmas = lemmatizer.lemmatize(tokens, tags);

            // printing the results
            System.out.println("\nPrinting lemmas for the given sentence...");
            System.out.println("WORD -POSTAG : LEMMA");

            for(int i=0;i< tokens.length;i++){
                System.out.println(tokens[i] + " -" + tags[i] + " : " + lemmas[i]);
            }

        } catch (FileNotFoundException e){
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private File getFile(String fileName) {

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());
        return file;
    }

}
