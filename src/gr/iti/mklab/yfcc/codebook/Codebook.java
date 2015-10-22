package gr.iti.mklab.yfcc.codebook;

import java.util.Collection;
import java.util.Map;


public abstract class Codebook {
    
    protected Map<String, Word> words;
    protected int nDocs;

    public void computeIDFs() {
        Collection<Word> tmp_words = words.values();
        for(Word tmp_word : tmp_words)
            tmp_word.computeIDF(nDocs);
    }
    
    public Word getWord(String wordId) {
        return words.get(wordId);
    }
    
    /*
     * Writes the codebook to a file. Each line contains the data for a single word
     */
    public abstract void writeToFile(String filename);
    
    public abstract void loadFromFile(String filename);

    
}
