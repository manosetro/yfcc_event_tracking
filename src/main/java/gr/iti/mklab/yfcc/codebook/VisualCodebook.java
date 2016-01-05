package gr.iti.mklab.yfcc.codebook;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class VisualCodebook extends Codebook {

    public VisualCodebook() {
        words = new HashMap<String, Word>();
    }

    public Map<String, Word> getWords() {
        return words;
    }
    
    @Override
    public void writeToFile(String filename) {
        try{
            PrintWriter pw = new PrintWriter(new FileWriter(filename));
            Collection<Word> words_collection = words.values();
            Iterator<Word> it_word = words_collection.iterator();
            while(it_word.hasNext()) {
                VisualWord next_word = (VisualWord) it_word.next();
                next_word.writeToFile(pw);
            }
            pw.close();
        }
        catch(IOException e) {
        	
        }
    }

    @Override
    public void loadFromFile(String filename) {
        try {
            BufferedReader reader = null;
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
            VisualWord tmp_word=new VisualWord();
            while(tmp_word.loadFromFile(reader)) {
                words.put(tmp_word.getId(), tmp_word);
                tmp_word=new VisualWord();
            }
        }
        catch(IOException e) {
        	
        }
    }

    public VisualWord match(float[] descriptor) {
        float min_dist = Float.MAX_VALUE;
        
        VisualWord best_word = null;
        Collection<Word> words_col = words.values();
        Iterator<Word> it_word = words_col.iterator();
        while(it_word.hasNext()) {
            VisualWord word=(VisualWord) it_word.next();
            float tmp_dist = ((VisualWord)word).distance(descriptor);
            if(tmp_dist<min_dist) {
                min_dist = tmp_dist;
                best_word = word;
            }
        }
        
        return best_word;
    }
    
    
}
