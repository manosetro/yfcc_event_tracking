package gr.iti.mklab.yfcc.codebook;

import java.io.*;
import java.util.*;

public class TextualCodebook extends Codebook {

    public int getSize() {
        return words.size();
    }
    
    public TextualCodebook() {
        words = new HashMap<String, Word>();
    }

    @Override
    public void writeToFile(String filename) {
        System.out.println("Writing textual codebook to " + filename);
        try {
            PrintWriter pw = new PrintWriter(new FileWriter(filename));
            Collection<Word> words_collection=words.values();
            Iterator<Word> it_word=words_collection.iterator();
            pw.println(nDocs);
            while(it_word.hasNext()) {
                TextualWord next_word = (TextualWord) it_word.next();
                next_word.writeToFile(pw);
            }
            pw.close();
        }
        catch(IOException e) {
        	
        }
    }

    @Override
    public void loadFromFile(String filename) {
        System.out.println("Loading textual codebook from " + filename);
        try{
            BufferedReader reader = null;
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
            String line=reader.readLine();
            nDocs=Integer.parseInt(line);
            TextualWord tmp_word=new TextualWord();
            while(tmp_word.loadFromFile(reader)) {
                words.put(tmp_word.getId(), tmp_word);
                tmp_word=new TextualWord();
            }
        }
        catch(IOException e) {
        	
        }
    }
    
    public void addWord(TextualWord tmp_word) {
        words.put(tmp_word.id, tmp_word);
    }

    public void increaseNDocs() {
        nDocs++;
    }

    
}
