package gr.iti.mklab.yfcc.codebook;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

public class TextualWord extends Word {
    
    public TextualWord(String _id) {
        id = _id;
        df = 0;
        idf = 0;
    }

    public TextualWord() {
        df = 0;
        idf = 0;
    }
    
    
    public void writeToFile(PrintWriter pw) {
    	pw.print(id);
        pw.print(" ");
        pw.print(df);
        pw.print(" ");
        pw.println(idf);
    }

    public boolean loadFromFile(BufferedReader reader) {
        try {
            String line = reader.readLine();
            if((line == null) || (line.trim().equals(""))) 
            	return false;
            
            String[] parts = line.split(" ");
            id = parts[0];
            df = Integer.parseInt(parts[1]);
            idf = Double.parseDouble(parts[2]);
            return true;
        }
        catch(IOException e) {
            
        }
        
        return false;
    }
    
}
