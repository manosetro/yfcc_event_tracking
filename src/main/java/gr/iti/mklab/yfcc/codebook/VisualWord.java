package gr.iti.mklab.yfcc.codebook;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

public class VisualWord extends Word {
    
    private float[] word;

    public VisualWord(String _id, int len) {
        id = _id;
        word = new float[len];
        df = 0;
        idf = 0;
    }

    public VisualWord(int len) {
        word=new float[len];
        df=0;
        idf=0;
    }

    public VisualWord() {
        word = null;
        df = 0;
        idf = 0;
    }
    
    public float[] getWord() {
        return word;
    }

    public void writeToFile(PrintWriter pw) {
        pw.print(id);
        pw.print(" ");
        pw.print(df);
        pw.print(" ");
        pw.print(idf);
        pw.print(" ");
        int i;
        int n_dims=word.length;
        for(i=0;i<n_dims-1;i++)
            pw.print(word[i]+" ");
        pw.println(word[n_dims-1]);
    }

    public boolean loadFromFile(BufferedReader reader) {
        try {
            String line = reader.readLine();
            if(line != null) {
                if(line.trim().equals("")) 
                	return false;
                
                String[] parts = line.split(" ");
                if(word == null)
                    word = new float[parts.length-3];
                
                id = parts[0];
                df = Integer.parseInt(parts[1]);
                idf = Double.parseDouble(parts[2]);
                int i;
                int len = word.length;
                for(i=0; i<len; i++) {
                    word[i] = Float.parseFloat(parts[3+i]);
                }
                return true;
            }
        }
        catch(IOException e) {
            
        }
        return false;
    }

    public float distance(float[] descriptor) {
        float dist = 0;
        int i;
        int len1 = word.length;
        int len2 = descriptor.length;
        if(len1 != len2) 
        	return -1;
        
        for(i=0; i<len1; i++) {
            dist = dist + (descriptor[i]-word[i])*(descriptor[i]-word[i]);
        }
        return dist;
    }
    
    
}
