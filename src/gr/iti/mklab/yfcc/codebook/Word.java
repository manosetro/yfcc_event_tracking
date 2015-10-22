package gr.iti.mklab.yfcc.codebook;

import java.io.BufferedReader;
import java.io.PrintWriter;

public abstract class Word {

    protected String id;
    protected int df;
    protected double idf;

    public Word(String _id) {
        id = _id;
        df = 0;
        idf = 0;
    }

    public Word() {
        df = 0;
        idf = 0;
    }

    public void computeIDF(int nDocs) {
        idf = Math.log(((double) nDocs)/((double) df+1));
    }

    public int getDf() {
        return df;
    }

    public double getIdf() {
        return idf;
    }
    
    public double getIdf(int nDocs) {
        if(idf == 0) {
        	computeIDF(nDocs);
        }
        
        return idf;
    }


    public void increaseDF() {
        df++;
    }

    public abstract void writeToFile(PrintWriter pw);

    public abstract boolean loadFromFile(BufferedReader reader);

    public String getId() {
        return id;
    }
    
}


