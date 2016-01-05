package gr.iti.mklab.yfcc.models;

public class TFIDF {
    public double tf;
    public double idf;
    public double tfidf;
    
    @Override
    public String toString() {
    	return "[tf="+tf+", idf="+idf+", tf*idf="+tfidf+"]";
    }
}