package gr.iti.mklab.yfcc.sedmodel;

import gr.iti.mklab.yfcc.models.Item;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;


public class MultimodalSimilarity {
	
	public static TIME_DISTANCE_SCALING timeDistanceScaling = TIME_DISTANCE_SCALING.DAYS;
	
    public Item item1;
    public Item item2;
    
    private Instance similarities;

    public MultimodalSimilarity(Item item1, Item item2, List<Attribute> attributes) {
        this.item1 = item1;
        this.item2 = item2;
        
        similarities = computeSimilarities(item1, item2, attributes);
    }
    
    public Instance getSimilarities() {
    	return similarities;
    }
    
    public static Instance computeSimilarities(Item item1, Item item2, List<Attribute> attributes) {
        
        Instance sims = new DenseInstance(attributes.size());
        for(int i = 0; i<attributes.size()-1; i++) {
        	
        	String attribute = attributes.get(i).name();
        	
            if(attribute.equals(SIM_TYPES.DESCRIPTION_BM25.name())) {
                sims.setValue(i, item1.descriptionSimilarityBM25(item2));
            }
            
            if(attribute.equals(SIM_TYPES.DESCRIPTION_COSINE.name())) {
                sims.setValue(i, item1.descriptionSimilarityCosine(item2));
            }
            
            if(attribute.equals(SIM_TYPES.DISTANCE_SMALLER_THAN_1KM.name())) {
                sims.setValue(i, item1.locationSimilarity1Km(item2));
            }
            
            if(attribute.equals(SIM_TYPES.LOCATION.name())) {
                sims.setValue(i, item1.locationSimilarity(item2));
            }
            
            if(attribute.equals(SIM_TYPES.SAME_USER.name())) {
                sims.setValue(i, item1.sameUserSimilarity(item2));
            }
            
            if(attribute.equals(SIM_TYPES.TAGS_BM25.name())) {
                sims.setValue(i, item1.tagsSimilarityBM25(item2));
            }
            
            if(attribute.equals(SIM_TYPES.TAGS_COSINE.name())) {
                sims.setValue(i, item1.tagsSimilarityCosine(item2));
            }
            
            if(attribute.equals(SIM_TYPES.TEXT_COSINE.name())) {
                sims.setValue(i, item1.textSimilarityCosine(item2));
            }
            
            if(attribute.equals(SIM_TYPES.TIME_TAKEN.name())) {
            	double divisor = 1000.0;
                if (timeDistanceScaling == TIME_DISTANCE_SCALING.MINUTES) {
                    divisor = divisor * 60;
                }
                if (timeDistanceScaling == TIME_DISTANCE_SCALING.HOURS) {
                    divisor = divisor * 60 * 60;
                }
                if (timeDistanceScaling == TIME_DISTANCE_SCALING.DAYS) {
                    divisor = divisor * 60 * 60 * 24;
                }
                sims.setValue(i, item1.timeTakenSimilarity(item2, divisor));
            }
            
            if(attribute.equals(SIM_TYPES.TIME_TAKEN_DAY_DIFF_3.name())) {
                sims.setValue(i, item1.timeTakenDayDiff3(item2));
            }
            
            if(attribute.equals(SIM_TYPES.TIME_TAKEN_HOUR_DIFF_12.name())) {
                sims.setValue(i, item1.timeTakenHourDiff12(item2));
            }
            
            if(attribute.equals(SIM_TYPES.TIME_TAKEN_HOUR_DIFF_24.name())) {
                sims.setValue(i, item1.timeTakenHourDiff24(item2));
            }
            
            if(attribute.equals(SIM_TYPES.TIME_UPLOADED.name())) {
            	double divisor = 1000.0;
                if (timeDistanceScaling == TIME_DISTANCE_SCALING.MINUTES) {
                    divisor = divisor * 60;
                }
                if (timeDistanceScaling == TIME_DISTANCE_SCALING.HOURS) {
                    divisor = divisor * 60 * 60;
                }
                if (timeDistanceScaling == TIME_DISTANCE_SCALING.DAYS) {
                    divisor = divisor * 60 * 60 * 24;
                }
                
                sims.setValue(i, item1.timeUploadedSimilarity(item2, divisor));
            }
            
            if(attribute.equals(SIM_TYPES.TITLE_BM25.name())) {
                sims.setValue(i, item1.titleSimilarityBM25(item2));
            }
            
            if(attribute.equals(SIM_TYPES.TITLE_COSINE.name())) {
                sims.setValue(i, item1.titleSimilarityCosine(item2));
            }
            
            if(attribute.equals(SIM_TYPES.VLAD_SURF.name())) {
            	double distance = .0;
            	double[] v1 = item1.getVlad();
            	double[] v2 = item2.getVlad();
            	if(v1 != null && v2 != null) {
            		 for(int index=0; index<v1.length; index++) {
         	            double diff = v1[index] - v2[index];
         	            distance = distance + diff*diff;
         	        }
            		 sims.setValue(i, distance);
            	}
            	else {
            		return null;
            	}
            } 
            
        }
        
        return sims;
    }
    
    public void saveToFile(PrintWriter pw) {
        int i;
        int n_similarities = similarities.numAttributes()-1;
        for(i=0; i<n_similarities-1; i++) {
            pw.print(similarities.value(i) + " ");
        }
        pw.println(similarities.value(i));
    }
    
    public static ArrayList<Attribute> getAttributes(SIM_TYPES[] usedSimTypes) {
        
    	ArrayList<Attribute> attributesList = new ArrayList<Attribute>();

        int n_sims = usedSimTypes.length;
        for(int i=0; i<n_sims; i++) {
            if(usedSimTypes[i] == SIM_TYPES.LOCATION) {
            	attributesList.add(new Attribute(SIM_TYPES.LOCATION.toString()));
            }
            
            if(usedSimTypes[i] == SIM_TYPES.DISTANCE_SMALLER_THAN_1KM) {
            	attributesList.add(new Attribute(SIM_TYPES.DISTANCE_SMALLER_THAN_1KM.toString()));
        	}
        
            if(usedSimTypes[i] == SIM_TYPES.TEXT_COSINE) {
            	attributesList.add(new Attribute(SIM_TYPES.TEXT_COSINE.toString()));
            }
            
            if(usedSimTypes[i] == SIM_TYPES.TITLE_COSINE) {
            	attributesList.add(new Attribute(SIM_TYPES.TITLE_COSINE.toString()));
            }
            
            if(usedSimTypes[i] == SIM_TYPES.TAGS_COSINE) {
            	attributesList.add(new Attribute(SIM_TYPES.TAGS_COSINE.toString()));
            }
            
            if(usedSimTypes[i] == SIM_TYPES.DESCRIPTION_COSINE) {
            	attributesList.add(new Attribute(SIM_TYPES.DESCRIPTION_COSINE.toString()));
            }
            
            if(usedSimTypes[i] == SIM_TYPES.TITLE_BM25) {
            	attributesList.add(new Attribute(SIM_TYPES.TITLE_BM25.toString()));
            }
            
            if(usedSimTypes[i] == SIM_TYPES.TAGS_BM25) {
            	attributesList.add(new Attribute(SIM_TYPES.TAGS_BM25.toString()));
        	}
        
            if(usedSimTypes[i] == SIM_TYPES.DESCRIPTION_BM25) {
            	attributesList.add(new Attribute(SIM_TYPES.DESCRIPTION_BM25.toString()));
        	}
        
            if(usedSimTypes[i] == SIM_TYPES.TIME_UPLOADED) {
            	attributesList.add(new Attribute(SIM_TYPES.TIME_UPLOADED.toString()));
            }
            
            if(usedSimTypes[i] == SIM_TYPES.TIME_TAKEN) {
            	attributesList.add(new Attribute(SIM_TYPES.TIME_TAKEN.toString()));
            }
            
            if(usedSimTypes[i] == SIM_TYPES.SAME_USER) {
            	attributesList.add(new Attribute(SIM_TYPES.SAME_USER.toString()));
            }
            
            if(usedSimTypes[i] == SIM_TYPES.TIME_TAKEN_HOUR_DIFF_12) {
            	attributesList.add(new Attribute(SIM_TYPES.TIME_TAKEN_HOUR_DIFF_12.toString()));
            }
            
            if(usedSimTypes[i] == SIM_TYPES.TIME_TAKEN_HOUR_DIFF_24) {
            	attributesList.add(new Attribute(SIM_TYPES.TIME_TAKEN_HOUR_DIFF_24.toString()));
            }
            
            if(usedSimTypes[i] == SIM_TYPES.TIME_TAKEN_DAY_DIFF_3) {
            	attributesList.add(new Attribute(SIM_TYPES.TIME_TAKEN_DAY_DIFF_3.toString()));
            }
            
            if(usedSimTypes[i] == SIM_TYPES.VLAD_SURF) {
            	attributesList.add(new Attribute(SIM_TYPES.VLAD_SURF.toString()));
            }
        }

        List<String> fvClassVal = new ArrayList<String>(2);
        fvClassVal.add("negative");
        fvClassVal.add("positive");
        Attribute ClassAttribute = new Attribute("theClass", fvClassVal);

        attributesList.add(ClassAttribute);
        
        return attributesList;
    }
 
    public String toString() {
    	StringBuffer strBf = new StringBuffer();
    	strBf.append("{");
    	for(int i=0;i<similarities.numAttributes();i++) {
    		strBf.append(similarities.value(i) + " ");
    	}
    	
    	strBf.append("}");
    	return strBf.toString();
    }
    
    //Available time scalings
    public static enum TIME_DISTANCE_SCALING {SECONDS, MINUTES, HOURS, DAYS};
    
    //The following are the types of available modalities
    public static enum SIM_TYPES {
    	TIME_TAKEN,
    	TIME_UPLOADED,
    	LOCATION,
    	SIFT,
    	SAME_USER,
    	GIST,
    	VLAD_SURF,
    	TIME_TAKEN_HOUR_DIFF_12,
    	TIME_TAKEN_HOUR_DIFF_24,
    	TIME_TAKEN_DAY_DIFF_3,
    	TITLE_COSINE,
    	TAGS_COSINE,
    	DESCRIPTION_COSINE,
    	TITLE_BM25,TAGS_BM25,
    	DESCRIPTION_BM25,
    	TEXT_COSINE,
    	DISTANCE_SMALLER_THAN_1KM,
    	DEEP_CONV};
}
