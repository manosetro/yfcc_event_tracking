package gr.iti.mklab.yfcc.sem;

import gr.iti.mklab.yfcc.models.Event;
import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.utils.GeodesicDistanceCalculator;

import java.io.PrintWriter;
import java.util.ArrayList;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;

public class MultimodalSimilarity {

    public static double AVG_TITLE = 1;
    public static double AVG_DESCRIPTION = 1;
    public static double AVG_TAGS = 1;
	    
	public Instance similarities;

    public MultimodalSimilarity(Item item1, Item item2, ArrayList<Attribute> attributes) {
        similarities = computeSimilarities(item1, item2, attributes);
    }
    
    public MultimodalSimilarity(Event event1, Event event2, ArrayList<Attribute> attributes) {
        similarities = computeEventsSimilarities(event1, event2, attributes);
    }
    
    public static Instance computeSimilarities(Item item1, Item item2, ArrayList<Attribute> attributes) {
    	
    	Instance sims = new DenseInstance(attributes.size());
    	
        for(int i = 0; i<attributes.size()-1; i++) {
        	String attrName = attributes.get(i).name();
        
        	/*
        	 * Location Attribute
        	 */
            if(attrName.equals(ATTRIBUTES.LOCATION.name())) {
                sims.setValue(i, locationSimilarity(item1, item2));
            }
            
            /* 
             * User Attribute
             */
            if(attrName.equals(ATTRIBUTES.SAME_USER.name())) {
                sims.setValue(i, sameUserSimilarity(item1, item2));
        	}
        
            /*
             * Time Attributes
             */
            if(attrName.equals(ATTRIBUTES.TIME_TAKEN.name())) {
                sims.setValue(i, timeTakenSimilarity(item1, item2));
            }
            
            if(attrName.equals(ATTRIBUTES.TIME_TAKEN_DAY_DIFF_3.name())) {
                sims.setValue(i, timeTakenDayDiff3(item1, item2));
            }
            
            if(attrName.equals(ATTRIBUTES.TIME_TAKEN_HOUR_DIFF_12.name())) {
                sims.setValue(i, timeTakenHourDiff12(item1, item2));
        	}
        
            if(attrName.equals(ATTRIBUTES.TIME_TAKEN_HOUR_DIFF_24.name())) {
                sims.setValue(i, timeTakenHourDiff24(item1, item2));
            }
            
            /* 
             * Textual Attributes
             */
            if(attrName.equals(ATTRIBUTES.TITLE_BM25.name())) {
                sims.setValue(i, titleSimilarityBM25(item1, item2));
            }
            
            if(attrName.equals(ATTRIBUTES.TITLE_COSINE.name())) {
                sims.setValue(i, titleSimilarityCosine(item1, item2));
        	}
        
            if(attrName.equals(ATTRIBUTES.TAGS_BM25.name())) {
                sims.setValue(i, tagsSimilarityBM25(item1, item2));
            }
            
            if(attrName.equals(ATTRIBUTES.TAGS_COSINE.name())) {
                sims.setValue(i, tagsSimilarityCosine(item1, item2));
            }
            
        	if(attrName.equals(ATTRIBUTES.DESCRIPTION_BM25.name())) {
                sims.setValue(i, descriptionSimilarityBM25(item1, item2));
        	}
        	
            if(attrName.equals(ATTRIBUTES.DESCRIPTION_COSINE.name())) {
                sims.setValue(i, descriptionSimilarityCosine(item1, item2));
            }

        }
        return sims;
    }
    
    public static Instance computeEventsSimilarities(Event event1, Event event2, ArrayList<Attribute> attributes) {
        
    	int nDistances = attributes.size();
        Instance sims = new DenseInstance(nDistances + 1);

        for(int i=0; i<attributes.size()-1; i++) {
        	String attrName = attributes.get(i).name();
        
        	/*
        	 * Location Attribute
        	 */
            if(attrName.equals(ATTRIBUTES.LOCATION.name()))
                sims.setValue(i, event1.locationSimilarity(event2));
            
            /* 
             * User Attribute
             */
            if(attrName.equals(ATTRIBUTES.SAME_USER.name()))
                sims.setValue(i, event1.userSetSimilarity(event2));
            
            /*
             * Time Attributes
             */
            if(attrName.equals(ATTRIBUTES.TIME_TAKEN.name()))
                sims.setValue(i, event1.timeTakenSimilarity(event2));
         
            if(attrName.equals(ATTRIBUTES.TIME_TAKEN_DAY_DIFF_3.name()))
                sims.setValue(i, event1.timeTakenDayDiff3(event2));
            
            if(attrName.equals(ATTRIBUTES.TIME_TAKEN_HOUR_DIFF_12.name()))
                sims.setValue(i, event1.timeTakenHourDiff12(event2));
            
            if(attrName.equals(ATTRIBUTES.TIME_TAKEN_HOUR_DIFF_24.name()))
                sims.setValue(i, event1.timeTakenHourDiff24(event2));
           
            /*
             * Textual Attributes
             */
            if(attrName.equals(ATTRIBUTES.TITLE_BM25.name()))
                sims.setValue(i, event1.titleSimilarityBM25(event2));
            
            if(attrName.equals(ATTRIBUTES.TITLE_COSINE.name()))
                sims.setValue(i, event1.titleSimilarityCosine(event2));
            
            if(attrName.equals(ATTRIBUTES.TAGS_BM25.name()))
                sims.setValue(i, event1.tagsSimilarityBM25(event2));
            
            if(attrName.equals(ATTRIBUTES.TAGS_COSINE.name()))
                sims.setValue(i, event1.tagsSimilarityCosine(event2));

            if(attrName.equals(ATTRIBUTES.DESCRIPTION_BM25.name()))
                sims.setValue(i, event1.descriptionSimilarityBM25(event2));
            
            if(attrName.equals(ATTRIBUTES.DESCRIPTION_COSINE.name()))
                sims.setValue(i, event1.descriptionSimilarityCosine(event2));
                
            /*
             * Visual Attribute
             */
            if(attrName.equals(ATTRIBUTES.VLAD_SURF.name()))
                sims.setValue(i, -1);
            
        }
        return sims;
    }
    
    public void saveToFile(PrintWriter pw) {
        int n_similarities = similarities.numAttributes() - 1;
        int i;
        for(i=0; i<n_similarities-1; i++)
            pw.print(similarities.value(i)+" ");
        pw.println(similarities.value(i));
    }
    
    public static ArrayList<Attribute> getAttributes(ATTRIBUTES[] simTypes) {
        ArrayList<Attribute> attributesList = new ArrayList<Attribute>();

		for(ATTRIBUTES simType : simTypes) {
        	attributesList.add(new Attribute(simType.toString()));
        }

        ArrayList<String> fvClassVal = new ArrayList<String>(2);
        fvClassVal.add("negative");
        fvClassVal.add("positive");
        Attribute ClassAttribute = new Attribute("theClass", fvClassVal);

        attributesList.add(ClassAttribute);
        
        return attributesList;
    }
    
    public static ArrayList<Attribute> getEventAttributes(ATTRIBUTES[] simTypes) {
        ArrayList<Attribute> attributesList = new ArrayList<Attribute>();

		for(ATTRIBUTES simType : simTypes) {
        	attributesList.add(new Attribute(simType.toString()));
        }

        ArrayList<String> fvClassVal = new ArrayList<String>(2);
        fvClassVal.add("negative");
        fvClassVal.add("positive");
        Attribute ClassAttribute = new Attribute("theClass", fvClassVal);

        attributesList.add(ClassAttribute);
        
        return attributesList;
    }
    
    public static double locationSimilarity(Item item1, Item item2) {
        if(item1.hasLocation() && item2.hasLocation())
            return GeodesicDistanceCalculator.vincentyDistance(
            		item1.getLatitude(), item1.getLongitude(), 
            		item2.getLatitude(), item2.getLongitude());
        else 
            return -1;
    }
    
    public static double timeUploadedSimilarity(Item item1, Item item2) {
        if(item1.getUploadDate() != null && item2.getUploadDate() != null) {
            double divisor = 1000.0 * 60 * 60;
            return Math.abs(item1.getUploadDate().getTime() - item2.getUploadDate().getTime()) / divisor;
        }
        else {
            return -1;
        }
    }
    
    public static double timeTakenSimilarity(Item item1, Item item2) {
    	if(item1.getTakenDate() != null && item2.getTakenDate() != null) {
    		double divisor = 1000.0 * 60 * 60;
        	return Math.abs(item1.getTakenDate().getTime() - item2.getTakenDate().getTime())/divisor;
    	}
    	else {
    		return -1;
    	}
    }

    public static double timeTakenHourDiff12(Item item1, Item item2) {
        double hours = timeTakenSimilarity(item1, item2);
        if(hours < 12 && hours > 0) {
            return 1;
        }
        else { 
            return 0;
        }
    }
    
    public static double timeTakenHourDiff24(Item item1, Item item2){
    	double hours = timeTakenSimilarity(item1, item2);
        if(hours < 24 && hours > 0) {
            return 1;
        }
        else { 
            return 0;
        }
    }

    public static double timeTakenDayDiff3(Item item1, Item item2) {
    	double hours = timeTakenSimilarity(item1, item2);
        double days = hours / 24;
        if(days < 3 && days>0) {
            return 1;
        }
        else { 
            return 0;
        }
    }
    
    public static double sameUserSimilarity(Item item1, Item item2) {
        if(item1.getUserid().equals(item2.getUserid())) {
            return 1;
        }
        
        return 0;
    }    
    
    public static double descriptionSimilarityCosine(Item item1, Item item2) {
    	if(item1.getDescriptionVector() == null || item2.getDescriptionVector() == null)
    		return .0;
    	
        return item1.getDescriptionVector().cosineSimilarity(item2.getDescriptionVector());
    }
    
    public static double titleSimilarityCosine(Item item1, Item item2) {
    	if(item1.getTitleVector() == null || item2.getTitleVector() == null)
    		return .0;
    	
        return item1.getTitleVector().cosineSimilarity(item2.getTitleVector());
    }
    
    public static double tagsSimilarityCosine(Item item1, Item item2) {
    	if(item1.getTagsVector() == null || item2.getTagsVector() == null)
    		return .0;
    	
        return item1.getTagsVector().cosineSimilarity(item2.getTagsVector());
    }

    public static double descriptionSimilarityBM25(Item item1, Item item2) {
    	if(item1.getDescriptionVector() == null || item2.getDescriptionVector() == null)
    		return .0;
    	
        return item1.getDescriptionVector().BM25Similarity(item2.getDescriptionVector(), AVG_DESCRIPTION);
    }
    
    public static double titleSimilarityBM25(Item item1, Item item2) {
    	if(item1.getTitleVector() == null || item2.getTitleVector() == null)
    		return .0;
    	
        return item1.getTitleVector().BM25Similarity(item2.getTitleVector(), AVG_TITLE);
    }
    
    public static double tagsSimilarityBM25(Item item1, Item item2) {
    	if(item1.getTagsVector() == null || item2.getTagsVector() == null)
    		return .0;
    	
        return item1.getTagsVector().BM25Similarity(item2.getTagsVector(), AVG_TAGS);
    }
    
    public static enum ATTRIBUTES {
    	TIME_TAKEN,
    	TIME_TAKEN_HOUR_DIFF_12,
    	TIME_TAKEN_HOUR_DIFF_24,
    	TIME_TAKEN_DAY_DIFF_3,
    	LOCATION,
    	SAME_USER,
    	VLAD_SURF,
    	TITLE_COSINE,
    	TITLE_BM25,
    	TAGS_COSINE,
    	TAGS_BM25,
    	DESCRIPTION_COSINE,
    	DESCRIPTION_BM25
    };  
    
}