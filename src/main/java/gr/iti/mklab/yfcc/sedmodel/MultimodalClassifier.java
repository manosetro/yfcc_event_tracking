package gr.iti.mklab.yfcc.sedmodel;

import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.sedmodel.MultimodalSimilarity.SIM_TYPES;
import gr.iti.mklab.yfcc.dao.SolrItemClient;
import gr.iti.mklab.yfcc.vocabulary.Vocabulary;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.solr.client.solrj.SolrServerException;

import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;


public class MultimodalClassifier {
    
	public static double CLASSIFICATION_THRESHOLD = 0.97;
	
    //public static SIM_TYPES[] usedSimTypes = {SIM_TYPES.TIME_TAKEN_DAY_DIFF_3, SIM_TYPES.TIME_TAKEN_HOUR_DIFF_12, SIM_TYPES.TIME_TAKEN_HOUR_DIFF_24, SIM_TYPES.TIME_TAKEN, SIM_TYPES.DESCRIPTION_COSINE, SIM_TYPES.TAGS_COSINE, SIM_TYPES.TITLE_COSINE};
    //public static SIM_TYPES[] usedSimTypes = {SIM_TYPES.TIME_TAKEN_DAY_DIFF_3, SIM_TYPES.TIME_TAKEN_HOUR_DIFF_12, SIM_TYPES.TIME_TAKEN_HOUR_DIFF_24, SIM_TYPES.TIME_TAKEN};
    public static SIM_TYPES[] txtSimTypes = {SIM_TYPES.DESCRIPTION_COSINE, SIM_TYPES.TAGS_COSINE, SIM_TYPES.TITLE_COSINE};
    public static SIM_TYPES[] txtVisSimTypes = {SIM_TYPES.DESCRIPTION_COSINE, SIM_TYPES.TAGS_COSINE, SIM_TYPES.TITLE_COSINE, SIM_TYPES.TEXT_COSINE, SIM_TYPES.VLAD_SURF};
    
    private Instances dataTrain;
    public Classifier model;
    
    private ArrayList<Attribute> attributes;

	private boolean useVisual = false;

    public MultimodalClassifier(boolean useVisual) {
    	
    	this.useVisual = useVisual;
    	if(useVisual) {
    		attributes = MultimodalSimilarity.getAttributes(txtVisSimTypes);
    	}
    	else {
    		attributes = MultimodalSimilarity.getAttributes(txtSimTypes);
    	}
    	
    	dataTrain = new Instances("tr", attributes, attributes.size());
        dataTrain.setClassIndex(dataTrain.numAttributes() - 1);
    }
    
    public MultimodalClassifier(String modelFile, boolean useVisual) {
    	this(useVisual);
    	load(modelFile);
    }
    
    public double test(Item item1, Item item2) {
        if(model == null) {
            System.out.println("MODEL IS NULL!!!!!");
            return 0;
        }
        
        MultimodalSimilarity multimodalSimilarity = new MultimodalSimilarity(item1, item2, attributes);
        try {
            Instance instance = multimodalSimilarity.getSimilarities();
            double[] probs = null;
            synchronized(model) {
            	instance.setDataset(dataTrain);
            	probs = model.distributionForInstance(instance);
            }
            
            if(probs[1] > CLASSIFICATION_THRESHOLD) {
                return 1;
            }
            else {
                return -1;
            }
            
        } catch (Exception ex) {
            Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
        }
        return -1;
    }

    public double[] testProbability(Item item1, Item item2) {
        if(model==null) {
            System.out.println("MODEL IS NULL!!!!!");
            return null;
        }
        
        MultimodalSimilarity multimodalSimilarity = new MultimodalSimilarity(item1, item2, attributes);
        try {
            if(dataTrain == null) {
                dataTrain = new Instances("tr", attributes, attributes.size());
                dataTrain.setClassIndex(dataTrain.numAttributes()-1);
            }
            
            Instance instance = multimodalSimilarity.getSimilarities();
            
            instance.setDataset(dataTrain);
            double[] probs = model.distributionForInstance(instance);
            
            return probs;
        
        } catch (Exception ex) {
            Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
    

    public void save(String modelDirectory) {
        if(!modelDirectory.endsWith(File.separator)) {
            modelDirectory = modelDirectory + File.separator;
        }
        
        File tmp_dir = new File(modelDirectory);
        if((!tmp_dir.exists()) || (!tmp_dir.isDirectory())) {
            tmp_dir.mkdirs();
        }
        
        if(model != null) {
            System.out.println("saving model " + modelDirectory);
            try {
                //model.save(modelDirectory+"model.svm");
                weka.core.SerializationHelper.write(modelDirectory+"model.svm", model);
            } catch (Exception ex) {
                Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public void load(String modelFile) {
        File tmp_file = new File(modelFile);
        if(tmp_file.exists()) {
            try {
            	model = (Classifier) weka.core.SerializationHelper.read(modelFile);
            	System.out.println(model.getClass() + " loeaded!");
            	
            	if(useVisual) {
            		attributes = MultimodalSimilarity.getAttributes(txtVisSimTypes);
            	}
            	else {
            		attributes = MultimodalSimilarity.getAttributes(txtSimTypes);
            	}
            	System.out.println("ATTRIBUTES: " + attributes);
            } catch (Exception ex) {
                Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("loading model from " + modelFile);
        }
        else {
            model=null;
        }
        
        if(model == null) { 
        	System.out.println("Model failed to load!!!!");
        }
    }


    public ArrayList<Attribute> getAttributes() {
        return attributes;
    }
    	
    public static enum CLASSIFIER_TYPES {
    	SVM,
    	DECISION_TREE,
    	PART,
    	JRIP,
    	RANDOM_TREE,
    	RANDOM_FOREST,
    	MULTILAYER_PERCEPTRON,
    	NAIVE_BAYES,
    	NEAREST_NEIGHBOUR,
    	GP
    };
    
    public static String SVM_PARAMETERS="-C 1 -M ";				//http://weka.sourceforge.net/doc/weka/classifiers/functions/SMO.html
    public static String DECISION_TREE_PARAMETERS="-M 5";		//http://weka.sourceforge.net/doc/weka/classifiers/trees/J48.html
    public static String PART_PARAMETERS="";					//http://weka.sourceforge.net/doc/weka/classifiers/rules/PART.html
    public static String JRIP_PARAMETERS="";					//http://weka.sourceforge.net/doc/weka/classifiers/rules/JRip.html
    public static String RANDOM_TREE_PARAMETERS="";				//http://weka.sourceforge.net/doc/weka/classifiers/trees/RandomTree.html
    public static String RANDOM_FOREST_PARAMETERS="";			//http://weka.sourceforge.net/doc/weka/classifiers/trees/RandomForest.html
    public static String MULTILAYER_PERCEPTRON_PARAMETERS="";	//http://weka.sourceforge.net/doc/weka/classifiers/functions/MultilayerPerceptron.html
    public static String NAIVE_BAYES_PARAMETERS="";				//http://weka.sourceforge.net/doc/weka/classifiers/bayes/NaiveBayes.html
    public static String NEAREST_NEIGHBOUR_PARAMETERS="";		//http://weka.sourceforge.net/doc/weka/classifiers/lazy/IBk.html
    
    public static void main(String...args) throws IOException, SolrServerException, ParseException {
    	
    	Vocabulary vocabulary = Vocabulary.loadFromFile("/second_disk/workspace/yahoogc/vocabulary.txt");
    	
    	//String modelFile = "/second_disk/workspace/yahoogc/models/textual.svm";
    	//String modelFile = "/second_disk/workspace/yahoogc/models/temporal_textual.svm";
    	//String modelFile = "/second_disk/workspace/yahoogc/models/textual.svm";
    	String modelFile = "/second_disk/workspace/yahoogc/models/textual_visual.svm";
    	
    	MultimodalClassifier classifier = new MultimodalClassifier(modelFile, false);

    	SolrItemClient itemClient = new SolrItemClient("http://160.40.51.16:8983/solr/items");
    	
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date sinceDate = sdf.parse("2008-01-01T00:00:00Z");
		Date untilDate = DateUtils.addHours(sinceDate, 24);
		
		List<Item> items = itemClient.getInRange(sinceDate, untilDate);
		for(Item item : items) {
			vocabulary.loadItemVectors(item);
		}
		
		for(int i=0; i<items.size()-1; i++) {
			Item item1 = items.get(i);
			for(int j=i+1; j<items.size(); j++) {
				Item item2 = items.get(j);
				double semScore = classifier.test(item1, item2);
				if(semScore > 0) {
					System.out.println(item1.getId() + " - " + item2.getId() + " => " + semScore);
				}
				
			}
		}
			
    }
}
