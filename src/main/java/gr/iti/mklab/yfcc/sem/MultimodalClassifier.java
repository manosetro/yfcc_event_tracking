package gr.iti.mklab.yfcc.sem;

import gr.iti.mklab.yfcc.models.Event;
import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.sem.MultimodalSimilarity.ATTRIBUTES;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.classifiers.functions.SMO;
import weka.classifiers.lazy.IBk;
import weka.classifiers.rules.JRip;
import weka.classifiers.rules.PART;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;
import weka.classifiers.trees.RandomTree;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;

public class MultimodalClassifier {
    
	ATTRIBUTES[] eventSimTypes;
	
	private List<Pair<Item, Item>> positiveTrainingPairs;
	private List<Pair<Item, Item>> negativeTrainingPairs;
	
    private Instances dataTrain;
    private Classifier model;
    
    private ArrayList<Attribute> attributes;
    
    List<MultimodalSimilarity> positiveMultimodalSimilarities;
    List<MultimodalSimilarity> negativeMultimodalSimilarities;
	
    private ATTRIBUTES[] usedSimTypes;

    public MultimodalClassifier(int nPositivePairs, int nNegativePairs, ATTRIBUTES[] usedSimTypes) {
    	
    	positiveTrainingPairs = new ArrayList<Pair<Item, Item>>(nPositivePairs);
    	negativeTrainingPairs = new ArrayList<Pair<Item, Item>>(nNegativePairs);
    	
    	this.usedSimTypes = usedSimTypes;
    }

    public void addPositiveTrainingPair(Item item1, Item item2) {
    	positiveTrainingPairs.add(Pair.of(item1, item2));	
    }
    
    public void addNegativeTrainingPair(Item item1, Item item2) {
    	negativeTrainingPairs.add(Pair.of(item1, item2));
    }

    public Pair<Item, Item> getPositiveTrainingPair(int index) {
    	return positiveTrainingPairs.get(index);	
    }
    
    public Pair<Item, Item> getNegativeTrainingPair(int index) {
    	return negativeTrainingPairs.get(index);
    }
    
    public void train(ClassifierTypes classifierType) {
    	
        String parameters = "";
        if(classifierType == ClassifierTypes.decisionTree)
            parameters = DECISION_TREE_PARAMETERS;
        if(classifierType == ClassifierTypes.jrip)
            parameters = JRIP_PARAMETERS;
        if(classifierType == ClassifierTypes.MultilayerPerceptron)
            parameters = MULTILAYER_PERCEPTRON_PARAMETERS;
        if(classifierType == ClassifierTypes.NaiveBayes)
            parameters = NAIVE_BAYES_PARAMETERS;
        if(classifierType == ClassifierTypes.NearestNeighbour)
            parameters = NEAREST_NEIGHBOUR_PARAMETERS;
        if(classifierType == ClassifierTypes.part)
            parameters = PART_PARAMETERS;
        if(classifierType == ClassifierTypes.randomForest)
            parameters = RANDOM_FOREST_PARAMETERS;
        if(classifierType == ClassifierTypes.randomTree)
            parameters = RANDOM_TREE_PARAMETERS;
        if(classifierType == ClassifierTypes.svm)
            parameters = SVM_PARAMETERS;
        
        String[] options;
        System.out.println("Classifier type: " + classifierType.toString());
        try {
            options = weka.core.Utils.splitOptions(parameters);
            if(classifierType == ClassifierTypes.svm) {
                model = new SMO();
                ((SMO)model).setOptions(options);
            }
            if(classifierType == ClassifierTypes.decisionTree){
                model = new J48();
                System.out.println(options);
                ((J48)model).setOptions(options);
            }
            if(classifierType == ClassifierTypes.jrip){
                model = new JRip();
                ((JRip)model).setOptions(options);
            }
            if(classifierType == ClassifierTypes.MultilayerPerceptron){
                model = new MultilayerPerceptron();
                ((MultilayerPerceptron)model).setOptions(options);
            }
            if(classifierType == ClassifierTypes.NaiveBayes){
                model = new NaiveBayes();
                ((NaiveBayes)model).setOptions(options);
            }
            if(classifierType == ClassifierTypes.NearestNeighbour){
                model = new IBk();
                ((IBk)model).setOptions(options);
            }
            if(classifierType == ClassifierTypes.part){
                model = new PART();
                ((PART)model).setOptions(options);
            }
            if(classifierType == ClassifierTypes.randomForest){
                model = new RandomForest();
                ((RandomForest)model).setOptions(options);
            }
            if(classifierType == ClassifierTypes.randomTree){
                model = new RandomTree();
               ((RandomTree)model).setOptions(options);
            }
        } catch (Exception ex) {
            Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        int nPositiveExamples = positiveTrainingPairs.size();
        int nNegativeExamples = negativeTrainingPairs.size();
        
        attributes = MultimodalSimilarity.getAttributes(usedSimTypes);
        System.out.println("ATTRIBUTES : " + attributes);
        
        positiveMultimodalSimilarities = new ArrayList<MultimodalSimilarity>(nPositiveExamples);
        for(Pair<Item, Item> pair : positiveTrainingPairs) {
            positiveMultimodalSimilarities.add(new MultimodalSimilarity(pair.getLeft(), pair.getRight(), attributes));
        }
        
        negativeMultimodalSimilarities = new ArrayList<MultimodalSimilarity>(nNegativeExamples);
        for(Pair<Item, Item> pair : negativeTrainingPairs) {
        	negativeMultimodalSimilarities.add(new MultimodalSimilarity(pair.getLeft(), pair.getRight(), attributes));
    	}
        int n_points = nPositiveExamples/2 + nNegativeExamples/2;
        dataTrain = new Instances("train", attributes, n_points);
        Instances dataTestPos = new Instances("testPos", attributes, n_points/2);
        Instances dataTestNeg = new Instances("testNeg", attributes, n_points/2);
        dataTrain.setClassIndex(dataTrain.numAttributes() - 1);
        dataTestPos.setClassIndex(dataTrain.numAttributes() - 1);
        dataTestNeg.setClassIndex(dataTrain.numAttributes() - 1);

        int n_vars = usedSimTypes.length;
        
        System.out.println("n vars : " + n_vars);
        System.out.println("n dims : " + (positiveMultimodalSimilarities.get(0).similarities.numAttributes()-1));

        for(int i=0; i<nPositiveExamples/2; i++) {
            Instance instance = positiveMultimodalSimilarities.get(i).similarities;
            instance.setDataset(dataTrain);
            instance.setValue(instance.numAttributes()-1, "positive");
            dataTrain.add(instance);
        }
        for(int i=0; i<nNegativeExamples/2; i++){
            Instance instance = negativeMultimodalSimilarities.get(i).similarities;
            instance.setDataset(dataTrain);
            instance.setValue(instance.numAttributes()-1, "negative");
            dataTrain.add(instance);
        }

        System.out.println("No of items: " + dataTrain.numInstances());
        System.out.println("No of variables: " + dataTrain.numAttributes());
        System.out.println("No of classes: " + dataTrain.numClasses());
        
        try {
        	// build classifier
            model.buildClassifier(dataTrain);         
        } catch (Exception ex) {
            Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        System.out.println("Trained");
        
        System.out.println("Positive test");
        for(int i = nPositiveExamples/2; i<nPositiveExamples; i++) {
            Instance instance = positiveMultimodalSimilarities.get(i).similarities;
            instance.setDataset(dataTestPos);
            instance.setValue(instance.numAttributes()-1, "positive");
            dataTestPos.add(instance);
        }
        for(int i = nNegativeExamples/2; i<nNegativeExamples; i++) {
            Instance instance = negativeMultimodalSimilarities.get(i).similarities;
            instance.setDataset(dataTestNeg);
            instance.setValue(instance.numAttributes()-1, "negative");
            dataTestNeg.add(instance);
        }
        dataTestPos.setClassIndex(dataTestPos.numAttributes() - 1);
        dataTestNeg.setClassIndex(dataTestNeg.numAttributes() - 1);

        Evaluation evalPos;
        try {
            evalPos = new Evaluation(dataTrain);
            if(model == null) 
            	System.out.println("model is null");
            evalPos.evaluateModel(model, dataTestPos);
            System.out.println(evalPos.toSummaryString("\nResults\n======\n", false));            
        } catch (Exception ex) {
            Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
        }

        Evaluation evalNeg;
        try {
            evalNeg = new Evaluation(dataTrain);
            if(model == null) 
            	System.out.println("model is null");
            evalNeg.evaluateModel(model, dataTestNeg);
            System.out.println(evalNeg.toSummaryString("\nResults\n======\n", false));            
        } catch (Exception ex) {
            Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
        
    
    public double sameClassScore(Item item1, Item item2) {
        if(model == null) 
        	return 0;
        
        if(attributes == null) {
            attributes = MultimodalSimilarity.getAttributes(usedSimTypes);
        }
        
        MultimodalSimilarity multimodalSimilarity = new MultimodalSimilarity(item1, item2, attributes);
        try {
            if(dataTrain == null) {
                dataTrain = new Instances("tr", attributes, attributes.size());
                dataTrain.setClassIndex(dataTrain.numAttributes() - 1);
            }
            multimodalSimilarity.similarities.setDataset(dataTrain);
            return model.classifyInstance(multimodalSimilarity.similarities);
        } catch (Exception ex) {
            Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
        }
        return -1;
    }

    public double sameClassScore(Event event1, Event event2) {
        if(model == null) 
        	return 0;
        
        if(attributes == null) {
            attributes = MultimodalSimilarity.getEventAttributes(eventSimTypes);
        }
        
        MultimodalSimilarity multimodalSimilarity = new MultimodalSimilarity(event1, event2, attributes);
        try {
            if(dataTrain == null) {
                dataTrain = new Instances("tr", attributes, attributes.size());
                dataTrain.setClassIndex(dataTrain.numAttributes() - 1);
            }
            multimodalSimilarity.similarities.setDataset(dataTrain);
            return model.classifyInstance(multimodalSimilarity.similarities);
        } catch (Exception ex) {
            Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
        }
        return -1;
    }
    
    public double[] testProbability(Item item1, Item item2) {
        if(model == null) 
        	return null;
        
        if(attributes == null)
            attributes = MultimodalSimilarity.getAttributes(usedSimTypes);
        MultimodalSimilarity multimodalSimilarity = new MultimodalSimilarity(item1, item2, attributes);
        double[] probs = new double[2];
        try {
        	if(dataTrain == null) {
                dataTrain = new Instances("tr", attributes, attributes.size());
                dataTrain.setClassIndex(dataTrain.numAttributes() - 1);
            }
            multimodalSimilarity.similarities.setDataset(dataTrain);
            probs = model.distributionForInstance(multimodalSimilarity.similarities);
        } catch (Exception ex) {
            Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
        }
        return probs;
    }
    
    public void save(String modelFilename) {
        if(model != null) {
            System.out.println("saving model in " + modelFilename);
            try {
                weka.core.SerializationHelper.write(modelFilename, model);
            } catch (Exception ex) {
                Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
            } 
        }
    }
    
    public void save(String modelDirectory, ClassifierTypes classifierType) {
        if(!modelDirectory.endsWith(File.separator))
            modelDirectory = modelDirectory + File.separator;
        
    	String modelFile = modelDirectory + File.separator + "model." + classifierType;
    	
        File dir = new File(modelDirectory);
        if((!dir.exists()) || (!dir.isDirectory()))
            dir.mkdirs();
        if(model != null) {
            System.out.println("saving model in " + modelDirectory);
            try {
                weka.core.SerializationHelper.write(modelFile, model);
            } catch (Exception ex) {
                Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
            } 
        }
        savePositivePairsIds(modelDirectory);
        saveNegativePairsIds(modelDirectory);
    }

    private void savePositivePairsIds(String directory) {
    	if(!directory.endsWith(File.separator))
    		directory = directory + File.separator;
    	
        String filename = directory + "positive_pairs.txt";
        int n_pairs = positiveTrainingPairs.size();
        try {
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(filename)));
            for(int i=0; i<n_pairs; i++) {
            	Pair<Item, Item> pair = positiveTrainingPairs.get(i);
                pw.println(pair.getLeft().getId() + " " + pair.getRight().getId());
            }
            pw.close();
        }
        catch(IOException e) {
        }
    }

    private void saveNegativePairsIds(String directory) {
    	if(!directory.endsWith(File.separator))
    		directory = directory + File.separator;
    	
        String filename = directory + "negative_pairs.txt";
        int n_pairs = negativeTrainingPairs.size();
        try {
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(filename)));
			for(int i=0; i<n_pairs; i++) {
				Pair<Item, Item> pair = negativeTrainingPairs.get(i);
                pw.println(pair.getKey().getId() + " " + pair.getValue().getId());
			}
            pw.close();
        }
        catch(IOException e) {
        }
    }
   
    public void load(String modelFilename) {
        File file=new File(modelFilename);
        if(file.exists()) {
            try {
            	model = (Classifier) weka.core.SerializationHelper.read(modelFilename);
            	
            	System.out.println("Capabilities: " + model.getCapabilities());
            } catch (Exception ex) {
                Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("loading model from : "+modelFilename);
        }
        else {
            model = null;
        }
    }
    
    public void load(String modelDirectory, ClassifierTypes classifierType) {
        
    	String modelFile = modelDirectory + File.separator + "model." + classifierType;
    	
        File file=new File(modelFile);
        if(file.exists()) {
            try {
            	model = (Classifier) weka.core.SerializationHelper.read(modelFile);
            	
            	System.out.println("Capabilities: " + model.getCapabilities());
            } catch (Exception ex) {
                Logger.getLogger(MultimodalClassifier.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("loading model from : "+modelFile);
        }
        else {
            model = null;
        }
    }
    
    public void loadPositivePairsIds(String directory, Map<String, Item> collection) {
        try{
            String filename = directory + File.separator + "positive_pairs.txt";
            FileInputStream fstream = new FileInputStream(filename);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            ArrayList<String> lines = new ArrayList<String>();
            while ((strLine = br.readLine()) != null) {
                if(!strLine.trim().equals(""))
                    lines.add(strLine);
            }
            int n_pairs = lines.size();
            positiveTrainingPairs = new ArrayList<Pair<Item, Item>>(n_pairs);
            for(int i=0; i<n_pairs; i++){
                String[] parts = lines.get(i).split(" ");
                Item item1 = collection.get(parts[0]);
                Item item2 = collection.get(parts[1]);
                
                positiveTrainingPairs.add(Pair.of(item1, item2));
            }
            br.close();
        }
        catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void loadNegativePairsIds(String directory, Map<String, Item> collection) {
        try {
            String filename = directory + File.separator + "negative_pairs.txt";
            FileInputStream fstream = new FileInputStream(filename);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            ArrayList<String> lines=new ArrayList<String>();
            while ((strLine = br.readLine()) != null) {
                if(!strLine.trim().equals(""))
                    lines.add(strLine);
            }
            
            int n_pairs = lines.size();
            negativeTrainingPairs = new ArrayList<Pair<Item, Item>>(n_pairs);
            for(int i=0; i<n_pairs; i++) {
                String[] parts = lines.get(i).split(" ");
                Item item1 = collection.get(parts[0]);
                Item item2 = collection.get(parts[1]);
                
                positiveTrainingPairs.add(Pair.of(item1,  item2));
            }
            
            br.close();
        }
        catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void savePositiveMultimodalSimilarities(String modelDirectory) {
    	if(!modelDirectory.endsWith(File.separator))
    		modelDirectory = modelDirectory + File.separator;
    	
        if(positiveMultimodalSimilarities != null) {
            String filename = modelDirectory + "positive_similarities.txt";
            try {
                PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(filename)));
                for(MultimodalSimilarity positiveMultimodalSimilarity : positiveMultimodalSimilarities)
                	positiveMultimodalSimilarity.saveToFile(pw);
                pw.close();
            }
            catch(IOException e) {
            }
        }
    }

    public void saveNegativeMultimodalSimilarities(String modelDirectory) {
    	if(!modelDirectory.endsWith(File.separator))
    		modelDirectory = modelDirectory + File.separator;
    	
        if(negativeMultimodalSimilarities != null) {
            String filename=modelDirectory +"negative_similarities.txt";
            try {
                PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(filename)));
                for(MultimodalSimilarity negativeMultimodalSimilarity : negativeMultimodalSimilarities)
                	negativeMultimodalSimilarity.saveToFile(pw);
                pw.close();
            }
            catch(IOException e) {
            }
        }
    }

    public ArrayList<Attribute> getAttributes() {
        return attributes;
    }

    public static enum ClassifierTypes {
    	svm,
    	decisionTree,
    	part,
    	jrip,
    	randomTree,
    	randomForest,
    	MultilayerPerceptron,
    	NaiveBayes,
    	NearestNeighbour
    };
    
    public static String SVM_PARAMETERS = "-C 1 -M";
    public static String DECISION_TREE_PARAMETERS = "-M 5";
    public static String PART_PARAMETERS = "";
    public static String JRIP_PARAMETERS = "";
    public static String RANDOM_TREE_PARAMETERS = "";
    public static String RANDOM_FOREST_PARAMETERS = "";
    public static String MULTILAYER_PERCEPTRON_PARAMETERS = "";
    public static String NAIVE_BAYES_PARAMETERS = "";
    public static String NEAREST_NEIGHBOUR_PARAMETERS = "";
    
    public static void main(String...args) {
    	ATTRIBUTES[] attributes = {ATTRIBUTES.SAME_USER, ATTRIBUTES.TIME_TAKEN_DAY_DIFF_3, 
    			ATTRIBUTES.TIME_TAKEN_HOUR_DIFF_12, ATTRIBUTES.TIME_TAKEN_HOUR_DIFF_24, ATTRIBUTES.TIME_TAKEN,
    			ATTRIBUTES.DESCRIPTION_COSINE, ATTRIBUTES.TAGS_COSINE, ATTRIBUTES.TITLE_COSINE};
    	
		MultimodalClassifier sem = new MultimodalClassifier(0, 0, attributes);
		
		sem.load("/second_disk/workspace/yahoogc/model.svm");
		
    }
}