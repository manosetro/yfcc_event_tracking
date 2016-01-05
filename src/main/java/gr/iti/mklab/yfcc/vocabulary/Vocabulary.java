package gr.iti.mklab.yfcc.vocabulary;

import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.models.TFIDFVector;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

public class Vocabulary {

	private double b = 2.;
	private long documents = 0;
	private Map<String, TermInfo> voc;
	private HashSet<String> boostedTerms;
	
	public Vocabulary() {
		voc = new HashMap<String, TermInfo>();
		boostedTerms = new HashSet<String>();
	}
	
//	static Vocabulary vocabularySingleton = null;
//	public static Vocabulary getInstance() {
//		if(vocabularySingleton == null) {
//			vocabularySingleton = new Vocabulary();
//		}
//		return vocabularySingleton;
//	}
	
	public void addTerm(String term) {
		if(voc.containsKey(term)) {
			voc.get(term).inc();
		}
		else {
			voc.put(term, new TermInfo(voc.size(), 1L));
		}
	}
	
	public void addTerm(String term, long df) {
		if(voc.containsKey(term)) {
			voc.get(term).inc(df);
		}
		else {
			voc.put(term, new TermInfo(voc.size(), df));
		}
	}
	
	public void addBoostedTerms(Collection<String> terms) {
		boostedTerms.addAll(terms);
	}
	
	public double idf(String term) {
		TermInfo info = voc.get(term);
		if(info==null) {
			return 0;
		}
		
		double idf = Math.log10(((double)documents) / ((double)info.df)); 
		if(boostedTerms.contains(term)) {
			idf = b * idf;
		}
		return idf;
	}
	
	public int index(String term) {
		TermInfo info = voc.get(term);
		if(info == null) {
			return -1;
		}
		
		return info.index;
	}
	
	public void update(String terms[]) {
		documents++;
		for(String term : terms) {
			addTerm(term);
		}
	}

	public void merge(Vocabulary vocabulary) {
		this.documents += vocabulary.documents;
		
		for(Entry<String, TermInfo> vocEntry : vocabulary.voc.entrySet()) {
			String term = vocEntry.getKey();
			TermInfo termInfo = vocEntry.getValue();
			
			addTerm(term, termInfo.df);
		}
		
		this.addBoostedTerms(vocabulary.boostedTerms);
	}
	
	public void update(List<String> terms) {
		documents++;
		for(String term : terms) {
			addTerm(term);
		}
	}
    
	public void update(Set<String> terms) {
		documents++;
		for(String term : terms) {
			addTerm(term);
		}
	}
	
        
	public long documents() {
		return documents;
	}
	
	private static class TermInfo {
		
		public int index;
		public long df;
		
		public TermInfo(int index, long df) {
			this.index = index;
			this.df = df;
		}
		
		public void inc() {
			df++;
		}
		
		public void inc(long df) {
			this.df += df;
		}
	}
	
	public int size() {
		return voc.size();
	}
	
	public void writeToFile(String outputFile) throws IOException {
		File f = new File(outputFile);
		File p = f.getParentFile();
		if(!p.exists()) {
			p.mkdirs();
		}
		
		BufferedWriter outputFileWriter = new BufferedWriter( new OutputStreamWriter(new FileOutputStream(f), "UTF8"));
		
		outputFileWriter.append(documents + "\n");
		for(Entry<String, TermInfo> cluster : voc.entrySet()) {
			StringBuffer strbf = new StringBuffer();
			
			String term = cluster.getKey();
			strbf.append(new String(term.getBytes(), "UTF8") + "\t");
			strbf.append(cluster.getValue().df + "\t");
			strbf.append(cluster.getValue().index + "\t");
			if(boostedTerms.contains(term)) {
				strbf.append("1");
			}
			else {
				strbf.append("0");
			}
			strbf.append("\n");
			try {
				outputFileWriter.write(strbf.toString());
			} catch (IOException e) {
				continue;
			}
		}
		outputFileWriter.close();
	}
	
	public static Vocabulary loadFromFile(String inputFile) throws IOException {
		Vocabulary vocabulary = new Vocabulary();
		BufferedReader inputFileReader = new BufferedReader( new InputStreamReader( 
				new FileInputStream(inputFile), "UTF8"));
		
		String line = inputFileReader.readLine();
		vocabulary.documents = Integer.parseInt(line);
		while( (line =inputFileReader.readLine()) != null) {
			String[] parts = line.split("\t");
			
			TermInfo info = new TermInfo(Integer.parseInt(parts[2]), Long.parseLong(parts[1]));
			vocabulary.voc.put(parts[0], info);
			
			// add boosted terms (named entities)
			if(parts[3].equals("1")) {
				vocabulary.boostedTerms.add(parts[0]);
			}
		}
		
		inputFileReader.close();
		return vocabulary;
	}

	public static Map<String, Vocabulary> createMultipleVocabularies(List<Item> items) {
		
		Vocabulary titleVocabulary = new Vocabulary();
		Vocabulary descVocabulary = new Vocabulary();
		Vocabulary tagsVocabulary = new Vocabulary();
		Vocabulary neVocabulary = new Vocabulary();
		
		Iterator<Item> it = items.iterator();
		while(it.hasNext()){
			Item item = it.next();
			
			List<String> titleTerms = item.getTitleTerms();
			if(titleTerms != null && !titleTerms.isEmpty()) {
				titleVocabulary.update(titleTerms);
			}
			List<String> descTerms = item.getDescriptionTerms();
			if(descTerms != null && !descTerms.isEmpty()) {
				descVocabulary.update(descTerms);
			}
			List<String> tagsTerms = item.getTagsTerms();
			if(tagsTerms != null && !tagsTerms.isEmpty()) {
				tagsVocabulary.update(tagsTerms);
			}	
			
			List<String> namedEntities = item.getNamedEntities();
			if(namedEntities != null && !namedEntities.isEmpty()) {
				neVocabulary.update(namedEntities);
			}	
		}
		
		Map<String, Vocabulary> vocabularies = new HashMap<String, Vocabulary>();
		vocabularies.put("title", titleVocabulary);
		vocabularies.put("description", descVocabulary);
		vocabularies.put("tags", tagsVocabulary);
		vocabularies.put("namedEntities", neVocabulary);
		
		return vocabularies;
	}
	
	public static Vocabulary createVocabulary(List<Item> items, boolean useNamedEntities) {
		Vocabulary vocabulary = new Vocabulary();
		Iterator<Item> it = items.iterator();
		while(it.hasNext()){
			Item item = it.next();
			
			Set<String> tokens = new HashSet<String>();
			List<String> titleTerms = item.getTitleTerms();
			if(titleTerms != null && !titleTerms.isEmpty()) {
				tokens.addAll(titleTerms);
			}
			List<String> descTerms = item.getDescriptionTerms();
			if(descTerms != null && !descTerms.isEmpty()) {
				tokens.addAll(descTerms);
			}
			List<String> tagsTerms = item.getTagsTerms();
			if(tagsTerms != null && !tagsTerms.isEmpty()) {
				tokens.addAll(tagsTerms);
			}
			
			if(useNamedEntities) {
				List<String> namedEntities = item.getNamedEntities();
				if(namedEntities != null && !namedEntities.isEmpty()) {
					tokens.addAll(namedEntities);
				}
				vocabulary.addBoostedTerms(namedEntities);
			}
			
			
			// Add new document in the vocabulary
			if(!tokens.isEmpty()) {
				vocabulary.update(tokens);
			}
		}
		return vocabulary;
	}
         
	public static Vocabulary createVocabularyWithTokenization(List<Item> items) {
		Vocabulary vocabulary = new Vocabulary();
		Iterator<Item> it = items.iterator();
		while(it.hasNext()){
			Item item = it.next();
			
			Set<String> tokens = new HashSet<String>();
			
			String title = item.getTitle();
			if(title != null && !title.isEmpty()) {
				StringTokenizer tokenizer = new StringTokenizer(title);
				while(tokenizer.hasMoreTokens()) {
					tokens.add(tokenizer.nextToken());
				}
			}
			
			String desc = item.getDescription();
			if(desc != null && !desc.isEmpty()) {
				StringTokenizer tokenizer = new StringTokenizer(desc);
				while(tokenizer.hasMoreTokens()) {
					tokens.add(tokenizer.nextToken());
				}
			}
			
			List<String> tags = item.getTags();
			if(tags != null && !tags.isEmpty()) {
				String tagsLine = StringUtils.join(tags, " ");
				StringTokenizer tokenizer = new StringTokenizer(tagsLine);
				while(tokenizer.hasMoreTokens()) {
					tokens.add(tokenizer.nextToken());
				}
			}
			vocabulary.update(tokens);
		}
		return vocabulary;
	}
	
	public void loadItemVectors(Item item) {
		
		TFIDFVector textVector = new TFIDFVector();
				
		List<String> titleTerms = item.getTitleTerms();
		if(titleTerms != null && !titleTerms.isEmpty()) {
			TFIDFVector titleVector = getTFIDFVector(titleTerms);
			if(titleVector != null) {
				textVector.mergeVector(titleVector);
			}
			item.setTitleVector(titleVector);
		}
		
		List<String> descTerms = item.getDescriptionTerms();
		if(descTerms != null && !descTerms.isEmpty()) {
			TFIDFVector descVector = getTFIDFVector(descTerms);
			if(descVector != null) {
				textVector.mergeVector(descVector);
			}
			item.setDescriptionVector(descVector);
		}

		List<String> tagsTerms = item.getTagsTerms();
		if(tagsTerms != null && !tagsTerms.isEmpty()) {
			TFIDFVector tagsVector = getTFIDFVector(tagsTerms);
			if(tagsVector != null) {
				textVector.mergeVector(tagsVector);
			}
			item.setTagsVector(tagsVector);
		}
		
		if(!textVector.isEmpty()) {
			item.setTextVector(textVector);
		}
	}
	
	private TFIDFVector getTFIDFVector(List<String> terms) {
		TFIDFVector vector = new TFIDFVector();
		for(String term : terms) {
			if(term.length()<3) {
				continue;
			}
			if(term.contains(".") || term.contains(":")) {
				continue;
			}
			if(NumberUtils.isNumber(term)) {
				continue;
			}
				
			double idf = idf(term);
			vector.update(term, 1, idf);
		}
		return vector;
	}

	public static void main(String...args) throws IOException {
		String inputFile = "/second_disk/workspace/yahoogc/vocabulary.txt";
		String outputFile = "/second_disk/workspace/yahoogc/vocabulary_correct.txt";
		BufferedReader inputFileReader = new BufferedReader( new InputStreamReader(new FileInputStream(inputFile), "UTF8"));
		BufferedWriter outputFileWriter = new BufferedWriter( new OutputStreamWriter(new FileOutputStream(outputFile), "UTF8"));
		
		String line = inputFileReader.readLine();
		outputFileWriter.write(line + "\n");
		while( (line =inputFileReader.readLine()) != null) {
			String[] parts = line.split(" ");
			if(parts.length <= 4) {
				outputFileWriter.write(StringUtils.join(parts, "\t"));
			}
			else {
				String newline = StringUtils.join(parts, " ", 0, parts.length-3) + " \t" 
						+ parts[parts.length-3] + " \t"
						+ parts[parts.length-2] + " \t"
						+ parts[parts.length-1] + " \t";
					
				outputFileWriter.write(newline);
			}
			outputFileWriter.write("\n");
			
		}
		inputFileReader.close();
		outputFileWriter.close();
	}
	
}
