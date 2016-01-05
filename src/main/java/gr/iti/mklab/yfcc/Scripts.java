package gr.iti.mklab.yfcc;

import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.models.NamedEntity;
import gr.iti.mklab.yfcc.nlp.EntitiesExtractor;
import gr.iti.mklab.yfcc.utils.FileIterator;
import gr.iti.mklab.yfcc.vocabulary.Vocabulary;
import gr.iti.mklab.yfcc.dao.SolrItemClient;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.solr.client.solrj.SolrServerException;

public class Scripts {
	
	static int timeslotLength = 24;
	static SolrItemClient solrClient = new SolrItemClient("http://160.40.51.16:8983/solr/items");
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	
	static String serializedClassifier3Class = "edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz";		
	static EntitiesExtractor entitiesExtractor = new EntitiesExtractor(serializedClassifier3Class);
	
	public static void main(String[] argv) throws IOException, ParseException {		
		//indexDirectoryToSol("/second_disk/yfcc100m/raw/");
		
		//updateTerms();
		//multithreadedUpdateTerms();
		
		createVocabulary();
	}
	
	public static void indexDirectoryToSol(String dir) throws IOException, ParseException {
		File directory = new File(dir);
		for(File file : directory.listFiles()) {
			indexFileToSolr(file);	
		}	
	}
	
	public static void indexFileToSolr(File file) throws IOException, ParseException {
			
		Date sinceDate = sdf.parse("2006-01-01T00:00:00Z");
		Date endDate = sdf.parse("2015-01-01T00:00:00Z");	
			
		FileIterator iterator = new FileIterator(file);
		int k = 0;
		List<Item> items = new ArrayList<Item>();
		while(iterator.hasNext()) {	
			// index & commit every 10000 items
			if((++k)%10000 == 0) {
				System.out.println(k + " items processed from " + file.getName());
				try {
					solrClient.index(items);
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				items.clear();
			}	
			
			Item item = iterator.next();
			if(item != null) {
				Date takenDate = item.getTakenDate();
				Date uploadDate = item.getUploadDate();
				// Add items in the specified time range
				if(takenDate.after(sinceDate) && takenDate.before(endDate) && 
						uploadDate.after(sinceDate) && uploadDate.before(endDate)) {
					
					Collection<NamedEntity> entities = entitiesExtractor.extractItemEntities(item);
					
					List<String> persons = new ArrayList<String>();
					List<String> organizations = new ArrayList<String>();
					List<String> locations = new ArrayList<String>();	
					for(NamedEntity entity : entities) {
						if(entity.getType().equals("PERSON")) {
							persons.add(entity.getName());
						}
						else if(entity.getType().equals("ORGANIZATION")) {
							organizations.add(entity.getName());
						}
						else if(entity.getType().equals("LOCATION")) {
							locations.add(entity.getName());
						}
					}
					
					item.setPersons(persons);
					item.setOrganizations(organizations);
					item.setLocations(locations);
					
					items.add(item);
				}
				
			}
		}
		
		if(!items.isEmpty()) {
			try {
				solrClient.index(items);
				solrClient.commit();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void updateTerms() throws ParseException, IOException {

		Date endDate = sdf.parse("2015-01-01T00:00:00Z");	
		
		Date sinceDate = sdf.parse("2006-01-01T00:00:00Z");
		Date untilDate = DateUtils.addHours(sinceDate, timeslotLength);	
		
		while(sinceDate.before(endDate)) {	
			try {
				List<Item> items = solrClient.getInRange(sinceDate, untilDate);	
				System.out.println(sinceDate + " - " + untilDate + " => " + items.size() + " items.");
				for(Item item : items) {
					try {
						solrClient.loadItemVectors(item);
						if(items.indexOf(item)%500==0) {
							System.out.print(".");
						}
					}
					catch(Exception e) {
						e.printStackTrace();
					}
				}
				System.out.println();
				
				for(Item item : items) {
					Map<String, Object> fieldsToUpdate = new HashMap<>();
					if(item.getTitleVector() != null && !item.getTitleVector().isEmpty()) {
						fieldsToUpdate.put("titleTerms", item.getTitleVector().keySet());
					}
					if(item.getDescriptionVector() != null && !item.getDescriptionVector().isEmpty()) {
						fieldsToUpdate.put("descriptionTerms", item.getDescriptionVector().keySet());
					}
					if(item.getTagsVector() != null && !item.getTagsVector().isEmpty()) {
						fieldsToUpdate.put("tagsTerms", item.getTagsVector().keySet());
					}
					System.out.println(fieldsToUpdate);
					if(!fieldsToUpdate.isEmpty()) {
						solrClient.update(item.getId(), fieldsToUpdate);
					}
				}
				solrClient.commit();
				
			} catch (SolrServerException ex) {
				ex.printStackTrace();
			}	
			
			sinceDate = DateUtils.addHours(sinceDate, timeslotLength);
			untilDate = DateUtils.addHours(sinceDate, timeslotLength);	
		}		
	}
	
	public static void multithreadedUpdateTerms() throws ParseException, IOException {
		
		List<TermsUpdater> runnables = new ArrayList<TermsUpdater>();
		//runnables.add(new TermsUpdater("2011-11-01T00:00:00Z", "2011-11-10T00:00:00Z"));
		//runnables.add(new TermsUpdater("2011-11-10T00:00:00Z", "2011-11-20T00:00:00Z"));
		//runnables.add(new TermsUpdater("2011-11-20T00:00:00Z", "2011-12-01T00:00:00Z"));
		//runnables.add(new TermsUpdater("2011-12-01T00:00:00Z", "2011-12-10T00:00:00Z"));
		//runnables.add(new TermsUpdater("2011-12-10T00:00:00Z", "2011-12-20T00:00:00Z"));
		//runnables.add(new TermsUpdater("2011-12-20T00:00:00Z", "2011-12-24T00:00:00Z"));
		//runnables.add(new TermsUpdater("2011-12-24T00:00:00Z", "2011-12-25T00:00:00Z"));
		//runnables.add(new TermsUpdater("2011-12-25T00:00:00Z", "2011-12-26T00:00:00Z"));
		//runnables.add(new TermsUpdater("2011-12-26T00:00:00Z", "2012-01-01T00:00:00Z"));
	
		//runnables.add(new TermsUpdater("2008-01-01T00:00:00Z", "2009-12-31T00:00:00Z"));
		//runnables.add(new TermsUpdater("2010-01-01T00:00:00Z", "2011-12-31T00:00:00Z"));
		//runnables.add(new TermsUpdater("2012-01-01T00:00:00Z", "2012-12-31T00:00:00Z"));
		//runnables.add(new TermsUpdater("2013-01-01T00:00:00Z", "2014-12-31T00:00:00Z"));
		
		runnables.add(new TermsUpdater("2006-12-31T00:00:00Z", "2007-01-01T00:00:00Z"));
		runnables.add(new TermsUpdater("2007-12-31T00:00:00Z", "2008-01-01T00:00:00Z"));
		runnables.add(new TermsUpdater("2008-12-31T00:00:00Z", "2009-01-01T00:00:00Z"));
		runnables.add(new TermsUpdater("2009-12-31T00:00:00Z", "2010-01-01T00:00:00Z"));
		runnables.add(new TermsUpdater("2010-12-31T00:00:00Z", "2011-01-01T00:00:00Z"));
		//runnables.add(new TermsUpdater("2011-12-31T00:00:00Z", "2012-01-01T00:00:00Z"));
		runnables.add(new TermsUpdater("2012-12-31T00:00:00Z", "2013-01-01T00:00:00Z"));
		runnables.add(new TermsUpdater("2013-12-31T00:00:00Z", "2014-01-01T00:00:00Z"));
		
		List<Thread> threads = new ArrayList<Thread>();
		for(TermsUpdater updater : runnables) {
			Thread thread = new Thread(updater);
			threads.add(thread);
		}
		
		for(Thread thread : threads) {
			thread.start();
		}
		
		while(true) {
			int dead = 0;
			for(Thread thread : threads) {
				if(!thread.isAlive()) {
					dead++;
				}
			}
			
			if(dead == threads.size()) {
				break;
			}
			
			try {
				System.out.println(dead + " dead threads!");
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		
	}
	
	public static class TermsUpdater implements Runnable {
		
		SolrItemClient client = new SolrItemClient("http://160.40.51.16:8983/solr/items");
		
		private Date endDate;
		private Date sinceDate;
		private Date untilDate;
		private int timeslot = 8;
		
		public TermsUpdater(String since, String end) throws ParseException {
			this.endDate = sdf.parse(end);	
			this.sinceDate = sdf.parse(since);
			this.untilDate = DateUtils.addHours(sinceDate, timeslot);	
		}
		
		@Override
		public void run() {
			while(sinceDate.before(endDate)) {	
				try {
					List<Item> items = client.getInRange(sinceDate, untilDate);	
					System.out.println(Thread.currentThread().getName() + " => " + sinceDate + " - " + untilDate + " => " + items.size() + " items.");
					for(Item item : items) {
						try {
							client.loadItemVectors(item);
							//if(items.indexOf(item)%500==0) {
								//System.out.print(".");
							//}
						}
						catch(Exception e) {
							e.printStackTrace();
						}
					}
					System.out.println(Thread.currentThread().getName() + " load vectors");
					
					for(Item item : items) {
						Map<String, Object> fieldsToUpdate = new HashMap<>();
						if(item.getTitleVector() != null && !item.getTitleVector().isEmpty()) {
							fieldsToUpdate.put("titleTerms", item.getTitleVector().keySet());
						}
						if(item.getDescriptionVector() != null && !item.getDescriptionVector().isEmpty()) {
							fieldsToUpdate.put("descriptionTerms", item.getDescriptionVector().keySet());
						}
						if(item.getTagsVector() != null && !item.getTagsVector().isEmpty()) {
							fieldsToUpdate.put("tagsTerms", item.getTagsVector().keySet());
						}

						if(!fieldsToUpdate.isEmpty()) {
							client.update(item.getId(), fieldsToUpdate);
						}
					}
					client.commit();
					
				} catch (SolrServerException | IOException ex) {
					ex.printStackTrace();
				}	
				
				sinceDate = DateUtils.addHours(sinceDate, timeslot);
				untilDate = DateUtils.addHours(sinceDate, timeslot);
			}
		}
		
	}
	
	public static void createVocabulary() throws ParseException, IOException {
		
		Date endDate = sdf.parse("2015-06-01T00:00:00Z");
		
		Date sinceDate = sdf.parse("2006-01-01T00:00:00Z");
		Date untilDate = DateUtils.addHours(sinceDate, timeslotLength);	
		
		Vocabulary vocabulary = new Vocabulary();
		while(sinceDate.before(endDate)) {
			try {
				List<Item> items = solrClient.getInRange(sinceDate, untilDate);
				System.out.println(sinceDate + " - " + untilDate + " => " + items.size() + " items.");
				
				if(!items.isEmpty()) {
					// Create time-slot vocabulary 
					Vocabulary timeslotVocabulary = Vocabulary.createVocabulary(items, true);
					timeslotVocabulary.writeToFile("/second_disk/workspace/yahoogc/vocabularies/" + sinceDate.getTime() + ".txt");
					
					// merge time-slot vocabulary to the global vocabulary
					vocabulary.merge(timeslotVocabulary);
					
					System.out.println("#documents: " + vocabulary.documents());
					System.out.println("#terms: " + vocabulary.size());
					System.out.println("=============================================");
				}
				
			} catch (SolrServerException ex) {
				ex.printStackTrace();
			}
			
			sinceDate = DateUtils.addHours(sinceDate, timeslotLength);
			untilDate = DateUtils.addHours(sinceDate, timeslotLength);	
		}
		vocabulary.writeToFile("/second_disk/workspace/yahoogc/vocabulary.txt");
	}
	
	public static void createMultipleVocabularies() throws ParseException, IOException {
		
		Date endDate = sdf.parse("2015-06-01T00:00:00Z");
		
		Date sinceDate = sdf.parse("2006-01-01T00:00:00Z");
		Date untilDate = DateUtils.addHours(sinceDate, timeslotLength);	
		
		Map<String, Vocabulary> vocabularies = new HashMap<>();
		vocabularies.put("title", new Vocabulary());
		vocabularies.put("description", new Vocabulary());
		vocabularies.put("tags", new Vocabulary());
		vocabularies.put("namedEntities", new Vocabulary());
		
		while(sinceDate.before(endDate)) {
			try {
				List<Item> items = solrClient.getInRange(sinceDate, untilDate);
				System.out.println(sinceDate + " - " + untilDate + " => " + items.size() + " items.");
				
				if(!items.isEmpty()) {
					// Create time-slot vocabulary 
					Map<String, Vocabulary> timeslotVocabularies = Vocabulary.createMultipleVocabularies(items);
					for(String key : timeslotVocabularies.keySet()) {
						Vocabulary timeslotVocabulary = timeslotVocabularies.get(key);
						timeslotVocabulary.writeToFile("/second_disk/workspace/yahoogc/vocabularies/" + key + "/" + sinceDate.getTime() + ".txt");
						
						Vocabulary vocabulary = vocabularies.get(key);
						// merge time-slot vocabulary to the global vocabulary
						vocabulary.merge(timeslotVocabulary);
						
						System.out.println(key + " #documents: " + vocabulary.documents());
						System.out.println(key + " #terms: " + vocabulary.size());
					}
					System.out.println("=============================================");
				}
				
			} catch (SolrServerException ex) {
				ex.printStackTrace();
			}
			sinceDate = DateUtils.addHours(sinceDate, timeslotLength);
			untilDate = DateUtils.addHours(sinceDate, timeslotLength);	
		}
		
		for(String key : vocabularies.keySet()) {
			Vocabulary vocabulary = vocabularies.get(key);
			vocabulary.writeToFile("/second_disk/workspace/yahoogc/" + key + "-vocabulary.txt");
		}
		
	}

}

