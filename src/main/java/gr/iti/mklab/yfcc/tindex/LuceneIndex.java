package gr.iti.mklab.yfcc.tindex;

import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.models.TFIDFVector;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.Version;

public class LuceneIndex {
	
	private TFIDFSimilarity similarity = new DefaultSimilarity();
	
	private static Analyzer analyzer = new Analyzer() {
		@Override
		protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
			Tokenizer source = new StandardTokenizer(Version.LUCENE_46, reader);
			TokenStream filter = new  LowerCaseFilter(Version.LUCENE_46, source);
			StopFilter stopFilter = new StopFilter(Version.LUCENE_46, filter, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
			
			return new TokenStreamComponents(source, stopFilter);
		}
	};
	
	private static Analyzer ngramAnalyzer = new Analyzer() {
		@Override
		protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
			Tokenizer source = new StandardTokenizer(Version.LUCENE_46, reader);
			TokenStream filter = new  LowerCaseFilter(Version.LUCENE_46, source);
			ShingleFilter ngramFilter = new ShingleFilter(filter, 2, 2);
			ngramFilter.setOutputUnigrams(false);
			
			return new TokenStreamComponents(source, ngramFilter);
		}
	};
	
	private static Analyzer entitiesAnalyzer = new Analyzer() {
		@Override
		protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
			Tokenizer source = new KeywordTokenizer(reader);
			TokenStream filter = new  LowerCaseFilter(Version.LUCENE_46, source);			
			return new TokenStreamComponents(source, filter);
		}
	};
	
	private static Map<String, Analyzer> analyzersPerField = new HashMap<String, Analyzer>();
	{
		analyzersPerField.put("text", analyzer);
		analyzersPerField.put("ngrams", ngramAnalyzer);
		analyzersPerField.put("person", entitiesAnalyzer);
		analyzersPerField.put("location", entitiesAnalyzer);
		analyzersPerField.put("organization", entitiesAnalyzer);
	}
	private static PerFieldAnalyzerWrapper analyzersWrapper = new PerFieldAnalyzerWrapper(analyzer, analyzersPerField);
	
	private String indexDir;
	private IndexSearcher searcher;

	private IndexWriter iwriter = null;

	private IndexReader reader;
	
	public LuceneIndex(String indexDir) throws IOException {
		this.indexDir = indexDir;
	}
	
	public void open() {
		try {
			FSDirectory dir = FSDirectory.open(new File(indexDir));
			reader = DirectoryReader.open(dir);
			searcher = new IndexSearcher(reader);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Map<String, Double> search(String text) {
		Map<String, Double> similar = new HashMap<String, Double>();
		try {
			TokenStream tokenStream = analyzer.tokenStream("text", text);
			CharTermAttribute charTermAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			BooleanQuery bQuery = new BooleanQuery();
			while (tokenStream.incrementToken()) {
				String token = charTermAtt.toString();
				TermQuery tq = new TermQuery(new Term("text", token));
				
				bQuery.add(tq, Occur.SHOULD);
			}
			tokenStream.close();
			
			TopDocs results = searcher.search(bQuery, 100);
			ScoreDoc[] hits = results.scoreDocs;
			for(ScoreDoc hit : hits) {				
				Document doc = searcher.doc(hit.doc);
				similar.put(doc.get("id"), new Double(hit.score));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return similar;
	}
	
	public Map<String, Double> expandedSearch(String text) {
		Map<String, Double> similar = new HashMap<String, Double>();
		try {
			TermQuery query = new TermQuery(new Term("text", text));
			
			TopDocs results = searcher.search(query, 100000);
			ScoreDoc[] hits = results.scoreDocs;
			for(ScoreDoc hit : hits) {				
				Document doc = searcher.doc(hit.doc);
				similar.put(doc.get("id"), new Double(hit.score));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return similar;
	}
	
	public Map<String, Double> searchIndex(Item item, int n) throws IOException {
		Map<String, Double> similar = new HashMap<String, Double>();
		
		QueryBuilder builder = new QueryBuilder(analyzer);
		try {
			String textQuery = item.getText();
			//textQuery = StringUtils.clean(textQuery, item.getUrls());
			textQuery = textQuery.replaceAll("\\r\\n|\\r|\\n", " ");	
			textQuery = QueryParser.escape(textQuery);
	    	
			org.apache.lucene.search.Query luceneQuery = 
					builder.createMinShouldMatchQuery("text", textQuery, 0.6f);
			
			TopDocs topDocs = searcher.search(luceneQuery, n);
			for(ScoreDoc scoredDoc : topDocs.scoreDocs) {
				Document document = searcher.doc(scoredDoc.doc);
				String id = document.get("id");
				if(id != null) {
					similar.put(document.get("id"), (double) scoredDoc.score);
				}
			}
		} catch (Exception e) { }
		
		return similar;
	}
	
	public List<Document> searchDocuments(String text) {
		List<Document> documents = new ArrayList<Document>();
		try {
			TokenStream tokenStream = analyzer.tokenStream("text", text);
			CharTermAttribute charTermAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			BooleanQuery bQuery = new BooleanQuery();
			while (tokenStream.incrementToken()) {
				String token = charTermAtt.toString();
				TermQuery tq = new TermQuery(new Term("text", token));
				tq.setBoost(2f);
				
				bQuery.add(tq, Occur.MUST);
			}
			tokenStream.close();
			
			TopDocs results = searcher.search(bQuery, 100000);
			ScoreDoc[] hits = results.scoreDocs;
			for(ScoreDoc hit : hits) {				
				Document doc = searcher.doc(hit.doc);
				doc.add(new FloatField("score", hit.score, FloatField.TYPE_STORED));
				documents.add(doc);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return documents;
	}
	
	private void initializeIndex() {
		try {
			Directory directory = FSDirectory.open(new File(indexDir));
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzersWrapper);
	    	iwriter = new IndexWriter(directory, config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void finalizeIndex() {
		if(iwriter != null) {
			try {
				iwriter.commit();
				iwriter.close();
				iwriter = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void index(Collection<Item> items) throws IOException {
		index( items.iterator());
	}
	
	public void index(Iterator<Item> iterator) throws IOException {
		initializeIndex();
		
	    int k = 0;
		while(iterator.hasNext()) {
	    	if(++k%500==0) {
	    		System.out.print(".");
	    		if(k%10000==0) {
	    			System.out.println(" ("+k+")");
	    		}
	    	}
	    	Item item = iterator.next();
	    	if(item != null) {
	    		index(item);
	    	}
	    }
		finalizeIndex();
	}
	
	private void index(Item item) throws IOException {
		String id = item.getId();
    	
		String title = item.getTitle();
		String description = item.getDescription();
    	Date takenDate = item.getTakenDate();
    	Date uploadDate = item.getUploadDate();
    	
    	Document document = new Document();
		
		Field idField = new StringField("id", id, Store.YES);
    	document.add(idField);
		
		FieldType fieldType = new FieldType();
		fieldType.setStored(true);
		fieldType.setIndexed(true);
		fieldType.setStoreTermVectors(true);
		
		if(title != null) {
			document.add(new Field("title", title, fieldType));
		}
		
		if(description != null) {
			document.add(new Field("description", description, fieldType));
		}
		
		document.add(new LongField("takenDate", takenDate.getTime(), LongField.TYPE_STORED));
		document.add(new LongField("uploadDate", uploadDate.getTime(), LongField.TYPE_STORED));
		
		
		FieldType NEType = new FieldType();
		NEType.setIndexed(true);
		NEType.setOmitNorms(true);
		NEType.setIndexOptions(IndexOptions.DOCS_ONLY);
		NEType.setStored(true);
		NEType.setTokenized(false);
		NEType.setStoreTermVectors(true);
		if(item.getPersons() != null) {
			for(String person : item.getPersons()) {
				Field field = new Field("person", person, NEType);
				document.add(field);
			}
		}
		if(item.getOrganizations() != null) {
			for(String organization : item.getOrganizations()) {
				Field field = new Field("organization", organization, NEType);
				document.add(field);
			}
		}
		if(item.getLocations() != null) {
			for(String location : item.getLocations()) {
				Field field = new Field("location", location, NEType);
				document.add(field);
			}
		}
		
		if(iwriter != null) {
			iwriter.addDocument(document);
		}
	}
	
	public int count() {
		if(reader != null)
			return reader.numDocs();
		else {
			return -1;
		}
	}
	
	private double idf(String term, String field) {
		try {
			// TOO SLOW!!!
			//TopDocs docs = searcher.search(new WildcardQuery(new Term(field, "*")), Integer.MAX_VALUE);
			//long numDocs = docs.totalHits;
			
			long numDocs = reader.numDocs();
			long docFreq = reader.docFreq(new Term(field, term));
			
			double idf = (double) similarity.idf(docFreq, numDocs);
			return idf;
		} catch (IOException e) {
			return .0;
		}
	}
	
	public Document getDoc(String id) throws IOException {
		TermQuery tq = new TermQuery(new Term("id", id));
		TopDocs results = searcher.search(tq, 1);
		ScoreDoc[] hits = results.scoreDocs;
		if(hits.length > 0) {
			Document doc = searcher.doc(hits[0].doc);
			return doc;
		}
		else {
			return null;
		}
	}
	

	public List<String> getDocumentTerms(String id) throws IOException {
		List<String> terms = new ArrayList<String>();
		try {
			TermQuery tq = new TermQuery(new Term("id", id));
			TopDocs results = searcher.search(tq, 1);
			ScoreDoc[] hits = results.scoreDocs;
			if(hits.length > 0) {
				int docID = hits[0].doc;
				Terms tv = reader.getTermVector(docID, "text");
				if(tv != null) {
					TermsEnum termsEnum = tv.iterator(null);
			    	while(termsEnum.next() != null) {
			    		BytesRef term = termsEnum.term();			
			    		if(term != null) {
			    			terms.add(term.utf8ToString());
			    		}
			    	}
				}
			}
		}
		catch(Exception e) {}
		return terms;
	}
	
	public TFIDFVector getTfIdfVector(String id) throws IOException {
		TFIDFVector vector = new TFIDFVector();
		try {
			TFIDFVector titleVector = getTfIdfVector(id, "title");
			if(titleVector != null) {
				vector.mergeVector(titleVector);
			}
		
			TFIDFVector descriptionVector = getTfIdfVector(id, "description");
			if(descriptionVector != null) {
				vector.mergeVector(descriptionVector);
			}
		
			TFIDFVector personsVector = getTfIdfVector(id, "persons");
			if(personsVector != null) {
				vector.mergeVector(personsVector);
			}
		
			TFIDFVector locationsVector = getTfIdfVector(id, "locations");
			if(locationsVector != null) {
				vector.mergeVector(locationsVector);
			}
		
			TFIDFVector organizationsVector = getTfIdfVector(id, "organizations");
			if(organizationsVector != null) {
				vector.mergeVector(organizationsVector);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		vector.computeLength();

		return vector;
	}
	
	public TFIDFVector getTfIdfVector(int docID) throws IOException {
		TFIDFVector vector = new TFIDFVector();
		try {
			TFIDFVector titleVector = getTfIdfVector(docID, "title");
			if(titleVector != null) {
				vector.mergeVector(titleVector);
			}
		
			TFIDFVector descriptionVector = getTfIdfVector(docID, "description");
			if(descriptionVector != null) {
				vector.mergeVector(descriptionVector);
			}
		
			TFIDFVector personsVector = getTfIdfVector(docID, "persons");
			if(personsVector != null) {
				vector.mergeVector(personsVector);
			}
		
			TFIDFVector locationsVector = getTfIdfVector(docID, "locations");
			if(locationsVector != null) {
				vector.mergeVector(locationsVector);
			}
		
			TFIDFVector organizationsVector = getTfIdfVector(docID, "organizations");
			if(organizationsVector != null) {
				vector.mergeVector(organizationsVector);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		vector.computeLength();

		return vector;
	}
	
	public TFIDFVector getTfIdfVector(String id, String field) throws IOException {
		if(field == null) {
			return getTfIdfVector(id);
		}
		
		TFIDFVector vector = new TFIDFVector();
		try {
			field = field.toLowerCase().trim();
			TermQuery tq = new TermQuery(new Term("id", id));
			TopDocs results = searcher.search(tq, 1);
			ScoreDoc[] hits = results.scoreDocs;
			if(hits.length > 0) {
				int docID = hits[0].doc;
				Map<String, Pair<Double, Double>> termsMap = getTermsMap(docID, field);
				for(String term : termsMap.keySet()) {
					Pair<Double, Double> tfidf = termsMap.get(term);
					vector.addTerm(term, tfidf.getLeft(), tfidf.getRight());
				}
			}
		}
		catch(Exception e) {
			
		}
		return vector;
	}
	
	public TFIDFVector getTfIdfVector(int docID, String field) throws IOException {
		if(field == null) {
			return getTfIdfVector(docID);
		}
		
		TFIDFVector vector = new TFIDFVector();
		try {
			Map<String, Pair<Double, Double>> termsMap = getTermsMap(docID, field);
			for(String term : termsMap.keySet()) {
				Pair<Double, Double> tfidf = termsMap.get(term);
				vector.addTerm(term, tfidf.getLeft(), tfidf.getRight());
			}
		}
		catch(Exception e) {
			
		}
		return vector;
	}
	
	private Map<String, Pair<Double, Double>> getTermsMap(int docID, String field) throws IOException {
		Map<String, Pair<Double, Double>> termsMap = new HashMap<String, Pair<Double, Double>>();
		Terms tv = reader.getTermVector(docID, field);
		if(tv != null) {
			TermsEnum termsEnum = tv.iterator(null);
	    	while(termsEnum.next() != null) {
	    		BytesRef term = termsEnum.term();			
	    		if(term != null) {
	    			
	    			String termStr = termsEnum.term().utf8ToString();
		    		double tf = similarity.tf(termsEnum.totalTermFreq());
		    		double idf = this.idf(termStr, field);
		    		
		    		Pair<Double, Double> tfIdf = Pair.of(tf, idf);
		    		termsMap.put(termStr, tfIdf);
	    		}
	    	}
		}
		return termsMap;
	}
	
	public List<String> getTerms(int docID, String field) throws IOException {
		List<String> terms = new ArrayList<String>();
		Terms tv = reader.getTermVector(docID, field);
		if(tv != null) {
			TermsEnum termsEnum = tv.iterator(null);
	    	while(termsEnum.next() != null) {
	    		BytesRef term = termsEnum.term();			
	    		if(term != null) {
	    			terms.add(term.utf8ToString());
	    		}
	    	}
		}
		return terms;
	}
	
	public Map<String, TFIDFVector> getVectorsMap(Collection<String> ids) {
		return getVectorsMap(ids, null);
	}
	
	public Map<String, TFIDFVector> getVectorsMap(Collection<String> ids, String field) {
		Map<String, TFIDFVector> vectorsMap = new HashMap<String, TFIDFVector>();
		for(String itemId : ids) {
			try {
			TFIDFVector vector = this.getTfIdfVector(itemId, field);
	    		if(vector.getLength() == 0) {
	    			continue;
	    		}
	    	
	    		vectorsMap.put(itemId, vector);
	    		if(vectorsMap.size() % 50000 == 0) {
		    		System.out.println(vectorsMap.size() + " vectors");
		    	}
			}
	    	catch(Exception e) {
	    		
	    	}
		}
		return vectorsMap;
	}
	
	public void close() {	
		try {
			if(searcher != null) {
				IndexReader reader = searcher.getIndexReader();
				if(reader != null) {
					reader.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Iterator<Document> getDocumentsIterator() {
		Bits liveDocs = MultiFields.getLiveDocs(reader);
		return new Iterator<Document>() {
			private int documentIndex = 0;
			
			@Override
			public boolean hasNext() {
				while(liveDocs != null && !liveDocs.get(documentIndex)) {
					documentIndex++;
					if(documentIndex >= reader.maxDoc()) {
						break;
					}
				}
				
				if(documentIndex < reader.maxDoc()) {
					return true;
				}
				else {
					return false;
				}
			}

			@Override
			public Document next() {
				Document doc = null;
				try {
					doc = searcher.doc(documentIndex);
				} catch (IOException e) {
					e.printStackTrace();
				}
				documentIndex++;
				return doc;
			}
			
		};
	}

	public Iterator<TFIDFVector> getVectorsIterator() {
		Bits liveDocs = MultiFields.getLiveDocs(reader);
		return new Iterator<TFIDFVector>() {
			private int documentIndex = 0;
			
			@Override
			public boolean hasNext() {
				while(liveDocs != null && !liveDocs.get(documentIndex)) {
					documentIndex++;
					if(documentIndex >= reader.maxDoc()) {
						break;
					}
				}
				
				if(documentIndex < reader.maxDoc()) {
					return true;
				}
				else {
					return false;
				}
			}

			@Override
			public TFIDFVector next() {
				TFIDFVector vector = null;
				try {
					vector = getTfIdfVector(documentIndex);
				} catch (IOException e) {
					e.printStackTrace();
				}
				documentIndex++;
				return vector;
			}
			
		};
	}
	
	public static void main(String...args) throws IOException {
		LuceneIndex tIndex = new LuceneIndex("/second_disk/tIndex");
		
		/*
		File dir = new File("/second_disk/yfcc100m/raw/");
		for(File file : dir.listFiles()) {
			FileIterator it = new FileIterator(file);
			tIndex.index(it);
		}
		*/
		
		tIndex.open();
		System.out.println(tIndex.count() + " indexed items!");
		
		//Iterator<Document> it = tIndex.getDocumentsIterator();
		Iterator<TFIDFVector> it = tIndex.getVectorsIterator();
		while(it.hasNext()) {
			//Document doc = it.next();
			TFIDFVector vector = it.next();
			System.out.println(vector);
		}
	}
}
