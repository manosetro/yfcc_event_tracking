package gr.iti.mklab.yfcc.vindex;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import gr.iti.mklab.visual.datastructures.IVFPQ;
import gr.iti.mklab.visual.datastructures.Linear;
import gr.iti.mklab.visual.datastructures.PQ;
import gr.iti.mklab.visual.datastructures.PQ.TransformationType;
import gr.iti.mklab.visual.utilities.Answer;
import gr.iti.mklab.visual.utilities.Result;

/**
 *
 */
public class LocalVisualIndex {

	private int w = 8;
	private int cacheSize = 1024;
	
	/** parameters of the index */
	private boolean readonly = true;
	private boolean countSizeOnLoad = true;
	private boolean loadIndexInMemory = true;
	private int projectionLength = 1024;
	
	/** parameters of the product quantizer */
	private int numCoarseCentroids = 8192;
	private int numSubVectors = 64;
	private int numProductCentroids = 256;
	
	private IVFPQ ivfpq;
	private Linear linearIndex;
	
	public LocalVisualIndex(String ivfPqIndexPath, String learningFilesPath, String BDBEnvHome, int maxNumVectors) throws Exception {
		
		System.out.println("Load Linear Index");
		
		linearIndex = new Linear(projectionLength, maxNumVectors, readonly, BDBEnvHome,
				loadIndexInMemory, countSizeOnLoad, 0);
		
		String coarseQuantizerFilename = learningFilesPath + "/qcoarse_1024d_8192k.csv";
		String productQuantizerFilename = learningFilesPath + "/pq_1024_64x8_rp_ivf_8192k.csv";

		TransformationType transformation = PQ.TransformationType.RandomPermutation;
		
	    ivfpq = new IVFPQ(projectionLength, maxNumVectors, readonly, ivfPqIndexPath, numSubVectors,
				numProductCentroids, transformation, numCoarseCentroids, countSizeOnLoad, 0,
				loadIndexInMemory, cacheSize);
	    
		System.out.print("Loading coarse and product quantizer..");
		ivfpq.loadCoarseQuantizer(coarseQuantizerFilename); 
		ivfpq.loadProductQuantizer(productQuantizerFilename);
		ivfpq.setW(w);
		System.out.println();
		
	}
	
	public double[] getVector(String id) {
	    int iid = linearIndex.getInternalId(id);
	    if(iid >= 0) {
	    	double[] vector = linearIndex.getVector(iid);
	    	return vector;
	    }
	    
	    return null;
	}
	
	public void close() {
		if(linearIndex != null) {
			linearIndex.close();
		}
		
		if(ivfpq != null) {
			ivfpq.close();
		}
	}
	
	public void test() {
		System.out.println("LoadCounter: " + linearIndex.getLoadCounter());
		for(int iid=0; iid<linearIndex.getLoadCounter(); iid++) {
			String id = linearIndex.getId(iid);
			
			System.out.println(iid + ") " + id);
		}
		
		System.out.println("IVFPQ: " + ivfpq.getLoadCounter());
		for(int iid=0; iid<ivfpq.getLoadCounter(); iid++) {
			String id = ivfpq.getId(iid);
			
			System.out.println(iid + ") " + id);
		}
	}
	
	public List<Pair<String, Double>> getNearest(String queryId, int k) throws Exception {
	
		List<Pair<String, Double>> nn = new ArrayList<Pair<String, Double>>();
		
		double[] vector = getVector(queryId);
		
		if(vector == null)
			return nn;
		
		Answer answer = ivfpq.computeNearestNeighbors(k, vector);
		Result[] results = answer.getResults();
		for(Result result : results) {
			String id = result.getExternalId();
			double distance = result.getDistance();
			
			nn.add(Pair.of(id, distance));
		}
		
		return nn;
	}
	
	public static void main(String...args) throws Exception {
				
		String ivfPqIndexPath = "/second_disk/vIndex/yfcc100m_ivfpq";
		String learningFilesPath = "/second_disk/vIndex/learning_files";
		String BDBEnvHome = "/second_disk/vIndex/yfcc100m_linear";
		
		int maxNumVectors = 100000000;
		
		LocalVisualIndex vIndex = new LocalVisualIndex(ivfPqIndexPath, learningFilesPath, BDBEnvHome, maxNumVectors);
			
		vIndex.close();
		
		//ItemDAO dao = new ItemDAO("160.40.51.16", "YahooGrandChallenge");
		//QueryResults<Item> results = dao.find();
		//Iterator<Item> it = results.iterator();
		/*
		FileIterator it = new FileIterator(new File("/second_disk/yfcc100m/raw/yfcc100m_dataset-0"));
		while(it.hasNext()) {
			Item item = it.next();
			
			if(item == null)
				continue;
			
			try {
				String encodedId = IndexUtils.encodeId(item.getUrl());
				
				System.out.println("ID: " + item.getId());
				System.out.println("URL: " + item.getUrl());
				System.out.println("ENCODED ID: " + encodedId);

				List<Pair<String, Double>> nns = vIndex.getNearest(encodedId, 5);
				
				if(nns.isEmpty())
					continue;

				for(Pair<String, Double> nn : nns) {
					
					System.out.println(nn.getLeft() + ", " + nn.getRight());
					String nnUrl = IndexUtils.decodeUrl(nn.getLeft());
					
					System.out.println(nnUrl);
				}
				
				System.out.println("======================================");
			    
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		*/
		
	}
	
}
