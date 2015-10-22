package gr.iti.mklab.yfcc.vindex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import gr.iti.mklab.visual.datastructures.IVFPQ;
import gr.iti.mklab.visual.datastructures.Linear;
import gr.iti.mklab.visual.utilities.Answer;
import gr.iti.mklab.visual.utilities.Result;
import gr.iti.mklab.yfcc.vindex.NearestNeighbors.NearestNeighbor;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.QueryParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;

/**
 * Root resource (exposed at "visual/index" path)
 */
@Path("visual/index")
@Singleton
public class VisualIndexResource {

	@Context
	private Application appInfo;
	
	//private int w = 8;
	//private int cacheSize = 1024;
	
	/** parameters of the index */
	private boolean readonly = true;
	private boolean countSizeOnLoad = false;
	private boolean loadIndexInMemory = true;
	private int projectionLength = 1024;
	
	/** parameters of the product quantizer */
	//private int numCoarseCentroids = 8192;
	//private int numSubVectors = 64;
	//private int numProductCentroids = 256;

	/** Paths */
	//private String ivfPqIndexPath;
	private String learningFilesPath;
	private String BDBEnvHome;
	
	private static IVFPQ ivfpq = null;
	private static Linear linearIndex = null;
	
	public VisualIndexResource(@Context Application appInfo) {
		
		this.appInfo = appInfo;
		Map<String, Object> properties = appInfo.getProperties();
		
		//this.ivfPqIndexPath = (String) properties.get("ivfPqIndexPath");
		this.learningFilesPath = (String) properties.get("learningFilesPath");
		this.BDBEnvHome = (String) properties.get("BDBEnvHome");
		
		int maxNumVectors = (Integer) properties.get("maxNumVectors");
		
		if(!learningFilesPath.endsWith("/"))
			learningFilesPath = learningFilesPath + "/";
		
		//String coarseQuantizerFilename = learningFilesPath + "qcoarse_1024d_8192k.csv";
		//String productQuantizerFilename = learningFilesPath + "pq_1024_64x8_rp_ivf_8192k.csv";
		
		//TransformationType transformation = PQ.TransformationType.RandomPermutation;
		try {
			linearIndex = new Linear(projectionLength, maxNumVectors, readonly, BDBEnvHome, !loadIndexInMemory, countSizeOnLoad, maxNumVectors);
			System.out.println("=====================================");	
			
			//ivfpq = new IVFPQ(projectionLength, maxNumVectors, readonly, ivfPqIndexPath, numSubVectors, numProductCentroids, transformation, 
			//		numCoarseCentroids, countSizeOnLoad, maxNumVectors, !loadIndexInMemory, cacheSize);
			//System.out.print("Loading coarse and product quantizer..");
			//ivfpq.loadCoarseQuantizer(coarseQuantizerFilename); 
			//ivfpq.loadProductQuantizer(productQuantizerFilename);
			//ivfpq.setW(w);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "application/json" media type.
     * 
     * @return String that will be returned as a text/plain response.
     */
    @GET
    @Path("vector/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVector(@PathParam("id") String id, @QueryParam("url") String url) {
    	try {
    		
    		String encodedId = IndexUtils.encodeId(url);
    		if(!IndexUtils.decodeId(encodedId).equals(id)) {
    			throw new Exception(IndexUtils.decodeId(encodedId) + " != " + id);
    		}
    		
    		double[] vector = get(encodedId);
    		if(vector == null) {
    			throw new Exception("Vector is null.");
    		}	
    		
    		Vector v = new Vector(id, vector);
    		return Response.status(200).entity(v).build();
    		
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    
    		Vector v = new Vector(id, new double[0]);
    		return Response.status(400).entity(v).build();
    		
    	}
    }
    
    private double[] get(String id) {
    	int iid = linearIndex.getInternalId(id);
	    if(iid >= 0) {
	    	double[] vector = linearIndex.getVector(iid);
	    	return vector;
	    }	    
	    return null;
    }
    
    @GET
    @Path("nearest/{id}")
    @Produces(MediaType.APPLICATION_JSON)
	public Response getNearest(@PathParam("id") String id, @QueryParam("url") String url, @DefaultValue("10") @QueryParam("k") int k,
			@DefaultValue("0.0") @QueryParam("threshold") double threshold) {
    	
    	String queryId = IndexUtils.encodeId(url);
    	NearestNeighbors nns = new NearestNeighbors(id, url);
    	
		try {
			
			double[] vector = get(queryId);
			if(vector == null) {
				throw new Exception("Vector is null");
			}
			
			List<NearestNeighbor> nnList = new ArrayList<NearestNeighbor>();
			
			Answer answer = ivfpq.computeNearestNeighbors(k, vector);
			Result[] results = answer.getResults();
			for(Result result : results) {
				String encodedId = result.getExternalId();
				double distance = result.getDistance();
				
				String nnUrl = IndexUtils.decodeUrl(encodedId);
				String nnId = IndexUtils.decodeId(encodedId);
				
				nnList.add(new NearestNeighbor(nnId, nnUrl, distance));
			}
			nns.setNearest(nnList);
			
			return Response.status(200).entity(nns).build();
			
		} catch (Exception e) {
			return Response.status(400).entity(nns).build();
		}
	}
	
    @GET
    @Path("statistics")
    @Produces(MediaType.APPLICATION_JSON)
	public Response statistics() {
    	String body = "{\"linear\": " + linearIndex.getLoadCounter() + "}";
    	return Response.status(200).entity(body).build();
    	
    }
    
    @GET
    @Path("list")
    @Produces(MediaType.TEXT_PLAIN)
	public String list() {
    	List<String> ids = new ArrayList<String>();
    	for(int iid=0; iid<linearIndex.getLoadCounter(); iid++) {
    		String id = linearIndex.getId(iid);
    		ids.add(id);
    	}
    	
    	return StringUtils.join(ids, "\n");
    }
	
}
