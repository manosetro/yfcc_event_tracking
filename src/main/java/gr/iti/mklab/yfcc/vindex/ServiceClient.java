package gr.iti.mklab.yfcc.vindex;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.moxy.json.MoxyJsonFeature;

public class ServiceClient {
	
	private final Client client = ClientBuilder.newBuilder()
			.register(MoxyJsonFeature.class)
			.register(Service.createMoxyJsonResolver())
			.build();
	
	private String targetUri;
	
	public ServiceClient(String targetUri) {
		this.targetUri = targetUri;
	}
	
	public NearestNeighbors getNearest(String id, String url) {
		WebTarget target = client.target(targetUri)
			.path("visual/index/nearest/")
			.path(id)
			.queryParam("k", 500)
			.queryParam("url", url);
		
		NearestNeighbors nn = target.request(MediaType.APPLICATION_JSON).get(NearestNeighbors.class);
		return nn;
	}
	
	public double[] getVector(String id, String url) {
		WebTarget target = client.target(targetUri)
				.path("visual/index/vector/")
				.path(id)
				.queryParam("url", url);
		try {
			Vector vector = target.request(MediaType.APPLICATION_JSON).get(Vector.class);	
			if(vector == null)
				return null;
			
			return vector.vector;
		}
		catch(Exception e) {
			return null;
		}
		
	}
	
	public static void main(String[] args) {
		ServiceClient client = new ServiceClient("http://160.40.51.16:8888/");
		
		String id = IndexUtils.decodeId("1_100_293055525_ffc31d6d5b_z_0");
		String url = IndexUtils.decodeUrl("1_100_293055525_ffc31d6d5b_z_0");
		
		long t = System.currentTimeMillis();
		double[] vector = client.getVector(id, url);
		t = System.currentTimeMillis()-t;
		System.out.println(t + " mseconds");
		System.out.println(vector);
		
//		t = System.currentTimeMillis();
//		NearestNeighbors nn = client.getNearest(id, url);
//		t = System.currentTimeMillis()-t;
//		System.out.println(t + " mseconds");

	}
}
