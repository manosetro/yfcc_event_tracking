package gr.iti.mklab.yfcc.vindex;

public class IndexUtils {

	public static String decodeUrl(String encodedId) {
		String[] parts = encodedId.split("_");
		String farmId = parts[0];
		String serverId = parts[1];
		String identidier = parts[2];
		String secret = parts[3];

		String url = "http://farm" + farmId + ".staticflickr.com/" + serverId + "/" + identidier + "_"
				+ secret + ".jpg";
		return url;
	}
	
	public static String encodeId(String url) {
		url = url.replace("http://farm", "");
		url = url.replace(".staticflickr.com", "");
		url = url.replace(".jpg", "");

		String[] parts = url.split("/");
		
		String farmId = parts[0];
		String serverId = parts[1];
		String identidier_secret = parts[2];
		
		String encodedId = farmId + "_" + serverId + "_" + identidier_secret + "_z_0";
		
		return encodedId;
	}
	
	public static String decodeId(String encodedId) {
		String[] parts = encodedId.split("_");
		String id = parts[2];
		return id;
	}
	
	
}
