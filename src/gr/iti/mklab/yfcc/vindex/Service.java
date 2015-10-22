package gr.iti.mklab.yfcc.vindex;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.moxy.json.MoxyJsonConfig;
import org.glassfish.jersey.moxy.json.MoxyJsonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.ext.ContextResolver;

/**
 * Main class.
 */
public class Service {
	
    // Base URI the Grizzly HTTP server will listen on
    public static final String BASE_URI = "http://160.40.51.16:8888/";

    /**
     * Main method.
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
    	try {
    		
    		if(args.length != 4) {
    			System.out.println("Usage: \n \t java -jar Service.jar ivfPqIndexPath learningFilesPath BDBEnvHome maxNumVectors");
    			throw new Exception("Wrong number of arguments.");
    		}
    		
    		final ResourceConfig rc = createResourceConfig();
    		rc.property("ivfPqIndexPath", args[0]);
    		rc.property("learningFilesPath", args[1]);
    		rc.property("BDBEnvHome", args[2]);
    		rc.property("maxNumVectors", Integer.parseInt(args[3]));	
    		
    		final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
        
        	System.out.println(String.format("Jersey app started with WADL available at %sapplication.wadl", BASE_URI));
        	System.out.println("Hit enter to stop it...");
        	System.in.read();
        
        	server.shutdownNow();
    	}
    	catch(Exception ex) {
    		Logger.getLogger(Service.class.getName()).log(Level.SEVERE, null, ex);
    	}
    }
    
    public static ResourceConfig createResourceConfig() {
    	return new ResourceConfig()
    			.packages("gr.iti.mklab.yfcc.vindex")
    			.register(MoxyJsonFeature.class)
    			.register(createMoxyJsonResolver());
    }
    
    public static ContextResolver<MoxyJsonConfig> createMoxyJsonResolver() {
    	final MoxyJsonConfig moxyJsonConfig = new MoxyJsonConfig();
    	
    	Map<String, String> namespacePrefixMapper = new HashMap<String, String>(1);
    	namespacePrefixMapper.put("http://www.w3.org/2001/XMLSchema-instance", "xsi");
    	moxyJsonConfig.setNamespacePrefixMapper(namespacePrefixMapper);
    	moxyJsonConfig.setNamespaceSeparator(':');
    	
    	return moxyJsonConfig.resolver();
    }
}

