package gr.iti.mklab.yfcc.dao;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.Query;

import gr.iti.mklab.yfcc.models.Event;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class EventDAO extends BasicDAO<Event, ObjectId> {
	
	static Morphia morphia = new Morphia();

	public static EventDAO getDAO(String hostname, String dbName) {
		try {
			return new EventDAO(hostname, dbName);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		}
	}
	
    private EventDAO(String hostname, String dbName) throws UnknownHostException {
    	super(new MongoClient(hostname), morphia, dbName);
    	morphia.map(Event.class);
    }
    
    public Event get(String id, String collectionName) {
    	Query<Event> query = createQuery().filter("_id", id);
    	Event event = findOne(query);
    	
    	return event;
    }
    
    public void insert(List<Event> events) {
    	DBCollection collection = this.getCollection();
    	List<DBObject> dbObjects = new ArrayList<DBObject>();
    	for(Event event : events) {
    		DBObject obj = event.toDBObject();
    		if(obj != null) {
    			dbObjects.add(obj);
    		}
    	}
    	
    	if(!dbObjects.isEmpty()) {
    		collection.insert(dbObjects);
    	}
    }
        
}