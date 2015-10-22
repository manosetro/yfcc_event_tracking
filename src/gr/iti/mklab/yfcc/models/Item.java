package gr.iti.mklab.yfcc.models;

import gr.iti.mklab.yfcc.utils.GeodesicDistanceCalculator;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.beans.Field;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

@Entity
public class Item {

	@Id
	@Field("id")
	private String id;

	@Field("userid")
	private String userid;
	
	@Field("username")
	private String username;
	
	@Field("url")
	private String url;

	@Field("title")
	private String title;
	
	@Field("description")
	private String description;
	
	@Field("cleanDescription")
	private String cleanDescription;
	
	@Field("takenDate")
	private Date takenDate;
	
	@Field("uploadDate")
	private Date uploadDate;
	
	@Field("tags")
	private List<String> tags;
	
	@Field("machineTags")
	private List<String> machineTags;
	
	@Field("persons")
	private List<String> persons;
	
	@Field("organizations")
	private List<String> organizations;
	
	@Field("locations")
	private List<String> locations;
	
	@Field("latlon")
	private String latlon;
	
	@Field("latitude")
	private Double latitude = null;
	
	@Field("longitude")
	private Double longitude = null;

	// Recorded accuracy level of the location information. 
	// World level is 1, Country is ~3, Region ~6, City ~11, Street ~16. 
	@Field("locationAccuracy")
	private int locationAccuracy;

	@Field("views")
	private int views = 0;
	
	@Field("faves")
	private int faves = 0;
	
	@Field("comments")
	private int comments = 0;
	
	@Field("language")
	private String language;
	
	@Field("titleTerms")
	private List<String> titleTerms = null;
	
	@Field("tagsTerms")
	private List<String> tagsTerms = null;
	
	@Field("descriptionTerms")
	private List<String> descriptionTerms = null;
	
	private double[] vlad;
	
	/*
	 * TF-IDF vectors for textual fields 
	 */
	private TFIDFVector titleVector = null;
	private TFIDFVector tagsVector = null;
	private TFIDFVector descriptionVector = null;
	
	private TFIDFVector textVector = null;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getCleanDescription() {
		return cleanDescription;
	}

	public void setCleanDescription(String cleanDescription) {
		this.cleanDescription = cleanDescription;
	}
	
	public String getText() {
		String text = "";
		String title = getTitle();
		if(title != null) {
			text = title + ". ";
		}
		
		String description = getCleanDescription();
		if(description != null) {
			text += description;
		}
		return text;
	}
	
	public Date getTakenDate() {
		return takenDate;
	}

	public void setTakenDate(Date takenDate) {
		this.takenDate = takenDate;
	}

	public Date getUploadDate() {
		return uploadDate;
	}

	public void setUploadDate(Date uploadDate) {
		this.uploadDate = uploadDate;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public List<String> getMachineTags() {
		return machineTags;
	}

	public void setMachineTags(List<String> machineTags) {
		this.machineTags = machineTags;
	}
	
	public boolean hasMachineTags() {
		if(machineTags == null || machineTags.isEmpty()) {
			return false;
		}
		return true;
	}
	
	public List<String> getPersons() {
		return persons;
	}

	public void setPersons(List<String> persons) {
		this.persons = persons;
	}
	
	public List<String> getOrganizations() {
		return organizations;
	}

	public void setOrganizations(List<String> organizations) {
		this.organizations = organizations;
	}
	
	public List<String> getLocations() {
		return locations;
	}

	public void setLocations(List<String> locations) {
		this.locations = locations;
	}
	
	public List<String> getNamedEntities() {
		List<String> namedEntities = new ArrayList<String>();
		if(persons != null) {
			namedEntities.addAll(persons);
		}
		if(organizations != null) {
			namedEntities.addAll(organizations);
		}
		if(locations != null) {
			namedEntities.addAll(locations);
		}
		
		return namedEntities;
	}
	
	public Double getLatitude() {
		if(latitude == null) {
			String[] parts = latlon.split(",");
			latitude = Double.valueOf(parts[0]);
		}
		return latitude;
	}

	public Double getLongitude() {
		if(longitude == null) {
			String[] parts = latlon.split(",");
			longitude = Double.valueOf(parts[1]);
		}
		return longitude;
	}
	
	public String getLatLon() {
	    return latlon;
	}
	
	public void setLatLon(Double latitude, Double longitude) {
		if(latitude != null && longitude != null) {
			this.latitude = latitude;
			this.longitude = longitude;
			
		    this.latlon =  (latitude + "," + longitude);
		}
	}
	
	public int getLocationAccuracy() {
		return locationAccuracy;
	}
	
	public void setLocationAccuracy(int locationAccuracy) {
		this.locationAccuracy = locationAccuracy;
	}
	
	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public int getViews() {
		return views;
	}

	public void setViews(int views) {
		this.views = views;
	}
	
	public int getFavorities() {
		return faves;
	}

	public void setFavorities(int faves) {
		this.faves = faves;
	}
	
	public int getComments() {
		return comments;
	}

	public void setComments(int comments) {
		this.comments = comments;
	}
	
	public boolean hasLocation() {
		if(latlon != null) {
			return true;
		}
		
		return false;
	}

	public TFIDFVector getTitleVector() {
		return titleVector;
	}

	public void setTitleVector(TFIDFVector titleVector) {
		this.titleVector = titleVector;
	}

	public TFIDFVector getTagsVector() {
		return tagsVector;
	}

	public void setTagsVector(TFIDFVector tagsVector) {
		this.tagsVector = tagsVector;
	}

	public TFIDFVector getDescriptionVector() {
		return descriptionVector;
	}

	public void setDescriptionVector(TFIDFVector descriptionVector) {
		this.descriptionVector = descriptionVector;
	}
	
	public TFIDFVector getTextVector() {
		if(textVector == null) {
			textVector = new TFIDFVector();
			if(titleVector != null) {
				textVector.mergeVector(titleVector);
			}
			if(descriptionVector != null) {
				textVector.mergeVector(descriptionVector);
			}
			if(tagsVector != null) {
				textVector.mergeVector(tagsVector);
			}
		}
		return textVector;
	}

	public void setTextVector(TFIDFVector textVector) {
		this.textVector = textVector;
	}
	
	public List<String> getTitleTerms() {
		return titleTerms;
	}

	public void setTitleTerms(List<String> titleTerms) {
		this.titleTerms = titleTerms;
	}

	public List<String> getTagsTerms() {
		return tagsTerms;
	}

	public void setTagsTerms(List<String> tagsTerms) {
		this.tagsTerms = tagsTerms;
	}

	public List<String> getDescriptionTerms() {
		return descriptionTerms;
	}

	public void setDescriptionTerms(List<String> descriptionTerms) {
		this.descriptionTerms = descriptionTerms;
	}
	
	public DBObject toDBObject() {
		DBObject obj = new BasicDBObject();
		
		obj.put("className", "gr.iti.mklab.yfcc.models.Item");
		obj.put("_id", id);
		obj.put("userid", userid);
		obj.put("url", url);
		obj.put("title", title);
		obj.put("description", description);
		obj.put("tags", tags);
		obj.put("machineTags", machineTags);
		obj.put("takenDate", takenDate);
		obj.put("uploadDate", uploadDate);
		obj.put("latitude", latitude);
		obj.put("longitude", longitude);
		
		return obj;
	}

    public double locationSimilarity(Item item2) {
        if(this.hasLocation() && item2.hasLocation()) {
            return (GeodesicDistanceCalculator.vincentyDistance(latitude, longitude, item2.latitude, item2.longitude))/1000;
        }
        else { 
            return -1;
        }
    }
  
    public double locationSimilarity1Km(Item item2) {
    	if(this.hasLocation() && item2.hasLocation()) {
            double distKms=(GeodesicDistanceCalculator.vincentyDistance(latitude, longitude, item2.latitude, item2.longitude))/1000;
            if(distKms<0.5) {
                return 1;
            }
            else {
                return 0;
            }
        }
        else {
            return -1;
        }
    }

    public double descriptionSimilarityCosine(Item item2) {
    	if(descriptionVector == null || descriptionVector.isEmpty() || 
    			item2.descriptionVector == null || item2.descriptionVector.isEmpty())
    		return .0;
    	
        return descriptionVector.cosineSimilarity(item2.descriptionVector);
    }

    public double textSimilarityCosine(Item item2) {
        if((textVector == null) || textVector.isEmpty() || 
        		item2.textVector == null || item2.textVector.isEmpty()) {
            return .0;
        }
        return textVector.cosineSimilarity(item2.textVector);
    }
    
    public double titleSimilarityCosine(Item item2) {
    	if(titleVector == null || titleVector.isEmpty() || 
    			item2.titleVector == null || item2.titleVector.isEmpty()) {
    		return .0;
    	}
    	
        return titleVector.cosineSimilarity(item2.titleVector);
    }
    
    public double tagsSimilarityCosine(Item item2) {
    	if(tagsVector == null || tagsVector.isEmpty() || 
    			item2.tagsVector == null || item2.tagsVector.isEmpty()) {
    		return .0;
    	}
    	
        return tagsVector.cosineSimilarity(item2.tagsVector);
    }

    public double descriptionSimilarityBM25(Item item2) {
    	if(descriptionVector == null || item2.descriptionVector == null) {
    		return .0;
    	}
    	
        return descriptionVector.BM25Similarity(item2.descriptionVector, AVG_DESCRIPTION);
    }
    
    public double titleSimilarityBM25(Item item2) {
    	if(titleVector == null || item2.titleVector == null) {
    		return .0;
    	}
    	
        return titleVector.BM25Similarity(item2.titleVector, AVG_TITLE);
    }
    
    public double tagsSimilarityBM25(Item item2) {
    	if(tagsVector == null || item2.tagsVector == null) {
    		return .0;
    	}
    	
        return tagsVector.BM25Similarity(item2.tagsVector, AVG_TAGS);
    }
    
    
    public double timeUploadedSimilarity(Item item2, double divisor) {
    	
        if((this.uploadDate != null) && (item2.uploadDate != null)) {  
            return Math.abs(uploadDate.getTime() - item2.uploadDate.getTime())/divisor;
        }
        else {
            return -1;
        }
    }

    public double timeTakenSimilarity(Item item2, double divisor) {
        return Math.abs(this.takenDate.getTime() - item2.takenDate.getTime())/divisor;
    }

    public double timeTakenHourDiff12(Item item2) {
        double divisor = 1000.0;
        divisor = divisor * 60 * 60;
        double hours = Math.abs(takenDate.getTime() - item2.takenDate.getTime())/divisor;
        if(hours < 12) {
            return 1;
        }
        else { 
            return 0;
        }
    }
    
    public double timeTakenHourDiff24(Item item2) {
        double divisor = 1000.0;
        divisor = divisor * 60 * 60;
        double hours = Math.abs(takenDate.getTime() - item2.takenDate.getTime())/divisor;
        if(hours < 24) {
            return 1;
        }
        else { 
            return 0;
        }
    }

    public double timeTakenDayDiff3(Item item2) {
        double divisor = 1000.0;
        divisor = divisor * 60* 60*24;
        double days = Math.abs(takenDate.getTime() - item2.takenDate.getTime())/divisor;
        if(days < 3) {
            return 1;
        }
        else {
            return 0;
        }
    }
    
    public double sameUserSimilarityForCentroid(Item item2) {
        if((this.userid == null) || (item2.userid == null)){
            return 0;
        }
        
        if(this.userid.contains(item2.userid)) {
            return 1;
        }
        return 0;
    }    
    
    public double sameUserSimilarity(Item item2) {
        if((this.userid == null) || (item2.userid == null)) {
            return 0;
        }
        
        if(this.userid.equals(item2.userid)) {
            return 1;
        }
        
        return 0;
    }

    public boolean sameMachineTags(Item other) {
		List<String> mTags1 = this.getMachineTags();
		List<String> mTags2 = other.getMachineTags();
		if(mTags1 != null && mTags2 != null && !mTags1.isEmpty() && !mTags2.isEmpty()) {
			try {
				// use machine tags to find same event pairs
				for(String machineTag1 : mTags1) {
					String[] mtParts1 = machineTag1.split("=");
					if(mtParts1.length != 2) {
						continue;
					}
				
					for(String machineTag2 : mTags2) {
						String[] mtParts2 = machineTag2.split("=");
						if(mtParts2.length != 2) {
							continue;
						}
					
						if(mtParts1[0].trim().equalsIgnoreCase(mtParts2[0].trim()) && 
								mtParts1[1].trim().equalsIgnoreCase(mtParts2[1].trim())) {
							return true;
						}
					}
				}
			}
			catch(Exception e) {
				
			}
		}
		return false;
	}
    
    public boolean sameLanguage(Item other) {
    	String lang1 = this.getLanguage();
    	String lang2 = other.getLanguage();
    	if(lang1 != null && lang2 != null) {
    		if(lang1.equalsIgnoreCase(lang2)) {
    			return true;
    		}
    	}
    	return false;
    }
    
    public Set<String> namedEntitiesOverlap(Item other) {
		
	    Set<String> intersection = new HashSet<String>();
	   
		Set<String> ne1 = new HashSet<String>();
		if(this.getPersons() != null) {
			ne1.addAll(this.getPersons());
		}
		if(this.getOrganizations() != null) {
			ne1.addAll(this.getOrganizations());
		}
		
		Set<String> ne2 = new HashSet<String>();
		if(other.getPersons() != null) {
			ne2.addAll(other.getPersons());
		}
		if(other.getOrganizations() != null) {
			ne2.addAll(other.getOrganizations());
		}
		
		intersection.addAll(ne1);
		intersection.retainAll(ne2);
		
		return intersection;
	}

	public String getPageUrl() {
		return "https://www.flickr.com/photos/" + userid + "/" + id;
	}    
	
	public String toString() {
		return "id:" + id + ", title: " + title + ", takenDate: " + takenDate + ", tags: [" + StringUtils.join(tags," ")+"]";
				
	}
	
    public static double AVG_TITLE = 1;
    public static double AVG_DESCRIPTION = 1;
    public static double AVG_TAGS = 1;

	public double[] getVlad() {
		return vlad;
	}

	public void setVlad(double[] vlad) {
		this.vlad = vlad;
	}

}
