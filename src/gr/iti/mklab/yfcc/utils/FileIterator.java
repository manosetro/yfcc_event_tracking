package gr.iti.mklab.yfcc.utils;

import gr.iti.mklab.yfcc.models.Item;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class FileIterator implements Iterator<Item> {

	private static String datePattern = "yyyy-MM-dd HH:mm:ss.S";
	private static SimpleDateFormat sdf = new SimpleDateFormat(datePattern);
	
	private LineIterator it;
	
	public FileIterator(File file) throws IOException {
		it = FileUtils.lineIterator(file);
	}

	private Item lineToItem(String line) {
		
		String[] parts = line.split("\t");
		if(parts[22].equals("1")) {
			return null;
		}
		
		try {
			Item item = new Item();
			
			item.setId(parts[0]);
			item.setUserid(parts[1]);
			item.setUsername(parts[2]);
			
			Date takenDate = sdf.parse(parts[3]);
			item.setTakenDate(takenDate);
			
			Date uploadDate = new Date(Long.parseLong(parts[4]) * 1000l);
			item.setUploadDate(uploadDate);
		
			// textual fields
			String title = URLDecoder.decode(parts[6], "UTF8");
			item.setTitle(title);
			
			String description = URLDecoder.decode(parts[7], "UTF8");
			if(description != null && !description.equals("")) {
			    item.setDescription(description);
			    try {
			    	Document doc = Jsoup.parse(description);
			    	if(doc != null) {
			    		String cleanDescription = doc.text();
			    		item.setCleanDescription(cleanDescription);
			    	}
			    }
			    catch(Exception e) { 
			    	e.printStackTrace();
			    }
	        }
			
			List<String> tags = new ArrayList<String>();
			if(!parts[8].equals("")) {
				for(String tag : parts[8].split(",")) {
					String decodedTag = URLDecoder.decode(tag, "UTF8");
					tags.add(decodedTag);
				}
				item.setTags(tags);
			}
		
			List<String> machineTags = new ArrayList<String>();
			if(!parts[9].equals("")) {
				for(String mTag : parts[9].split(",")) {
					String decodedMachineTag = URLDecoder.decode(mTag, "UTF8");
					if(decodedMachineTag.startsWith("upcoming") || decodedMachineTag.startsWith("lastfm")
							|| decodedMachineTag.startsWith("active") 		|| decodedMachineTag.startsWith("facebook") 
							|| decodedMachineTag.startsWith("oshug") 		|| decodedMachineTag.startsWith("hdt")
							|| decodedMachineTag.startsWith("eventbrite") 	|| decodedMachineTag.startsWith("vedro") 
							|| decodedMachineTag.startsWith("rotown") 		|| decodedMachineTag.startsWith("foam")
							|| decodedMachineTag.startsWith("venteria") 	|| decodedMachineTag.startsWith("wevent")
							|| decodedMachineTag.startsWith("rockzillait") 	|| decodedMachineTag.startsWith("burningman")) {
						
							machineTags.add(decodedMachineTag);
					}
				}
				item.setMachineTags(machineTags);
			}
			
			if((description == null || description.length() < 10) && machineTags.isEmpty() && 
					(tags.isEmpty() || title == null || title.length() < 10)) {
				return null;
			}
			
			// geo-location
			if(!parts[10].equals("") && !parts[11].equals("")) {
				Double longitude = Double.parseDouble(parts[10]);
				Double latitude = Double.parseDouble(parts[11]);
				item.setLatLon(latitude, longitude);
				
				if(parts[12] != null && !parts[12].equals("")) {
					int accuracy = Integer.parseInt(parts[12]);
					item.setLocationAccuracy(accuracy);
				}
			}
		
			item.setUrl(parts[14]);
			
			return item;
		} catch (Exception e) {
			return null;
		}	
	}
	
	@Override
	public boolean hasNext() {
		return it.hasNext();
	}

	@Override
	public Item next() {
		String line = it.next();
		Item item = lineToItem(line);
		
		return item;
	}

	public static void main(String...args) throws IOException {
		String file = "/second_disk/yfcc100m/raw/yfcc100m_dataset-0";
		
		Set<String> users = new HashSet<String>();
		
		int items = 0, lastfm = 0, geotagged = 0, nulls = 0;
		FileIterator it = new FileIterator(new File(file));
		while(it.hasNext()) {
			Item item = it.next();
			if(item == null) {
				nulls++;
				continue;
			}
			
			items++;
			if(item.getLatLon() != null && !item.getLatLon().equals("")) {
				geotagged++;
			}
			
			String uid = item.getUserid();
			if(uid != null) {
				users.add(uid);
			}
		}
		System.out.println(items + " items, " + geotagged + " geotagged");
		System.out.println(users.size() + " users");
		System.out.println(nulls + " nulls");
		System.out.println(lastfm + " event machine tags");
	}

}
