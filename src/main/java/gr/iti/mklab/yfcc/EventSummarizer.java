package gr.iti.mklab.yfcc;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.Graph;
import gr.iti.mklab.yfcc.models.Event;
import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.models.SameEventLink;
import gr.iti.mklab.yfcc.ranking.GraphRanker;
import gr.iti.mklab.yfcc.structures.ItemsTimeline;
import gr.iti.mklab.yfcc.utils.GraphUtils;

/*
 * Image-Text Co-Ranking method for Visual Summarization
 * 
 */
public class EventSummarizer {
		
	public Map<String, Double> summarize(Event event) {
		
		//System.out.println("Summarization");
		Map<String, Item> itemsMap = event.getItemsMap();
		Graph<String, SameEventLink> eventGraph = event.getEventGraph();
		//System.out.println("eventGraph: " + eventGraph.getVertexCount() + " vertices, " + eventGraph.getEdgeCount() + " edges");
		
		Map<String, Double> scores = new HashMap<String, Double>();
		
		ItemsTimeline timeline = event.getTimeline();
		for(Entry<Long, Collection<String>> timeslot : timeline.entrySet()) {
		
			Collection<String> ids = timeslot.getValue();
			Map<String, Double> priorScores = getScores(ids, event);	
			//System.out.println(priorScores.size() + " time-slot prior scores");
			
			Graph<String, SameEventLink> timeslotGraph = GraphUtils.filter(eventGraph, ids);
			//System.out.println("timeslotGraph: " + timeslotGraph.getVertexCount() + " vertices, " + timeslotGraph.getEdgeCount() + " edges");
			DirectedGraph<String, SameEventLink> directedGraph = GraphUtils.toDirected(timeslotGraph, itemsMap);
			Graph<String, SameEventLink> normalizedGraph = GraphUtils.normalize(directedGraph);
			//System.out.println("normalizedGraph: " + normalizedGraph.getVertexCount() + " vertices, " + normalizedGraph.getEdgeCount() + " edges");
			
			Map<String, Double> timeslotScores = GraphRanker.divrankScoring(normalizedGraph, priorScores);
			scores.putAll(timeslotScores);
		}	

		//System.out.println(scores.size() + " total prior scores");
		return scores;
	}
	
	private Map<String, Double> getScores(Collection<String> ids, Event event) {
		Map<String, Double> scores = new HashMap<String, Double>();
		
		Item centroid = event.getCentroidItem();
		if(centroid == null) {
			System.out.println("Error: centroid item of event " + event.getId() + " is null!");
			return scores;
		}
		
		Double popularitySum = 0d;
		Map<String, Item> itemsMap = event.getItemsMap();
		for(String id : ids) {
			double similarity = .0;
			Item item = itemsMap.get(id);
			if(item != null) {
				double titleSimilarity = centroid.descriptionSimilarityCosine(item);
				double tagsSimilarity = centroid.descriptionSimilarityCosine(item);
				double descSimilarity = centroid.descriptionSimilarityCosine(item);
				
				similarity = 0.33*(titleSimilarity + tagsSimilarity + descSimilarity);
			}
			popularitySum += (similarity+1);
			
			scores.put(id, similarity);
		}
		
		for(String id : scores.keySet()) {
			Double popularity = scores.get(id);
			if(popularity != null) {
				scores.put(id, (popularity+1)/popularitySum);
			}
			else {
				scores.put(id, .0);
			}
		}
		
		return scores;
	}
	
}
