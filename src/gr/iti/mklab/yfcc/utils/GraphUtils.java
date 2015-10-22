package gr.iti.mklab.yfcc.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.Predicate;
import org.apache.commons.collections15.Transformer;
import org.apache.commons.lang.StringUtils;

import edu.uci.ics.jung.algorithms.filters.EdgePredicateFilter;
import edu.uci.ics.jung.algorithms.filters.Filter;
import edu.uci.ics.jung.algorithms.filters.VertexPredicateFilter;
import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;
import edu.uci.ics.jung.graph.util.Pair;
import edu.uci.ics.jung.io.GraphIOException;
import edu.uci.ics.jung.io.GraphMLWriter;
import edu.uci.ics.jung.io.graphml.EdgeMetadata;
import edu.uci.ics.jung.io.graphml.GraphMLReader2;
import edu.uci.ics.jung.io.graphml.GraphMetadata;
import edu.uci.ics.jung.io.graphml.HyperEdgeMetadata;
import edu.uci.ics.jung.io.graphml.NodeMetadata;
import edu.uci.ics.jung.io.graphml.GraphMetadata.EdgeDefault;
import gr.iti.mklab.yfcc.models.Item;
import gr.iti.mklab.yfcc.models.SameEventLink;
import gr.iti.mklab.yfcc.models.TFIDFVector;

public class GraphUtils {

	public static <V> DirectedGraph<V, SameEventLink> toDirected(Graph<V, SameEventLink> graph) {	
		DirectedGraph<V, SameEventLink> directedGraph = new DirectedSparseGraph<V, SameEventLink>();
	
		// Add all vertices first
		Collection<V> vertices = graph.getVertices();
		for(V vertex : vertices) {
			directedGraph.addVertex(vertex);
		}
		
		// Add directed edges
		for(SameEventLink edge : graph.getEdges()) {	
			Pair<V> endpoints = graph.getEndpoints(edge);
			directedGraph.addEdge(new SameEventLink(edge.weight), endpoints.getFirst(), endpoints.getSecond());
			directedGraph.addEdge(new SameEventLink(edge.weight), endpoints.getSecond(), endpoints.getFirst());
		}
		return directedGraph;
	}

	public static <V> DirectedGraph<V, SameEventLink> toDirected(Graph<V, SameEventLink> graph, Map<String, Item> itemsMap) {	
		DirectedGraph<V, SameEventLink> directedGraph = new DirectedSparseGraph<V, SameEventLink>();
	
		// Add all vertices first
		Collection<V> vertices = graph.getVertices();
		for(V vertex : vertices) {
			directedGraph.addVertex(vertex);
		}
		
		// Add directed edges
		for(SameEventLink edge : graph.getEdges()) {	
			Pair<V> endpoints = graph.getEndpoints(edge);
			
			V firstId = endpoints.getFirst();
			V secondId = endpoints.getSecond();
			
			Item firstItem = itemsMap.get(firstId);
			Item secondItem = itemsMap.get(secondId);
			
			//long dt = Math.abs(firstItem.getPublicationTime() - secondItem.getPublicationTime());
			double timeProximity = 1;//TemporalKernel.gaussian(dt, 72*3600*1000);
			
			if(firstItem.getTakenDate().after(secondItem.getTakenDate())) {
				directedGraph.addEdge(new SameEventLink(timeProximity*edge.weight), firstId, secondId);
			}
			else if(firstItem.getTakenDate().before(secondItem.getTakenDate())) {
				directedGraph.addEdge(new SameEventLink(timeProximity*edge.weight), secondId, firstId);
			}
			else {
				// If items have the same publication date then do not add any edge
				directedGraph.addEdge(new SameEventLink(timeProximity*edge.weight), secondId, firstId);
				directedGraph.addEdge(new SameEventLink(timeProximity*edge.weight), firstId, secondId);
			}
		}
		
		return directedGraph;
	}
	
	public static <V> Graph<V, SameEventLink> normalize(Graph<V, SameEventLink> graph) {
		Graph<V, SameEventLink> normalizedGraph = new DirectedSparseGraph<V, SameEventLink>();
		
		Collection<V> vertices = graph.getVertices();
		for(V vertex : vertices) {
			normalizedGraph.addVertex(vertex);
		}
		
		for(V vertex : vertices) {		
			Collection<V> successors = graph.getSuccessors(vertex);
			
			double totalWeight = 0;	
			for(V successor : successors) {
				SameEventLink edge = graph.findEdge(vertex, successor);
				if(edge != null) {
					totalWeight += edge.weight;
				}
			}
			
			if(totalWeight == 0)
				continue;
	
			for(V successor : successors) {
				SameEventLink edge = graph.findEdge(vertex, successor);
				if(edge == null)
					continue;
				
				Double normalizedWeight = edge.weight / totalWeight;
				
				SameEventLink normalizedEdge = new SameEventLink(normalizedWeight);
				normalizedGraph.addEdge(normalizedEdge, vertex, successor);
			}
		}
		
		return normalizedGraph;
	}
	
	public static  <V, E> Graph<V, E> filter(Graph<V, E> graph, final Collection<V> vertices) {
		
		Predicate<V> predicate = new Predicate<V>() {
			@Override
			public boolean evaluate(V vertex) {
				if(vertices.contains(vertex))
					return true;
			
				return false;
			}
		};
	
		//Filter graph
		Filter<V, E> verticesFilter = new VertexPredicateFilter<V, E>(predicate);
		graph = verticesFilter.transform(graph);

		return graph;
	}
	
	public static  <V> Graph<V, SameEventLink> filter(final Graph<V, SameEventLink> graph, final int degree) {
		Predicate<V> predicate = new Predicate<V>() {
			@Override
			public boolean evaluate(V vertex) {
				Collection<SameEventLink> incidentEdges = graph.getIncidentEdges(vertex);
				if(incidentEdges.size() > degree) {
					return true;
				}
				return false;
			}
		};
	
		//Filter graph
		Filter<V, SameEventLink> verticesFilter = new VertexPredicateFilter<V, SameEventLink>(predicate);
		return verticesFilter.transform(graph);
		
	}
	
	public static  <V> Graph<V, SameEventLink> filter(Graph<V, SameEventLink> graph, final double weight) {
		
		Predicate<SameEventLink> edgePredicate = new Predicate<SameEventLink>() {
			@Override
			public boolean evaluate(SameEventLink edge) {
				if(edge.weight > weight) {
					return true;
				}
				return false;
			}
		};
	
		//Filter graph
		Filter<V, SameEventLink> edgeFiler = new EdgePredicateFilter<V, SameEventLink>(edgePredicate);
		graph = edgeFiler.transform(graph);

		return graph;
	}
	
	public static void saveGraph(Graph<String, SameEventLink> graph, String filename) throws IOException {
		GraphMLWriter<String, SameEventLink> graphWriter = new GraphMLWriter<String, SameEventLink> ();
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filename)));
		
		graphWriter.addEdgeData("weight", null, "0", new Transformer<SameEventLink, String>() {
				@Override
				public String transform(SameEventLink edge) {
					return Double.toString(edge.weight);
				}
			}	
		);
		graphWriter.save(graph, out);
	}
	
	public static void saveGraph(Graph<String, SameEventLink> graph, File file) throws IOException {
		saveGraph(graph, file.toString());
	}
	
	public static Graph<String, SameEventLink> loadGraph(String filename) throws IOException {
		BufferedReader fileReader = new BufferedReader(new FileReader(filename));	
		Transformer<GraphMetadata, Graph<String, SameEventLink>> graphTransformer = new Transformer<GraphMetadata, Graph<String, SameEventLink>>() {
			public Graph<String, SameEventLink> transform(GraphMetadata metadata) {
				if (metadata.getEdgeDefault().equals(EdgeDefault.DIRECTED)) {
					return new DirectedSparseGraph<String, SameEventLink>();
				} else {
					return new UndirectedSparseGraph<String, SameEventLink>();
				}
			}
		};
		
		Transformer<NodeMetadata, String> vertexTransformer = new Transformer<NodeMetadata, String>() {
			public String transform(NodeMetadata metadata) {
				String vertex = metadata.getId();
				return vertex;
			}
		};
			
		Transformer<EdgeMetadata, SameEventLink> edgeTransformer = new Transformer<EdgeMetadata, SameEventLink>() {
			public SameEventLink transform(EdgeMetadata metadata) {
				Double weight = Double.parseDouble(metadata.getProperty("weight"));
				SameEventLink edge = new SameEventLink(weight);
				return edge;
			}
		};
		
		Transformer<HyperEdgeMetadata, SameEventLink> hyperEdgeTransformer = new Transformer<HyperEdgeMetadata, SameEventLink>() {
			public SameEventLink transform(HyperEdgeMetadata metadata) {
				Double weight = Double.parseDouble(metadata.getProperty("weight"));
				SameEventLink edge = new SameEventLink(weight);
				return edge;
			}
		};
					 
		GraphMLReader2<Graph<String, SameEventLink>, String, SameEventLink> graphReader = new GraphMLReader2<Graph<String, SameEventLink>, String, SameEventLink>(
				fileReader, graphTransformer, vertexTransformer, edgeTransformer, hyperEdgeTransformer);

		try {
			/* Get the new graph object from the GraphML file */
			Graph<String, SameEventLink> graph = graphReader.readGraph();
			return graph;
		} catch (GraphIOException ex) {
			return null;
		}
	}

	public static Graph<String, SameEventLink> loadGraph(File file) throws IOException {
		return loadGraph(file.toString());
	}
	
	public static Graph<String, SameEventLink> generateTextualGraph(Map<String, TFIDFVector> vectorsMap, double similarityThreshold) {

		Graph<String, SameEventLink> graph = new UndirectedSparseGraph<String, SameEventLink>();
		for(String node : vectorsMap.keySet()) {
			graph.addVertex(node);
		}
		
		String[] ids = vectorsMap.keySet().toArray(new String[vectorsMap.size()]);
		for(int i=0; i<ids.length-1; i++) {
			
			if(i%((int)(0.2*vectorsMap.size()))==0) {
				System.out.println(i + " / " + ids.length + " vectors processed => " + 
					graph.getVertexCount() + " vertices, " + graph.getEdgeCount() + " edges.");
			}
			
			String id1 = ids[i];
			TFIDFVector vector1 = vectorsMap.get(id1);
			if(vector1 == null)
				continue;
			
			for(int j=i+1; j<ids.length; j++) {
				
				String id2 = ids[j];
				TFIDFVector vector2 = vectorsMap.get(id2);
				if(vector2 == null)
					continue;
				
				Double similarity = vector1.cosineSimilarity(vector2);
				
				if(similarity > similarityThreshold) {
					SameEventLink link = new SameEventLink(similarity);
					graph.addEdge(link , id1, id2);
				}
			}
		}		
		return graph;
	}
	
	public static Graph<String, SameEventLink> generateVisualGraph(Map<String, Item> itemsMap, double similarityThreshold) {

		Graph<String, SameEventLink> graph = new UndirectedSparseGraph<String, SameEventLink>();
		for(Item item : itemsMap.values()) {
			if(item.getVlad() != null) {
				graph.addVertex(item.getId());
			}
		}
		
		String[] itemIds = itemsMap.keySet().toArray(new String[itemsMap.size()]);
		for(int i=0; i<itemIds.length-1; i++) {
			if(i%((int)(0.2*itemsMap.size()))==0) {
				System.out.println(i + " / " + itemIds.length + " items processed => " +
						graph.getVertexCount() + " vertices " + graph.getEdgeCount() + " edges!");
			}
			
			String id1 = itemIds[i];
			Item item1 = itemsMap.get(id1);
			double[] vector1 = item1.getVlad();
			if(vector1 == null)
				continue;
			
			for(int j=i+1; j<itemIds.length; j++) {
				String id2 = itemIds[j];
				Item item2 = itemsMap.get(id2);
				
				double[] vector2 = item2.getVlad();
				if(vector2 == null)
					continue;
				
				double similarity = L2.similarity(vector1, vector2);
				if(similarity > similarityThreshold) {
					SameEventLink link = new SameEventLink(similarity);
					graph.addEdge(link , id1, id2);
				}
			}
		}		
		return graph;
	}

	public static void fold(Graph<String, SameEventLink> graph, List<Set<String>> clusters) {
		
		int clustered = 0, removedEdges=0;
		for(Set<String> cluster : clusters) {
			
			List<String> list = new ArrayList<String>(cluster);
			Collections.sort(list);
			String newVertex = StringUtils.join(list, "-");
			
			//System.out.println("Cluster " + clusters.indexOf(cluster) + " size " + cluster.size());
			clustered += cluster.size();
			
			for(String v1 : cluster) {
				for(String v2 : cluster) {
					SameEventLink edge = graph.findEdge(v1, v2);
					if(edge != null) {
						removedEdges++;
						graph.removeEdge(edge);
					}
				}
			}
			//System.out.println("Between edges to remove:  " + edgesToRemove);

			Map<String, Set<SameEventLink>> map = new HashMap<String, Set<SameEventLink>>();
			
			for(String vertex : cluster) {
				Collection<String> neighbors = new ArrayList<String>(graph.getNeighbors(vertex));
				//System.out.println(vertex + " => " + neighbors.size());
				for(String neighbor : neighbors) {
					SameEventLink edge = graph.findEdge(vertex, neighbor);
					if(edge != null) {
						removedEdges++;
						graph.removeEdge(edge);
						Set<SameEventLink> edges = map.get(neighbor);
						if(edges == null) {
							edges = new HashSet<SameEventLink>();
							map.put(neighbor, edges);
						}
						edges.add(edge);
					}
				}
				graph.removeVertex(vertex);
			}
			
			
			graph.addVertex(newVertex);
			
			for(String neighbor : map.keySet()) {
				Set<SameEventLink> edges = map.get(neighbor);
				SameEventLink maxEdge = Collections.max(edges);
				graph.addEdge(maxEdge, neighbor, newVertex);
			}
		}
		
		System.out.println("Clustered Vertices: " + clustered + ", Removed Edges: " + removedEdges);
	}
	
	public static Transformer<SameEventLink, Double> getEdgeTransformer() {
		return new Transformer<SameEventLink, Double>() {
			@Override
			public Double transform(SameEventLink edge) {
				return edge.weight;
			}	
		};
	}

	public static void merge(Graph<String, SameEventLink> graph1, Graph<String, SameEventLink> graph2) {
		for(String vertex : graph2.getVertices()) {
			if(!graph1.containsVertex(vertex)) {
				graph1.addVertex(vertex);
			}
		}
		for(SameEventLink edge : graph2.getEdges()) {
			if(!graph1.containsEdge(edge)) {
				Pair<String> endpoints = graph2.getEndpoints(edge);
				String first = endpoints.getFirst();
	  	    	String second = 	endpoints.getSecond();
			
	  	    	graph1.addEdge(edge, first, second);
			}
		}
	}

	public static double density(Graph<?, SameEventLink> graph) {
		double density = .0;
		
		double edges = graph.getEdgeCount();
		double vertices = graph.getVertexCount();
		if(vertices > 1) {
			density = 2 * edges / (vertices * (vertices - 1));
		}
		
		return density;
	}
	
}
