package gr.iti.mklab.yfcc.ranking;

import edu.uci.ics.jung.algorithms.matrix.GraphMatrixOperations;
import edu.uci.ics.jung.algorithms.scoring.EigenvectorCentrality;
import edu.uci.ics.jung.algorithms.scoring.HITS.Scores;
import edu.uci.ics.jung.algorithms.scoring.PageRank;
import edu.uci.ics.jung.algorithms.scoring.HITS;
import edu.uci.ics.jung.algorithms.scoring.PageRankWithPriors;
import edu.uci.ics.jung.graph.Graph;
import edu.ucla.sspace.matrix.DivRank;
import edu.ucla.sspace.matrix.SparseHashMatrix;
import edu.ucla.sspace.matrix.SparseMatrix;
import edu.ucla.sspace.vector.DenseVector;
import edu.ucla.sspace.vector.DoubleVector;
import gr.iti.mklab.yfcc.models.SameEventLink;
import gr.iti.mklab.yfcc.utils.GraphUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.collections15.Transformer;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.IntArrayList;
import cern.colt.matrix.impl.SparseDoubleMatrix2D;

public class GraphRanker {
	
	public static double d = 0.75; // Random Jump Weight
	
	// Stoping criteria
	private static double tolerance = 0.000001;
	private static int maxIterations = 400;
	
	public static Map<String, Double> pagerankScoring(Graph<String, SameEventLink>  graph) {
		
		Transformer<SameEventLink, Double> edgeTransformer = GraphUtils.getEdgeTransformer();
		PageRank<String, SameEventLink> ranker = new PageRank<String, SameEventLink>(graph, edgeTransformer , d);
		
		ranker.setTolerance(tolerance) ;
		ranker.setMaxIterations(maxIterations);
		ranker.evaluate();
	 
		System.out.println("Iterations: " + ranker.getIterations());
		System.out.println("Tolerance: " + ranker.getTolerance());
		
		double maxScore = 0;
		Collection<String> vertices = graph.getVertices();
		Map<String, Double> verticesMap = new TreeMap<String, Double>();
		for(String vertex : vertices) {
			Double score = ranker.getVertexScore(vertex);
			
			if(score > maxScore) {
				maxScore = score;
			}
			
			verticesMap.put(vertex, score);
		}
		
		if(maxScore > 0) {
			for(Entry<String, Double> ve : verticesMap.entrySet()) {
				verticesMap.put(ve.getKey(), ve.getValue()/maxScore);
			}
		}
		
		return verticesMap;
	}
	
	public static Map<String, Double> pagerankScoring(Graph<String, SameEventLink>  graph, final Map<String, Double> priors) {
		
		Transformer<SameEventLink, Double> edgeTransformer = GraphUtils.getEdgeTransformer();
		Transformer<String, Double> priorsTransformer = new Transformer<String, Double>() {
			@Override
			public Double transform(String vertex) {
				Double vertexPrior = priors.get(vertex);
				if(vertexPrior == null)
					return 0d;
				
				return vertexPrior;
			}
		};
		
		PageRankWithPriors<String, SameEventLink> ranker = 
				new PageRankWithPriors<String, SameEventLink>(graph, edgeTransformer, priorsTransformer, d);
		
		ranker.setTolerance(tolerance) ;
		ranker.setMaxIterations(maxIterations);
		ranker.evaluate();
	
		double maxScore = 0;
		Collection<String> vertices = graph.getVertices();
		Map<String, Double> verticesMap = new TreeMap<String, Double>();
		for(String vertex : vertices) {
			Double score = ranker.getVertexScore(vertex);
			
			if(score > maxScore)
				maxScore = score;
			
			verticesMap.put(vertex, score);
		}
	
		if(maxScore > 0) {
			for(Entry<String, Double> ve : verticesMap.entrySet()) {
				verticesMap.put(ve.getKey(), ve.getValue()/maxScore);
			}
		}
		
		return verticesMap;
	}
	
	public static Map<String, Double> hitsScoring(Graph<String, SameEventLink>  graph) {
		
		Transformer<SameEventLink, Double> edgeTransformer = GraphUtils.getEdgeTransformer();
		HITS<String, SameEventLink> ranker = 
				new HITS<String, SameEventLink>(graph, edgeTransformer , d);
		
		ranker.setTolerance(tolerance) ;
		ranker.setMaxIterations(maxIterations);
		ranker.evaluate();
	
		Collection<String> vertices = graph.getVertices();
		Map<String, Double> verticesMap = new TreeMap<String, Double>();
		for(String vertex : vertices) {
			Scores hitsScores = ranker.getVertexScore(vertex);
			Double authorityScore = hitsScores.authority;
			//Double hubScore = hitsScores.hub;	
			verticesMap.put(vertex, authorityScore);
			//temp.put(vertex, hubScore);
		}
		return verticesMap;
	}

	public static TreeMap<String, Double> eigenvectorScoring(Graph<String, SameEventLink>  graph) {
		
		EigenvectorCentrality<String, SameEventLink> ranker = 
				new EigenvectorCentrality<String, SameEventLink>(graph, GraphUtils.getEdgeTransformer());
		ranker.evaluate();
		
		Collection<String> vertices = graph.getVertices();
		TreeMap<String, Double> verticesMap = new TreeMap<String, Double>();
		for(String vertex : vertices) {
			Double score = ranker.getVertexScore(vertex);
			verticesMap.put(vertex, score);
		}
		
		return verticesMap;
	}
	
	public static Map<String, Double> divrankScoring(Graph<String, SameEventLink>  graph) {
		
		Map<String, Double> priors = new HashMap<String, Double>();			
		for(String vertex : graph.getVertices()) {
			priors.put(vertex, 1.0 / graph.getVertexCount());
		}

		return divrankScoring(graph, priors);
	}
	
	public static Map<String, Double> divrankScoring(Graph<String, SameEventLink>  graph, final Map<String, Double> priors) {

		List<String> vertices = new ArrayList<String>(graph.getVertices());
		
		double[] initialScores = new double[vertices.size()];			
		
		int i = 0;
		for(String vertex : vertices) {
			Double priorScore = priors.get(vertex);
			if(priorScore == null)
				priorScore = .0;
			
			initialScores[i] = priorScore;
			i++;
		}

		SparseDoubleMatrix2D matrix = GraphMatrixOperations.graphToSparseMatrix(graph);
		
		IntArrayList iIndinces = new IntArrayList();
		IntArrayList jIndinces = new IntArrayList();
		DoubleArrayList weights = new DoubleArrayList();
		matrix.getNonZeros(iIndinces, jIndinces, weights);
	
		SparseMatrix affinityMatrix = new SparseHashMatrix(vertices.size(), vertices.size());
		for(int index=0; index<weights.size(); index++) {
			affinityMatrix.set(iIndinces.get(index), jIndinces.get(index), weights.get(index));
		}
		DoubleVector initialRanks = new DenseVector(initialScores);
		
		DivRank ranker = new DivRank(d);
		DoubleVector ranks = ranker.rankMatrix(affinityMatrix, initialRanks);
		
		Map<String, Double> verticesMap = new TreeMap<String, Double>();
		for(int index = 0 ; index<vertices.size(); index++) {
			String vertex = vertices.get(index);
			double score = ranks.get(index);	
			verticesMap.put(vertex, score);
		}
	
		double maxScore = .0;
		if(!verticesMap.isEmpty()) {
			maxScore = Collections.max(verticesMap.values());
		}
		
		if(maxScore > 0) {
			for(Entry<String, Double> vertexEntry : verticesMap.entrySet()) {
				verticesMap.put(vertexEntry.getKey(), vertexEntry.getValue()/maxScore);
			}
		}

		return verticesMap;
	}

	public static Map<String, Double> getPriors(Collection<String> ids, Map<String, Integer> scores) {
		
		Map<String, Double> priors = new HashMap<String, Double>();
		Double sum = 0d;
		for(String id : ids) {
			Integer popularity = scores.get(id);
			if(popularity != null) {
				sum += (popularity + 1);
			}
		}
		
		for(String id : ids) {
			Integer score = scores.get(id);
			if(score != null) {
				priors.put(id, (score.doubleValue() + 1)/sum);
			}
			else {
				priors.put(id, .0);
			}
		}
		return priors;
	}
}
