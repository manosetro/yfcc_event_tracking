package gr.iti.mklab.yfcc.geometry.convexhull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import gr.iti.mklab.yfcc.geometry.utils.Stack;
import gr.iti.mklab.yfcc.geometry.utils.LinkedStack;
import gr.iti.mklab.yfcc.geometry.utils.QuickSort;
import gr.iti.mklab.yfcc.geometry.algorithms.Point2f;
import gr.iti.mklab.yfcc.geometry.algorithms.RadialComparator;
import gr.iti.mklab.yfcc.models.Point;

/**
 * @author Jukka Moisio
 */ 
public class GrahamScan implements ConvexHullFunction {

        /**
		 *
		 */
        private Point2f[] preSort(Point2f[] pts)
        {
            Point2f t;

            // find the lowest point in the set. If two or more points have
            // the same minimum y coordinate choose the one with the minimu x.
            // This focal point is put in array location pts[0].
            for (int i = 1; i < pts.length; i++)
            {
                if ((pts[i].getY() < pts[0].getY()) || ((pts[i].getY() == pts[0].getY() ) 
                     && (pts[i].getX() < pts[0].getX() )))
                {
                    t = pts[0];
                    pts[0] = pts[i];
                    pts[i] = t;
                }
            }

            // sort the points radially around the focal point.
            QuickSort.sort(pts, new RadialComparator(pts[0]));

            return pts;
        }

        public List<Map<String, Double>> getConvexHull(List<Point> points) {
        	
        	Set<String> uniquePoints = new HashSet<String>();
        	
        	List<Point2f> pts = new ArrayList<Point2f>();
        	for(Point point : points) {
        		String pointHash = getPointHash(point);
        		if(uniquePoints.contains(pointHash)) {
					continue;
				}
				uniquePoints.add(pointHash);
				
        		pts.add(new Point2f(point.getLatitude(), point.getLongitude()));
        	}
        	
        	if(pts.size()<=2) {
        		return null;
        	}
        	
        	Stack<Point2f> convexHull = getConvexHull(pts.toArray(new Point2f[pts.size()]));
        	List<Map<String, Double>> convexHull_points = new ArrayList<Map<String, Double>>(convexHull.size());
        	for(int i=0; i<convexHull.size(); i++) {
        		Point2f p = convexHull.pop();
        		convexHull_points.add(new Point(p.getX(), p.getY()).toMap());
        	}
        	
        	//Collections.reverse(convexHull_points);
        	return convexHull_points;
        }
        
        private static String getPointHash( Point point ) {
    		int latInt = (int)Math.floor(point.getLatitude() * 10000000);
    		int lonInt = (int)Math.floor(point.getLongitude() * 10000000);
    		return String.valueOf(latInt) + "_" + String.valueOf(lonInt);
    	}
        
	/**
	 * implements interface
	 * @author Jukka Moisio
	 */
	public Stack<Point2f> getConvexHull(Point2f[] pts) {
		@SuppressWarnings("unused")
		Point2f[] sorted = preSort(pts);
		Point2f p;
		RadialComparator c = new RadialComparator(pts[0]);
		
		// palautettava pino
		Stack<Point2f> s = new LinkedStack<Point2f>();
        
		s.push(pts[pts.length - 1]);
		s.push(pts[0]);
		s.push(pts[1]);
		for (int i = 2; i < pts.length - 1; i++) {
				p = s.pop();
				c.setOrigin(s.peek());;
				
				while (c.compare(p, pts[i]) > 0) {
					p = s.pop();
					c.setOrigin(s.peek());
				}
				
				s.push(p);
				s.push(pts[i]);
		}
		
		// Tarkistetaan viel�, pit��k� viimeiseksi lis�tyn pisteen
		// todella kuulua pinoon.
		p = s.pop();
		
		c.setOrigin(s.peek());
		if (c.compare(p, pts[pts.length - 1]) <= 0)
			s.push(p);

		s.push( pts[pts.length - 1] ); 
			
		return s;
	}
}