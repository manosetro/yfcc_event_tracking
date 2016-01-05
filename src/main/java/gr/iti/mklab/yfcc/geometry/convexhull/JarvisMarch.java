package gr.iti.mklab.yfcc.geometry.convexhull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import gr.iti.mklab.yfcc.geometry.utils.QuickSort;
import gr.iti.mklab.yfcc.geometry.utils.Stack;
import gr.iti.mklab.yfcc.geometry.utils.LinkedStack;
import gr.iti.mklab.yfcc.geometry.algorithms.Point2f;
import gr.iti.mklab.yfcc.geometry.algorithms.XYComparator;
import gr.iti.mklab.yfcc.models.Point;

/**
 * Luokka kääntyy, muttei toimi vielä.
 * Edit: Lista on muutettu taulukoksi.
 * 
 * @author Ossi Lehto
 */
public class JarvisMarch implements ConvexHullFunction {

    /**
     * @.pre {true}
     */
    public JarvisMarch() {
    }

    /**
     * Laskee kolmion p1p2p3 alan
     */
    private double crossProduct(Point2f p1 , Point2f p2, Point2f p3) {
        return (p2.getX() - p1.getX())*(p3.getY() - p1.getY()) - (p3.getX() - p1.getX())*(p2.getY() - p1.getY());
    }

    
    public List<Map<String, Double>> getConvexHull(List<Point> points) {
    	Point2f[] pts = new Point2f[points.size()];
    	int i = 0;
    	for(Point point : points) {
    		pts[i++] = new Point2f(point.getLatitude(), point.getLongitude());
    	}
    	
    	Stack<Point2f> convexHull = getConvexHull(pts);
    	List<Map<String, Double>> convexHull_point = new ArrayList<Map<String, Double>>(convexHull.size());
    	for(i=0; i<convexHull.size(); i++) {
    		Point2f p = convexHull.pop();
    		convexHull_point.add(new Point(p.getX(), p.getY()).toMap());
    	}
    	
    	return convexHull_point;
    }
    
    
    /** Palauttaa pinossa peitteen pisteet niin,
     *  että ensimmäinen ja viimeinen alkio ovat samat.
     * 
     * 
     * .@pre {Q.length > 1}
     */
    public Stack<Point2f> getConvexHull(Point2f[] Q){
        QuickSort.<Point2f>sort( Q , new XYComparator() );
        int lowest = 0;
        int highest = Q.length-1;
        int n = 0;
        Stack<Integer> collinear = new LinkedStack<Integer>();
        boolean pass = true;

        // Palautettava pino.
        Stack<Point2f> S = new LinkedStack<Point2f>();
        S.push(Q[0]);

        /** Oikea ketju **/
        while (n < highest) {
            n++;
            /* Kokeillaan pistettä n seuraavaksi palautettavaksi pisteeksi */
            for(int i=n+1; i<=Q.length-1; i++) {
                // löytyi piste, joka oli viivan väärällä puolella
                if(crossProduct(S.peek(),Q[n],Q[i]) < 0)  {
                    pass = false;
                    break; // lopettaa ristitulojen tarkastamisen, pisteen vaihto
                }
                // löytyi kollineaarinen piste
                if(crossProduct(S.peek(),Q[n],Q[i]) == 0) {
                    collinear.push( i );
                }
            } // for

             // seuraava piste löydetty
            if (pass) {
                
                // Kollienaarisista pisteistä otetaan viimeiseksi lisätty,
                // koska se on kauimmainen
                if(!collinear.empty()) { n = collinear.pop(); }
                
                S.push(Q[n]);
            } // if
            // alustetaan tarkistus
            while(!collinear.empty()) {collinear.pop();}
            pass = true;
        
        } // while

        /** Vasen ketju, tässä vaiheessa n==highest **/
        while (n > lowest) {
            n--;
            // Kokeillaan pistettä n seuraavaksi palautettavaksi pisteeksi
            for(int i=n-1; i >= 0; i--) {
                // löytyi piste, joka oli viivan väärällä puolella
                if(crossProduct(Q[n],S.peek(),Q[i]) > 0) {
                    pass = false;
                    // lopettaa ristitulojen tarkastamisen, pisteen vaihto
                    break; 
                }

                // löytyi kollineaarinen piste
                if(crossProduct(Q[n],S.peek(),Q[i]) == 0) {
                    collinear.push(i);
                }
            
            } // for

             // seuraava piste löydetty
            if (pass) {
                
                // Kollienaarisista pisteistä otetaan viimeiseksi lisätty,
                // koska se on kauimmainen
                if(!collinear.empty()) { n = collinear.pop(); }
                
                S.push(Q[n]);
            } // if
            // alustetaan tarkistus
            while(!collinear.empty()) {collinear.pop();}
            pass = true;
        
        } // while
        
        return S; 
    }

} // end of class
