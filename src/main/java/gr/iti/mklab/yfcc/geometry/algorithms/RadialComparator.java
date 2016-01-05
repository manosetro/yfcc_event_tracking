package gr.iti.mklab.yfcc.geometry.algorithms;

import gr.iti.mklab.yfcc.geometry.utils.IComparator;

/**
 * @author Jukka Moisio, Teemu Linkosaari
 */
public class RadialComparator implements IComparator<Point2f> {

	/** Origo, jonka suhteen verrataan */
	private Point2f origin;

	/**
       * @.pre { origin != null )
	 */
	public RadialComparator( Point2f origin ) {
		assert origin != null;
		this. origin = origin;
	}
	
	public int compare( Point2f p1, Point2f p2 ) {
		return polarCompare(origin, p1, p2);
	}
	
	/**
	 * M��ritet��n uusi origo
 	 */
	public void setOrigin( Point2f newO ) {
		origin = newO;
	}

      private static int polarCompare(Point2f o, Point2f p, Point2f q) {
                double dxp = p.getX() - o.getX();
                double dyp = p.getY() - o.getY();
                double dxq = q.getX() - o.getX();
                double dyq = q.getY() - o.getY();
             
                int orient = ComputationalGeometry.computeOrientation(o, p, q);

                if(orient == ComputationalGeometry.CounterClockwise)
                    return -1;
                if(orient == ComputationalGeometry.Clockwise) 
                    return 1;

                // points are collinear - check distance
                double op = dxp * dxp + dyp * dyp;
                double oq = dxq * dxq + dyq * dyq;
                if (op < oq)
                    return 1;                
                if (op > oq)
                    return -1;

                return 0;
            }
}