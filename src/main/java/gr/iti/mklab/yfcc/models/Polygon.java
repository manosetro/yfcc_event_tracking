package gr.iti.mklab.yfcc.models;

import java.util.Arrays;
import java.util.List;

public class Polygon {

	private Point[] points;
	
	public Polygon(List<Point> points){
		this.points = new Point[points.size()];
		for (int i = 0; i < points.size(); i++) {
			this.points[i] = points.get(i);
		}
	}
	
	public Point getPointAt(int i) {
		return points[i];
	}
	
	public void setPointAt(int i, Point point) {
		points[i] = point;
	}
	
	public List<Point> getPoints(){
		return Arrays.asList(points);
	}
	
	public double area() { return Math.abs(signedArea()); }

    // return signed area of polygon
    public double signedArea() {
    	
        double sum = 0.0;
        for (int i = 0; i < points.length; i++) {
        	if(i == (points.length -1)){
        		sum = sum + (points[i].getLatitude() * points[0].getLongitude()) - (points[i].getLongitude() * points[0].getLatitude());
        	}else{
        		sum = sum + (points[i].getLatitude() * points[i+1].getLongitude()) - (points[i].getLongitude() * points[i+1].getLatitude());
        	}
        }
        return 0.5 * sum;
    }
    
    public boolean contains(Point p0) {
        int crossings = 0;
        for (int i = 0; i < points.length; i++) {
        	
        	if(i == (points.length -1)){
        		double slope = (points[0].getLatitude() - points[i].getLatitude())  / (points[0].getLongitude() - points[i].getLongitude());
        		if(Double.isNaN(slope)){
        			continue;
        		}
        		
                boolean cond1 = (points[i].getLongitude() <= p0.getLongitude()) && (p0.getLongitude() < points[0].getLongitude());
                boolean cond2 = (points[0].getLongitude() <= p0.getLongitude()) && (p0.getLongitude() < points[i].getLongitude());
                boolean cond3 = p0.getLatitude() <  slope * (p0.getLongitude() - points[i].getLongitude()) + points[i].getLatitude();
                if ((cond1 || cond2) && cond3) crossings++;
        	}else{
        		double slope = (points[i+1].getLatitude() - points[i].getLatitude())  / (points[i+1].getLongitude() - points[i].getLongitude());
        		
        		if(Double.isNaN(slope)){
        			continue;
        		}
        		
                boolean cond1 = (points[i].getLongitude() <= p0.getLongitude()) && (p0.getLongitude() < points[i+1].getLongitude());
                boolean cond2 = (points[i+1].getLongitude() <= p0.getLongitude()) && (p0.getLongitude() < points[i].getLongitude());
                boolean cond3 = p0.getLatitude() <  slope * (p0.getLongitude() - points[i].getLongitude()) + points[i].getLatitude();
                if ((cond1 || cond2) && cond3) crossings++;
        	}
        	
            
        }
        return (crossings % 2 != 0);
    }
	
}
