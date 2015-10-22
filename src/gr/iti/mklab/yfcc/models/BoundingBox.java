package gr.iti.mklab.yfcc.models;


import gr.iti.mklab.yfcc.utils.IOConstants;

public class BoundingBox {

	private Point bottomLeft;
	private Point topRight;
	
	
	public BoundingBox(Point bottomLeftCorner, Point topRightCorner) {
		this.bottomLeft = bottomLeftCorner;
		this.topRight = topRightCorner;
	}

	public BoundingBox(Polygon polygon){
		double minLon = 1000;
		double maxLon = -1000;
		double minLat = 1000;
		double maxLat = -1000;
		for(Point point : polygon.getPoints()){
			if(point.getLatitude() < minLat){
				minLat = point.getLatitude();
			}
			if(point.getLatitude() > maxLat){
				maxLat = point.getLatitude();
			}
			if(point.getLongitude() < minLon){
				minLon = point.getLongitude();
			}
			if(point.getLongitude() > maxLon){
				maxLon = point.getLongitude();
			}
		}
		this.bottomLeft = new Point(minLon, minLat);
		this.topRight = new Point(maxLon, maxLat);
	}

	public Point getBottomLeft() {
		return bottomLeft;
	}


	public Point getTopRight() {
		return topRight;
	}


	@Override
	public String toString() {
		return IOConstants.LEFT_BRACKET + bottomLeft + IOConstants.COMMA +
			IOConstants.WHITE_SPACE + topRight + IOConstants.RIGHT_BRACKET;
	}
	
	public boolean contains(Point point){
		if((point.getLatitude() < this.bottomLeft.getLatitude()) || (point.getLatitude() > this.topRight.getLatitude())){
			return false;
		}
		if( (point.getLongitude() < this.bottomLeft.getLongitude()) || (point.getLongitude() > this.topRight.getLongitude()) ){
				return false;
		}
		return true;
	}
}
