package gr.iti.mklab.yfcc.geometry.algorithms;

/**
 * Implements windowing transformation.
 * Screen coordinates are transformed to world coordinates and vice versa.
 *
 * @author Teemu Linkosaari
 */
public class Converter {
    
    private int ww,hh;
    private double w,h;


    	public Converter(int screen_w, int screen_h, double world_w, double world_h) {
        this.ww = screen_w;
        this.hh = screen_h;
        this.h = world_h;
        this.w = world_w;
    	}
    	
	/**
	 * Screen coordinate to World coordinate.
	 */ 
	public Point2f to( Point2f p ) {
		double x = p.getX()*2/ww*w - w;
		double y = -p.getY()*2/hh*h + h;
		
		return new Point2f( x, y );
	}

	/**
	 * World coordinate to Screen coordinate.
	 */ 	
	public Point2f from( Point2f p ) {
		int x = (int)(  ww/2 + (p.getX() / w)*ww/2  );
		int y = (int)(  hh/2 - (p.getY() / h)*hh/2  );
		
		return new Point2f( x, y );
	}
}    
