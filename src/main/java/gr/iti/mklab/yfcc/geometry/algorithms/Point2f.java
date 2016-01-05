package gr.iti.mklab.yfcc.geometry.algorithms;

/**
 *
 * @author Teemu
 */
public class Point2f {
    
    /** */
    private float x;
    
    /** */
    private float y;
    
    /**
     * 
     */
    public Point2f(double x, double y) {   
	this.x = (float)x;
	this.y = (float)y;
    }

    public Point2f() {   
    }
    
    public float getX() {
        return x;
    }
    
    public float getY() {
        return y;
    }

    /**
     * Aseta koordinaatti x
     * @param nx uusi x-koordinaatti
     * @pre {true}
     * @post { nx == getX() }
     */
    public void setX(float nx) {
        this.x = nx;
    }
    
    /**
     * Aseta koordinaatti y
     * @param ny uusi y-koordinaatti
     * @pre {true}
     * @post { ny == getY() }
     */
    public void setY(float ny) {
        this.y = ny;
    }

    	public String toString() {
		return "(" + x + "," + y + ")";
	}
}
