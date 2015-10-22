package gr.iti.mklab.yfcc.geometry.utils;

/**
 *
 * @author Teemu
 */
public interface Stack<T> {
    	public void push(T s);
    	public boolean empty();
    	public T pop();
   	public T peek();
	public int size();
}
