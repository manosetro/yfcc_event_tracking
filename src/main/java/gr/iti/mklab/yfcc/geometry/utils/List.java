package gr.iti.mklab.yfcc.geometry.utils;

/**
 *
 * @author Teemu
 */
public interface List<T> {
    
    /**
     * 
     * @param index
     * @return
     */
    public T search(int index);
    
    /**
     * 
     * @param obj
     */
    public void insert(T obj);
    
    /**
     * 
     * @param obj
     * @return
     */
    public T delete(T obj);
    
    /**
     * 
     * @return
     */
    public int size();

    public T[] toArray(T[] a);
}
