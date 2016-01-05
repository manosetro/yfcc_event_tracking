package gr.iti.mklab.yfcc.geometry.utils;

/**
 * Linkitetty lista.
 *
 * @.classInvariant { size >= 0, FOREACH( node in list ) node != null }
 *
 * @author Teemu Linkosaari
 */
public class LinkedList<T> implements List<T> {
    
    	/** Listan koko */
    	private int size;
    
    	/** Viittaus listan päähän. */
    	private Node<T> head = null;

    	/** Viittaus listan per�päähän. */
    	private Node<T> tail = null;
    
    	/**
    	 *  Luo tyhjän listan.
    	 * 
     	 * .@pre {true}
    	 * .@post { size() == 0 and this.search(0) == null }
    	 */
    	public LinkedList() {
    	    size = 0;
    	    head = null;
    	}
    
    	/**
    	 * @.pre { 0 <= index <= size()-1 }
    	 */
    	public T search(int index) {
        	if (index > size)
            throw new IllegalArgumentException();
        
        	Node<T> node = head;
        	for(int i = 0; i < index; i++) {
            	node = node.next();
        	}
        	return node.get();
    	}

    	public void insert(T data) {
        	if( head == null ) {
			this.head = new Node<T>(data, null);
			this.tail = head;
	  	} else {
			Node<T> n = new Node<T>(data, null);
			this.tail.setNext( n );
			tail = n;
	  	}

        	this.size++;
    	}

    	public T delete(T obj) {
        	throw new UnsupportedOperationException("Not supported yet.");
        	//size -= 1;
    	}

    	public int size() {
        	return size;
	}

	/**
       * Metodin sis�ll� ei voida luoda taulukkoa, koska
	 * Javassa taulukkoa ei voida luoda suoraan tyyppiparametriksi.
 	 * Eli new T[ koko ] ei k��nny.
	 * @.pre { table.length == this.size() }
	 * @.post { FORALL( i : table[i] == this.search( i ) ) }
       */
    	public T[] toArray(T[] table) {		
		assert table.length == this.size();

        	Node<T> node = this.head;
        
		for(int i = 0; i < size; i++) {
			table[i] = node.get();
            	node = node.next();
        	}
	
		return table;
    	}
}
