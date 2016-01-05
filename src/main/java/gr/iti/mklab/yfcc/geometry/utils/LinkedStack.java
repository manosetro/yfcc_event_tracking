package gr.iti.mklab.yfcc.geometry.utils;

/**
 *
 * @author Teemu
 */
public class LinkedStack<T> implements Stack<T> {

	private int size;

	public LinkedStack() {
		size = 0;
	}

    private Node<T> top;
    
    public void push(T s) {
		top = new Node<T>(s, top);
		size = size + 1;
    }

    public boolean empty() {
        return top == null;
    }

    /**
     * Palauttaa ja poistaa viitauksen pinon päällimäiseen.
     * .@pre { !empty() }
     * .@post { Jos push(s) ja push(t), niin
     *        peek() = t, pop() = t ja peek() = s }
     */
    public T pop() {
        if (top == null) {
            throw new UnsupportedOperationException("Not supported yet.");        
        } else {
            T ret = top.get();
            top = top.next();
		size = size - 1;
            return ret;            
        }
    }
    
    /**
     * Palauttaa viitauksen pinon päällimäiseen.
     * .@pre { !empty() }
     */
    public T peek() {
        return top.get();
    }

	public int size() {
		return this.size;
	}
}

