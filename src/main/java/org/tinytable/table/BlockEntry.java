package org.tinytable.table;

import java.io.Serializable;

public class BlockEntry implements Serializable, Comparable<BlockEntry> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String k;
	public String v;
	public long timeT;
	public BlockEntry(String s1, String s2, long t) {
		k = s1; v= s2; timeT = t;
	}
	
	public int compareTo(BlockEntry other) {
		return this.k.compareTo(other.k);
	}
}
