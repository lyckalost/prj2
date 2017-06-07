package org.tinytable.table;

import java.io.Serializable;

public class MetaBlockEntry implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public MetaBlockEntry(String sKey, String eKey, int bSize, int eNum) {
		// TODO Auto-generated constructor stub
		startKey = sKey;
		endKey = eKey;
		bufSize = bSize;
		entryNum = eNum;
	}
	
	public boolean equals(MetaBlockEntry other) {
		if (!this.startKey.equals(other.startKey))
			return false;
		if (!this.endKey.equals(other.endKey))
			return false;
		if (this.bufSize != other.bufSize)
			return false;
		if (this.entryNum != other.entryNum)
			return false;
		return true;
	}
	
	public String startKey;
	public String endKey;
	public int bufSize;
	public int entryNum;

}
