package org.tinytable.db;

import java.io.Serializable;

public class entry implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String k;
	public String v;
	public entry(String s1, String s2) {
		k = s1; v= s2;
	}
}