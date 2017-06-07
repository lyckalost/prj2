package org.tinytable.db;

import java.io.File;

public class TinyCol {

	public TinyCol(TinyTable table, String name) {
		// TODO Auto-generated constructor stub
		upperTable = table;
		colName = name;
		colPath = upperTable.getTablePath() + "/" + name;
		File dir = new File(colPath);
		if (dir.exists()) {;}
		else {
			boolean successful = dir.mkdir();
			if (successful) {
				System.out.println("col " + colPath + " created successfully");
			}
			else {
				System.out.println("col " + colPath + " create failed!");
			}
		}
	}
	
	public String getColName() {
		return colName;
	}

	private TinyTable upperTable;
	private String colName;
	private String colPath;	
}
