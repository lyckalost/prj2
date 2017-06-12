package org.tinytable.db;

import org.tinytable.table.ColTable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class TinyCol {

	public TinyCol(TinyTable table, String name) throws IOException, ClassNotFoundException {
		// TODO Auto-generated constructor stub
		upperTable = table;
		colName = name;
		colPath = upperTable.getTablePath() + "/" + name;
		colTable = new ColTable(name, colPath);



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


	public String get(String key) throws IOException, ClassNotFoundException {
		return colTable.get(key);
	}

	public String put(String key, String value) throws IOException, ClassNotFoundException {
		return colTable.put(key, value);
	}



	private TinyTable upperTable;
	private String colName;
	private String colPath;
	public ColTable colTable;
}
