package org.tinytable.db;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class TinyTable {

	public TinyTable(TinyDB db, String name) throws IOException, ClassNotFoundException {
		// TODO Auto-generated constructor stub
		upperDB = db;
		tableName = name;
		colMap = new HashMap<String, TinyCol>();
		tablePath = upperDB.getDBPath() + "/" + tableName;
		
		File dir = new File(tablePath);
		if (dir.exists()) 
			loadTableInfo(dir);
		else {
			boolean successful = dir.mkdir();
			if (successful) {
				System.out.println("table " + tablePath + " created successfully");
			}
			else {
				System.out.println("table " + tablePath + " create failed!");
			}
		}
	}
	
	private void loadTableInfo(File dir) throws IOException, ClassNotFoundException {
		String[] colList = dir.list();
		for (int i = 0; i < colList.length; i++) {
			colMap.put(colList[i], createCol(colList[i]));
		}
	}
	
	public TinyCol createCol(String colName) throws IOException, ClassNotFoundException {
		if (colMap.containsKey(colName))
			return colMap.get(colName);
		TinyCol newCol = new TinyCol(this, colName);
		colMap.put(colName, newCol);
		return newCol;
	}
	
	public String getTablePath() {
		return tablePath;
	}
	public String getTableName() {
		return tableName;
	}

	/*
	public TinyCol getColumn(String colName){
		if(colMap.containsKey(colName))
			return colMap.get(colName);
		else return null;

	}*/

	private TinyDB upperDB;
	private String tableName;
	private String tablePath;
	private static HashMap<String, TinyCol> colMap;
}
