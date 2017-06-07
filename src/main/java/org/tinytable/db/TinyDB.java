package org.tinytable.db;

import java.io.File;
import java.util.HashMap;

public class TinyDB {
	public TinyDB(String path, String name) {
		dbName = name;
		dbPath = path + dbName;
		tableMap = new HashMap<String, TinyTable>();
		initializeDB();
	}
	
	public TinyDB(String name) {
		dbName = name;
		dbPath = "/tmp/" + dbName;
		tableMap = new HashMap<String, TinyTable>();
		initializeDB();
	}
	
	public void initializeDB() {
		File dir = new File(dbPath);
		if (dir.exists()) {
			loadDBInfo(dir);
		}
		else {
			boolean successful = dir.mkdir();
			if (successful) {
				System.out.println("db " + dbPath + " created successfully");
			}
			else {
				System.out.println("db " + dbPath + " create failed!");
			}
		}		
	}
	
	private void loadDBInfo(File dir) {
		String[] tableList = dir.list();
		for (int i = 0; i < tableList.length; i++) {
			tableMap.put(tableList[i], createTable(tableList[i]));
		}
	}
	
	public TinyTable createTable(String tableName) {
		if (tableMap.containsKey(tableName))
			return tableMap.get(tableName);
		TinyTable newTable = new TinyTable(this, tableName);
		tableMap.put(tableName, newTable);
		return newTable;
	}
	
	public String getDBPath() {
		return dbPath;
	}
	
	private String dbName;
	private String dbPath;
	//4MB memtableSize
	private final int memtableSize = (4 << 20);
	private final int sstableSize = (4 << 20);
	private HashMap<String, TinyTable> tableMap;
	
}
