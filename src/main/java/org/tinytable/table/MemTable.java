package org.tinytable.table;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.tinytable.db.ObjectSizeFetcher;

public class MemTable {

	public MemTable() {
		// TODO Auto-generated constructor stub
		kvMap = new HashMap<String, BlockEntry>();
		bos = new ByteArrayOutputStream();
		try {
			oos = new ObjectOutputStream(bos);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public MemTable(ColTable colT) throws IOException {
		// TODO Auto-generated constructor stub
		kvMap = new HashMap<String, BlockEntry>();
		bos = new ByteArrayOutputStream();
		try {
			oos = new ObjectOutputStream(bos);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		parentColTable = colT;
		getDumpSST(parentColTable.getFreeSST());
	}
	
	public void getDumpSST(SSTable sst) {
		nextDumpSST = sst;
	}
	
	public String get(String key) {
		if (!kvMap.containsKey(key))
			return null;
		else 
			return kvMap.get(key).v;	
	}
	
	public String put(String key, String value) throws IOException {
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		long timeLong = timestamp.getTime();
		
		BlockEntry entry = new BlockEntry(key, value, timeLong);
		updateSize(key, entry);
		kvMap.put(key, entry);
		if (isFull()) {
			dumpToFile();
		}
		return value;
	}
	
	private void updateSize(String key, BlockEntry entry) throws IOException {
		oos.writeObject(entry);
		estimateSize = bos.size();
		BlockEntry oldEntry = kvMap.get(key);
		if (kvMap.containsKey(key))
			estimateSize -= ObjectSizeFetcher.getObjectSize(oldEntry);
	}
	
	public boolean isFull() {
		return estimateSize >= (MemTableSize - 2 * BLOCKSIZE);
	}
	
	
	private void dumpToFile() throws IOException {
		if (nextDumpSST == null && parentColTable != null)
			getDumpSST(parentColTable.getFreeSST());
		ArrayList<BlockEntry> kvArray = new ArrayList<BlockEntry>();
		convertMapToArray(kvArray);
		nextDumpSST.writeMemTableToSST(kvArray);
		
		clearMemTableNow();
		System.out.println("dumping file!");
	}
	
	private void clearMemTableNow() throws IOException {
		bos = new ByteArrayOutputStream();
		try {
			oos = new ObjectOutputStream(bos);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		kvMap.clear();
		nextDumpSST = null;
	}
	
	private void convertMapToArray(ArrayList<BlockEntry> kvArray) {
		for (BlockEntry entry : kvMap.values())
			kvArray.add(entry);
		Collections.sort(kvArray);
	}
	
	public void close() throws IOException {
		dumpToFile();
	}
	
	private HashMap<String, BlockEntry> kvMap;
	private int BLOCKSIZE = 4 * 1024;
	private int estimateSize = 0;
	private int MemTableSize = 128 * BLOCKSIZE; //size in bytes
	private SSTable nextDumpSST;
	private ColTable parentColTable;
	private ByteArrayOutputStream bos;
	private ObjectOutputStream oos;
}
