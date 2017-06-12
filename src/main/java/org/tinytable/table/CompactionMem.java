package org.tinytable.table;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;


import org.tinytable.db.ObjectSizeFetcher;

public class CompactionMem {

	public CompactionMem(Compaction pc) {
		// TODO Auto-generated constructor stub
		parentCom = pc;
		kvMap = new HashMap<String, BlockEntry>();
		bos = new ByteArrayOutputStream();
		try {
			oos = new ObjectOutputStream(bos);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void getDumpSST(SSTable sst) {
		nextDumpSST = sst;
	}
	
	public String put(BlockEntry entry) throws IOException {	
		updateSize(entry.k, entry);
		kvMap.put(entry.k, entry);
		if (isFull()) {
			dumpToFile();
		}
		return entry.v;
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
		if (nextDumpSST == null && parentCom != null)
			getDumpSST(parentCom.getFreeSST());
		ArrayList<BlockEntry> kvArray = new ArrayList<BlockEntry>();
		convertMapToArray(kvArray);
		nextDumpSST.writeMemTableToSST(kvArray);
		
		clearMemTableNow();
//		System.out.println("dumping compaction file!");
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
	

	private Compaction parentCom;
	private HashMap<String, BlockEntry> kvMap;
	private int BLOCKSIZE = 4 * 1024;
	private int estimateSize = 0;
	private int MemTableSize = 64 * BLOCKSIZE; //size in bytes
	private SSTable nextDumpSST;
	private ByteArrayOutputStream bos;
	private ObjectOutputStream oos;
}
