package org.tinytable.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

public class Compaction {

	public Compaction(ColTable colT) {
		// TODO Auto-generated constructor stub
		this.colT = colT;
		this.memCom = new CompactionMem(this);
	}
	
	
	public void mergeData() throws ClassNotFoundException, IOException {
		LinkedList<SSTable> oldList = colT.getCompactionList();
		int sstNum = oldList.size();
		HashMap<String, BlockEntry> keyToEntryMap = new HashMap<String, BlockEntry>();
		for (int i = 0; i < sstNum; i++) {
			ArrayList<BlockEntry> dataList = oldList.get(i).readAllData();
			for (BlockEntry entry : dataList) {
				if (!keyToEntryMap.containsKey(entry.k))
					keyToEntryMap.put(entry.k, entry);
				else {
					BlockEntry existedEntry = keyToEntryMap.get(entry.k);
					if (entry.timeT > existedEntry.timeT)
						keyToEntryMap.put(entry.k, entry);
				}
			}
		}
		for (String key : keyToEntryMap.keySet()) {
			memCom.put(keyToEntryMap.get(key));
		}
		memCom.close();
	}
	
	public SSTable getFreeSST() throws IOException {
		return colT.getLvTwoFreeSST();
	}
	
	

	//only compacts to second level
	private ColTable colT;
	private CompactionMem memCom;
}
