package org.tinytable.table;

import java.util.ArrayList;

public class BlockData {

	public BlockData(String bPath, ArrayList<BlockEntry> blockAr) {
		// TODO Auto-generated constructor stub
		cachedDataList = new ArrayList<BlockEntry>();
		blockPath = bPath;
		for (int i = 0; i < blockAr.size(); i++)
			cachedDataList.add(blockAr.get(i));
	}
	public ArrayList<BlockEntry> cachedDataList;
	public String blockPath;
}
