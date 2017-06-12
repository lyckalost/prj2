package org.tinytable.table;

import java.util.HashMap;
import java.util.LinkedList;

public class BlockCache {

	public BlockCache(int cachedBlocksNum) {
		// TODO Auto-generated constructor stub
		cachedBlockList = new LinkedList<BlockData>();
		pathToBlockDataMap = new HashMap<String, BlockData>();
		capacity = cachedBlocksNum;
	}
	
	public void putBlockData(BlockData dataForCache) {
		String bPath = dataForCache.blockPath;
		if (cachedBlockList.size() < capacity) {
			cachedBlockList.addLast(dataForCache);
			pathToBlockDataMap.put(bPath, dataForCache);
		}
		else {
			BlockData firstData = cachedBlockList.removeFirst();
			pathToBlockDataMap.remove(firstData.blockPath);
			cachedBlockList.addLast(dataForCache);
			pathToBlockDataMap.put(bPath, dataForCache);
		}
	}
	
	public BlockData getBlockData(String blockPath) {
		if (!pathToBlockDataMap.containsKey(blockPath))
			return null;
		return pathToBlockDataMap.get(blockPath);
	}
	
	private LinkedList<BlockData> cachedBlockList;
	private HashMap<String, BlockData> pathToBlockDataMap;
	private int capacity;

}
