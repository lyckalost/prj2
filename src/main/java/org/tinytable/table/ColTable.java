package org.tinytable.table;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ColTable {

	public ColTable(String na, String pa) throws IOException, ClassNotFoundException {
		// TODO Auto-generated constructor stub
		name = na;
		path = pa;
		lvOneOpenSstList = new LinkedList<SSTable>();
		lvTwoOpenSstList = new LinkedList<SSTable>();
		lvOneFreeSstList = new LinkedList<Integer>();
		lvTwoFreeSstList = new LinkedList<Integer>();
		colBlockCache = new BlockCache(256);
		
		String abPath = path + name;
		File dir = new File(abPath);
		if (dir.exists()) {
			initializeReload(dir);
		}
		else {
			initializeCreate();
		}
		memT = new MemTable(this);
	}
	
	
	private void initializeCreate() {
		for (int i = 0; i < lvOneNum; i++)
			lvOneFreeSstList.add(i);
		for (int i = 0; i < lvTwoNum; i++)
			lvTwoFreeSstList.add(i);
	}
	
	private void initializeReload(File dir) {
		
		String[] sstNames = dir.list();
		boolean[] lv1Flag = new boolean[lvOneNum];
		boolean[] lv2Flag = new boolean[lvTwoNum];
		for (int i = 0; i < lvOneNum; i++)
			lv1Flag[i] = true;
		for (int i = 0; i < lvTwoNum; i++)
			lv2Flag[i] = true;
		for (String str : sstNames) {
			//load existing sst
			String[] parts = str.split("-");
			if (parts == null)
				continue;
			
			SSTable sst = new SSTable();
			sst.setName(str);
			sst.setPath(path + name + "/");
			sst.setBelongColT(this);
			int sstNum = Integer.parseInt(parts[1]);
			if (parts[0].equals("lv1")) {
				lvOneOpenSstList.add(sst);
				lv1Flag[sstNum] = false;
			}
			else if (parts[0].equals("lv2")) {
				lvTwoOpenSstList.add(sst);
				lv2Flag[sstNum] = false;
			}
			else {;}
		}
		for (int i = 0; i < lvOneNum; i++) {
			if (lv1Flag[i])
				lvOneFreeSstList.add(i);
		}
		for (int i = 0; i < lvTwoNum; i++) {
			if (lv2Flag[i])
				lvTwoFreeSstList.add(i);
		}
	}
	
	
	public SSTable getFreeSST() throws IOException, ClassNotFoundException {
		// if empty do minor compaction
		if (lvOneFreeSstList.isEmpty()) {
			System.out.println("start compaction!");
			Compaction lvOneCom = new Compaction(this);
			lvOneCom.mergeData();
			for (SSTable entry : lvOneOpenSstList) {
				entry.recycleLvTable();
			}
			lvOneOpenSstList.clear();
			for (int i = 0; i < lvOneNum; i++)
				lvOneFreeSstList.add(i);
			System.out.println("lv1 size after compaction: " + lvOneFreeSstList.size());
			return getFreeSST();
		}
		else {
			int sstNum = lvOneFreeSstList.poll();
			SSTable sst = new SSTable();
			sst.setPath(path + name + "/");
			sst.setName("lv1-" + sstNum);
			sst.setBelongColT(this);
			sst.initializeCreate();
			lvOneOpenSstList.add(sst);
			return sst;
		}
	}
	
	public SSTable getLvTwoFreeSST() throws IOException {
		int sstNum = lvTwoFreeSstList.poll();
		SSTable sst = new SSTable();
		sst.setPath(path + name + "/");
		sst.setName("lv2-" + sstNum);
		sst.setBelongColT(this);
		sst.initializeCreate();
		lvTwoOpenSstList.add(sst);
		return sst;
	}
	
	public String put(String k, String v) throws IOException, ClassNotFoundException {
		writeLock.lock();
		String res = memT.put(k, v);
		writeLock.unlock();
		return res;
	}
	
	public String get(String k) throws ClassNotFoundException, IOException {
		readLock.lock();
		String strM = memT.get(k);
		if (strM != null) {
			readLock.unlock();
			return strM;
		}
		String strLvOne = combineLvRes(k, lvOneOpenSstList);
		if (strLvOne != null) {
			readLock.unlock();
			return strLvOne;
		}
		readLock.unlock();
		return combineLvRes(k, lvTwoOpenSstList);
	}
	
	public String combineLvRes(String k, LinkedList<SSTable> sstList) throws ClassNotFoundException, IOException {
		if (sstList.isEmpty())
			return null;
		BlockEntry tmpEntry = sstList.getFirst().getEntry(k);
		for (SSTable sst : sstList) {
			BlockEntry bn = sst.getEntry(k);
			if (bn == null)
				continue;
			if (tmpEntry == null) {
				tmpEntry = bn;
				continue;
			}
			if (bn.timeT > tmpEntry.timeT)
				tmpEntry = bn;
		}
		if (tmpEntry == null)
			return null;
		else
			return tmpEntry.v;
	}
	
	public LinkedList<SSTable> getCompactionList() {
		return lvOneOpenSstList;
	}
	
	public void close() throws IOException, ClassNotFoundException {
		memT.close();
	}
	
	public void putIntoBlockCache(String bPath, ArrayList<BlockEntry> blockEntryAr) {
		
		BlockData bData = new BlockData(bPath, blockEntryAr);
		colBlockCache.putBlockData(bData);
	}
	
	public ArrayList<BlockEntry> getCachedData(String bPath) {
		BlockData bData = colBlockCache.getBlockData(bPath);
		if (bData == null)
			return null;
		else
			return bData.cachedDataList;
	}
	
	private String name;
	private String path;
	private LinkedList<SSTable> lvOneOpenSstList;
	private LinkedList<SSTable> lvTwoOpenSstList;
	public LinkedList<Integer> lvOneFreeSstList;
	private LinkedList<Integer> lvTwoFreeSstList;
	private int lvOneNum = 10;
	private int lvTwoNum = 100;
	private MemTable memT;
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock readLock = readWriteLock.readLock();
	private final Lock writeLock = readWriteLock.writeLock();
	private BlockCache colBlockCache;
	

}
