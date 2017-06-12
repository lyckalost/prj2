package org.tinytable.table;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;

import org.tinytable.db.ObjectSizeFetcher;

public class SSTable {

	public SSTable() {
		// TODO Auto-generated constructor stub
		metaBlockAr = new ArrayList<MetaBlockEntry>();
		blockCounter = 0;
		sstBLF = new BloomFilter(BLOCKSIZE / 2, 7);
	}
	
	public void setName(String name) {
		sstName = name;
	}
	public void setPath(String path) {
		sstPath = path;
	}
	public String getFullPath() {
		return sstPath + sstName;
	}
	
	public void initializeCreate() throws IOException {
		sstFile = new RandomAccessFile(getFullPath(), "rw");
		sstFile.setLength(BLOCKNUM * BLOCKSIZE);
	}
	
	public void initializeReload() throws IOException, ClassNotFoundException {
		blockCounter = 0;
		metaBlockAr.clear();
		sstFile = new RandomAccessFile(getFullPath(), "rw");
		readHeaderBlock();
		readMetaBlock();
	}
	
	private void readHeaderBlock() throws IOException {
		RandomAccessFile file = sstFile;
		byte[] buf = new byte[32];
		file.read(buf);
		blockCounter = ByteBuffer.wrap(buf).getInt();
		
		file.seek(BLOCKSIZE / 2);
		byte[] blfBuf = new byte[BLOCKSIZE / 2];
		file.read(blfBuf);
		sstBLF.synchrWithByteArray(blfBuf);
	}
	
	private void readMetaBlock() throws IOException, ClassNotFoundException {
		RandomAccessFile file = sstFile;
		file.seek(1 * BLOCKSIZE);
		byte[] buf = new byte[BLOCKSIZE];
		file.read(buf);
		ByteArrayInputStream bis = new ByteArrayInputStream(buf);
		ObjectInputStream ois = new ObjectInputStream(bis);
		for (int i = 0; i < blockCounter; i++) {
			MetaBlockEntry metaEntry = (MetaBlockEntry)ois.readObject();
			metaBlockAr.add(metaEntry);
		}
		bis.close();
		ois.close();
	}
	
	public ArrayList<BlockEntry> readDataBlock(int blockIndex) throws IOException, ClassNotFoundException {
		RandomAccessFile file = sstFile;
		file.seek((blockIndex + 2) * BLOCKSIZE);
		byte[] buf = new byte[BLOCKSIZE];
		file.read(buf);
		
		ByteArrayInputStream bis = new ByteArrayInputStream(buf);
		ObjectInputStream ois = new ObjectInputStream(bis);
		ArrayList<BlockEntry> blockEntryAr = new ArrayList<BlockEntry>();
		int blockEntryNum = metaBlockAr.get(blockIndex).entryNum;
//		System.out.println("reading datablock i: " + blockIndex + " of entry Num: " + blockEntryNum);
		for (int i = 0; i < blockEntryNum; i++) {
			BlockEntry dataEntry = (BlockEntry)ois.readObject();
			blockEntryAr.add(dataEntry);
		}
		bis.close();
		ois.close();
		return blockEntryAr;
	}
	
	
	public ArrayList<BlockEntry> readAllData() throws ClassNotFoundException, IOException {
		ArrayList<BlockEntry> dataAr = new ArrayList<BlockEntry>();
		for (int i = 0; i < blockCounter; i++) {
			ArrayList<BlockEntry> blockDataAr = readDataBlock(i);
			dataAr.addAll(blockDataAr);
		}
		return dataAr;
	}
	
	public LinkedList<BlockEntry> readAllDataToLinkedList() throws ClassNotFoundException, IOException {
		LinkedList<BlockEntry> dataList = new LinkedList<BlockEntry>();
		for (int i = 0; i < blockCounter; i++) {
			ArrayList<BlockEntry> blockDataAr = readDataBlock(i);
			dataList.addAll(blockDataAr);
		}
		return dataList;	
	}
	
	public String get(String key) throws ClassNotFoundException, IOException {
		int blockIndex = locateBlock(key);;
		if (blockIndex == -1) {
			return null;
		}
		ArrayList<BlockEntry> blockDataAr = readDataBlock(blockIndex);
		int entryIndex = binarySearch(blockDataAr, key);
		if (entryIndex != -1)
			return blockDataAr.get(entryIndex).v;
		else
			return null;
	}
	
	public BlockEntry getEntry(String key) throws ClassNotFoundException, IOException {
		int blockIndex = locateBlock(key);;
		if (blockIndex == -1) {
			return null;
		}
		ArrayList<BlockEntry> blockDataAr = readDataBlock(blockIndex);
		int entryIndex = binarySearch(blockDataAr, key);
		if (entryIndex != -1)
			return blockDataAr.get(entryIndex);
		else
			return null;		
	}
	
	private int locateBlock(String key) {
		for (int i = 0; i < blockCounter; i++) {
			MetaBlockEntry metaE = metaBlockAr.get(i);
			if (metaE.startKey.compareTo(key) <= 0 && metaE.endKey.compareTo(key) >= 0)
				return i;
		}
		return -1;
	}
	
	private int binarySearch(ArrayList<BlockEntry> blockDataAr, String key) {
		int left = 0, right = blockDataAr.size() - 1;
		int mid = 0;
		while (left < right) {
			mid = left + (right - left) / 2;
			String tmp = blockDataAr.get(mid).k;
			if (tmp.compareTo(key) == 0) {
				return mid;
			}
			else if (tmp.compareTo(key) < 0) 
				left++;
			else
				right--;
				
		}
		String tmp = blockDataAr.get(left).k;
		if (tmp.compareTo(key) == 0)
			return left;
		return -1;
	}
	
	public RandomAccessFile getSSTFile() {
		return sstFile;
	}
	
	public int getBlockCounter() {
		return blockCounter;
	}
	public ArrayList<MetaBlockEntry> getMetaInfo() {
		return metaBlockAr;
	}
	
	public void writeMemTableToSST(ArrayList<BlockEntry> kvArray) throws IOException {
		for (int i = 0; i < kvArray.size(); i++)
			sstBLF.add(kvArray.get(i));
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		BlockHandler bHandler = new BlockHandler(this);
		int blockStartIndex = 0, blockEndIndex = 0;
		
		for (int i = 0; i < kvArray.size(); i++) {
			int entrySize = ObjectSizeFetcher.getObjectSize(kvArray.get(i));
			if (bos.size() + entrySize > BLOCKSIZE) {
				blockCounter++;
				blockEndIndex = i - 1;
				MetaBlockEntry metaEntry = new MetaBlockEntry(kvArray.get(blockStartIndex).k, 
															  kvArray.get(blockEndIndex).k, 
															  bos.size(),
															  blockEndIndex - blockStartIndex + 1);
				metaBlockAr.add(metaEntry);
				blockStartIndex = blockEndIndex + 1;
				byte[] blockBUF = bos.toByteArray();
				bHandler.writeDataBlock(blockBUF);
				
				bos = new ByteArrayOutputStream();
				oos = new ObjectOutputStream(bos);
				oos.writeObject(kvArray.get(i));
			}
			else {
				oos.writeObject(kvArray.get(i));
			}
		}
		if (blockStartIndex < kvArray.size() && bos.size() > 0) {
			blockCounter++;
			blockEndIndex = kvArray.size() - 1;
			MetaBlockEntry metaEntry = new MetaBlockEntry(kvArray.get(blockStartIndex).k, 
					  									  kvArray.get(blockEndIndex).k, 
					  									  bos.size(),
					  									  blockEndIndex - blockStartIndex + 1);
			metaBlockAr.add(metaEntry);
//			System.out.println("block num: " + metaBlockAr.size() + " last block size: " + bos.size());
			for (int i = blockStartIndex; i <= blockEndIndex; i++)
				oos.writeObject(kvArray.get(i));
			
			byte[] blockBUF = bos.toByteArray();
			bHandler.writeDataBlock(blockBUF);
		}
		bos.close();
		oos.close();
		
		bHandler.writeMetaBlock(metaBlockAr);
		bHandler.writeHeaderBlock(blockCounter);
	}
	
	public boolean recycleLvTable() {
		File delF = new File(this.getFullPath());
		return delF.delete();
	}
	
	public BloomFilter getBloomFilter() {
		return sstBLF;
	}
	
	private String sstName;
	private String sstPath;
	public int BLOCKSIZE = 4 * 1024; // 4K bytes block
	private ArrayList<MetaBlockEntry> metaBlockAr;
	private int BLOCKNUM = 64;
	private int blockCounter;
	private RandomAccessFile sstFile;
	private BloomFilter sstBLF;

}
