package tinytable;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.junit.Test;
import org.tinytable.table.BlockEntry;
import org.tinytable.table.MemTable;
import org.tinytable.table.MetaBlockEntry;
import org.tinytable.table.SSTable;

public class TableTest {

	public String genRandomString() {
		Random rn = new Random();
		int len = rn.nextInt(6) + 1;
		String str = new String();
		for (int i = 0; i < len; i++) {
			char c = (char)(rn.nextInt(26) + (int)('a'));
			str += c;
		}
		return str;
	}
	
	
	//memTable test
	@Test
	public void memTest() throws IOException, ClassNotFoundException {
		MemTable memT = new MemTable();
		HashMap<String, String> kvMap = new HashMap<String, String>();
		int testNUM = 50;
		for (int i = 0; i < testNUM; i++) {
			String tmpK = genRandomString();
			String tmpV = genRandomString();
			kvMap.put(tmpK, tmpV);
			memT.put(tmpK, tmpV);
		}
		for (String key : kvMap.keySet()) {
			assertEquals(kvMap.get(key), memT.get(key));
		}

	}
	
	@Test
	public void sstMetaTest() throws IOException, ClassNotFoundException {
		MemTable memT = new MemTable();
		SSTable ssT = new SSTable();
		ssT.setName("temp0");
		ssT.setPath("/tmp/tiny/testData/");
		ssT.initializeCreate();
		HashMap<String, String> kvMap = new HashMap<String, String>();
		int testNum = 10000;
		memT.getDumpSST(ssT);
		for (int i = 0; i < testNum; i++) {
			String tmpK = genRandomString();
			String tmpV = genRandomString();
			kvMap.put(tmpK, tmpV);
			memT.put(tmpK, tmpV);
			if (memT.isFull())
				break;
		}

		int oldBlockCounter = ssT.getBlockCounter();
		ArrayList<MetaBlockEntry> tmpMetaInfo = ssT.getMetaInfo();
		ArrayList<MetaBlockEntry> oldMetaInfo = new ArrayList<MetaBlockEntry>();
		for (MetaBlockEntry entry : tmpMetaInfo) {
			MetaBlockEntry oldEntry = new MetaBlockEntry(entry.startKey, entry.endKey, entry.bufSize, entry.entryNum);
			oldMetaInfo.add(oldEntry);
		}
		byte[] oldBlfAr = ssT.getBloomFilter().getBloomFilterArray();
		//reloading ssT
		ssT.initializeReload();
		
		byte[] newBlfAr = ssT.getBloomFilter().getBloomFilterArray();
		
		// test bloom filter write and load successfully
		assertArrayEquals(oldBlfAr, newBlfAr);
		assertEquals(oldBlfAr.length, newBlfAr.length);
		
		int newBlockCounter = ssT.getBlockCounter();
		ArrayList<MetaBlockEntry> newMetaInfo = ssT.getMetaInfo();
		// test block counter in the header
		assertEquals(oldBlockCounter, newBlockCounter);
		// test meta data write and load 
		assertEquals(oldMetaInfo.size(), newMetaInfo.size());
		for (int i = 0; i < newBlockCounter; i++) {
			MetaBlockEntry oe = oldMetaInfo.get(i);
			MetaBlockEntry ne = oldMetaInfo.get(i);
			assertTrue(oe.equals(ne));
		}
	}
	
	
	//test sstable reload and read all data
	@Test
	public void sstAllDataTest1() throws IOException, ClassNotFoundException {
		MemTable memT = new MemTable();
		SSTable ssT = new SSTable();
		ssT.setName("temp1");
		ssT.setPath("/tmp/tiny/testData/");
		ssT.initializeCreate();
		HashMap<String, String> kvMap = new HashMap<String, String>();
		int testNum = 10000;
		memT.getDumpSST(ssT);
		for (int i = 0; i < testNum; i++) {
			String tmpK = genRandomString();
			String tmpV = genRandomString();
			kvMap.put(tmpK, tmpV);
			memT.put(tmpK, tmpV);
			if (memT.isFull())
				break;
		}

		SSTable ssTNew = new SSTable();
		ssTNew.setName("temp1");
		ssTNew.setPath("/tmp/tiny/testData/");
		ssTNew.initializeReload();
		ArrayList<BlockEntry> dataAr = ssTNew.readAllData();

		for (BlockEntry entry : dataAr) {
			assertEquals(entry.v, kvMap.get(entry.k));
		}	
	}
	
	//test sstable reload get and put
	@Test
	public void sstDataTest2() throws IOException, ClassNotFoundException {
		MemTable memT = new MemTable();
		SSTable ssT = new SSTable();
		ssT.setName("temp2");
		ssT.setPath("/tmp/tiny/testData/");
		ssT.initializeCreate();
		
		HashMap<String, String> kvMap = new HashMap<String, String>();
		int testNum = 10000;
		memT.getDumpSST(ssT);
		for (int i = 0; i < testNum; i++) {
			String tmpK = genRandomString();
			String tmpV = genRandomString();
			kvMap.put(tmpK, tmpV);
			memT.put(tmpK, tmpV);
			if (memT.isFull())
				break;
		}
		SSTable ssTNew = new SSTable();
		ssTNew.setName("temp2");
		ssTNew.setPath("/tmp/tiny/testData/");
		ssTNew.initializeReload();
		
		for (String key : kvMap.keySet()) {
			assertEquals(kvMap.get(key), ssTNew.get(key));
//			assertEquals(kvMap.get(key), ssTNew.getEntry(key).v);
		}
	}

}
