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
		int len = rn.nextInt(10) + 1;
		String str = new String();
		for (int i = 0; i < len; i++) {
			char c = (char)(rn.nextInt(26) + (int)('a'));
			str += c;
		}
		return str;
	}
	
	
//	@Test
//	public void test() throws IOException, ClassNotFoundException {
//		MemTable memT = new MemTable();
//		HashMap<String, String> kvMap = new HashMap<String, String>();
//		int testNUM = 50;
//		for (int i = 0; i < testNUM; i++) {
//			String tmpK = genRandomString();
//			String tmpV = genRandomString();
//			kvMap.put(tmpK, tmpV);
//			memT.put(tmpK, tmpV);
//		}
//		for (String key : kvMap.keySet()) {
//			assertEquals(kvMap.get(key), memT.get(key));
//		}
//
//	}
	
//	@Test
//	public void sstMetaTest() throws IOException, ClassNotFoundException {
//		MemTable memT = new MemTable();
//		SSTable ssT = new SSTable();
//		ssT.initializeCreate();
//		ssT.setName("temp1");
//		ssT.setPath("/tmp/tiny/table1/col1/");
//		HashMap<String, String> kvMap = new HashMap<String, String>();
//		int testNum = 5000;
//		memT.getDumpSST(ssT);
//		for (int i = 0; i < testNum; i++) {
//			String tmpK = genRandomString();
//			String tmpV = genRandomString();
//			kvMap.put(tmpK, tmpV);
//			memT.put(tmpK, tmpV);
//			if (memT.isFull())
//				break;
//		}
//		int oldBlockCounter = ssT.getBlockCounter();
//		ArrayList<MetaBlockEntry> tmpMetaInfo = ssT.getMetaInfo();
//		ArrayList<MetaBlockEntry> oldMetaInfo = new ArrayList<MetaBlockEntry>();
//		for (MetaBlockEntry entry : tmpMetaInfo) {
//			MetaBlockEntry oldEntry = new MetaBlockEntry(entry.startKey, entry.endKey, entry.bufSize, entry.entryNum);
//			oldMetaInfo.add(oldEntry);
//		}
//		ssT.initializeReload();
//		
//		int newBlockCounter = ssT.getBlockCounter();
//		ArrayList<MetaBlockEntry> newMetaInfo = ssT.getMetaInfo();
//		assertEquals(oldBlockCounter, newBlockCounter);
//		assertEquals(oldMetaInfo.size(), newMetaInfo.size());
//		for (int i = 0; i < newBlockCounter; i++) {
//			MetaBlockEntry oe = oldMetaInfo.get(i);
//			MetaBlockEntry ne = oldMetaInfo.get(i);
//			assertTrue(oe.equals(ne));
//		}
//	}
	
	@Test
	public void sstAllDataTest1() throws IOException, ClassNotFoundException {
		MemTable memT = new MemTable();
		SSTable ssT = new SSTable();
		ssT.initializeCreate();
		ssT.setName("temp1");
		ssT.setPath("/tmp/tiny/table1/col1/");
		HashMap<String, String> kvMap = new HashMap<String, String>();
		int testNum = 5000;
		memT.getDumpSST(ssT);
		for (int i = 0; i < testNum; i++) {
			String tmpK = genRandomString();
			String tmpV = genRandomString();
			kvMap.put(tmpK, tmpV);
			memT.put(tmpK, tmpV);
			if (memT.isFull())
				break;
		}
		ssT.initializeReload();
		ArrayList<BlockEntry> dataAr = ssT.readAllData();
		for (BlockEntry entry : dataAr) {
			assertEquals(entry.v, kvMap.get(entry.k));
		}	
	}
	
//	@Test
//	public void testRead() throws ClassNotFoundException, IOException {
//		SSTable ssT = new SSTable();
//		ssT.setName("lv1-0");
//		ssT.setPath("/tmp/tiny/table1/col1/");
//		ssT.initializeReload();	
//		ArrayList<BlockEntry> dataAr = ssT.readAllData();
//		assertTrue(true);
//	}
	
//	@Test
//	public void sstDataTest2() throws IOException, ClassNotFoundException {
//		MemTable memT = new MemTable();
//		SSTable ssT = new SSTable();
//		ssT.initializeCreate();
//		ssT.setName("temp1");
//		ssT.setPath("/tmp/tiny/table1/col1/");
//		HashMap<String, String> kvMap = new HashMap<String, String>();
//		int testNum = 5000;
//		memT.getDumpSST(ssT);
//		for (int i = 0; i < testNum; i++) {
//			String tmpK = genRandomString();
//			String tmpV = genRandomString();
//			kvMap.put(tmpK, tmpV);
//			memT.put(tmpK, tmpV);
//			if (memT.isFull())
//				break;
//		}
//		ssT.initializeReload();
//		for (String key : kvMap.keySet()) {
//			assertEquals(kvMap.get(key), ssT.get(key));
//			assertEquals(kvMap.get(key), ssT.getEntry(key).v);
//		}
//	}

}
