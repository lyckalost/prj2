package tinytable;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.junit.Test;
import org.tinytable.table.ColTable;
import org.tinytable.table.MemTable;
import org.tinytable.table.SSTable;

public class colTableTest {

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
	
	@Test
	public void test1() throws IOException, ClassNotFoundException {
		ColTable colT = new ColTable("col1", "/tmp/tiny/table1/");
		HashMap<String, String> kvMap = new HashMap<String, String>();
		int testNum = 100000;
		for (int i = 0; i < testNum; i++) {
			String tmpK = genRandomString();
			String tmpV = genRandomString();
			colT.put(tmpK, tmpV);
			kvMap.put(tmpK, tmpV);
		}
		colT.close();
		for (String key : kvMap.keySet()) {
			assertEquals(kvMap.get(key), colT.get(key));
		}
	}
	
	

//	class TestThread extends Thread {
//		ColTable colT;
//		HashMap<String, String> kvMap;
//		public TestThread(ColTable colT, HashMap<String, String> kvMap) {
//			this.colT = colT;
//			this.kvMap = kvMap;
//		}
//		
//		public void run() {
//			for (String str : kvMap.keySet()) {
//				try {
//					colT.put(str, kvMap.get(str));
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//		}
//	}
	
	
	
	
//	@Test
//	public void testCurr() throws IOException, ClassNotFoundException, InterruptedException {
//		ColTable colT = new ColTable("col1", "/tmp/tiny/table1/");
//		HashMap<String, String> kvMap1 = new HashMap<String, String>();
//		HashMap<String, String> kvMap2 = new HashMap<String, String>();
//		int testNum = 20000;
//		for (int i = 0; i < testNum; i++) {
//			String tmpK = genRandomString();
//			String tmpV1 = genRandomString();
//			String tmpV2 = genRandomString();
//			kvMap1.put(tmpK, tmpV1);
//			kvMap2.put(tmpK, tmpV2);
//		}
//		TestThread t1 = new TestThread(colT, kvMap1);
//		TestThread t2 = new TestThread(colT, kvMap2);
//		t1.start(); 
//		t1.join();
//		t2.start();
//		t2.join();
//		for (String key : kvMap2.keySet()) {
//			assertEquals(kvMap2.get(key), colT.get(key));
//		}
//	}

}
