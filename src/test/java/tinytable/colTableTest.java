package tinytable;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Random;

import org.junit.Test;
import org.tinytable.table.ColTable;
import org.tinytable.table.MemTable;
import org.tinytable.table.SSTable;

public class colTableTest {

	public String genRandomString() {
		Random rn = new Random();
		int len = rn.nextInt(8) + 1;
		String str = new String();
		for (int i = 0; i < len; i++) {
			char c = (char)(rn.nextInt(26) + (int)('a'));
			str += c;
		}
		return str;
	}

	public void clearDir(String dirName) {
		File dir = new File(dirName);
		for (File item : dir.listFiles()) {
			if (!item.isDirectory())
				item.delete();
		}
	}
	
	

	
	@Test
	public void testSmallIO() throws IOException, ClassNotFoundException {
		clearDir("/tmp/tiny/table1/col1");
		ColTable colT = new ColTable("col1", "/tmp/tiny/table1/");
		HashMap<String, String> kvMap = new HashMap<String, String>();
		int testNum = 10000;
		Instant start = Instant.now();
		for (int i = 0; i < testNum; i++) {
			String tmpK = genRandomString();
			String tmpV = genRandomString();
			colT.put(tmpK, tmpV);
			kvMap.put(tmpK, tmpV);
		}
		colT.close();
		Instant end = Instant.now();
		long delta = Duration.between(start, end).toMillis();
		System.out.println("Small IO 10000 write time: " + delta);
		
		start = Instant.now();
		for (String key : kvMap.keySet()) {
			assertEquals(kvMap.get(key), colT.get(key));
		}
		end = Instant.now();
		delta = Duration.between(start, end).toMillis();
		System.out.println("Small IO 10000 read time: " + delta);
		System.out.println("");
	}	

	
	@Test
	public void testSmallIO1() throws IOException, ClassNotFoundException {
		clearDir("/tmp/tiny/table1/col1");
		ColTable colT = new ColTable("col1", "/tmp/tiny/table1/");
		HashMap<String, String> kvMap = new HashMap<String, String>();
		int testNum = 20000;
		Instant start = Instant.now();
		for (int i = 0; i < testNum; i++) {
			String tmpK = genRandomString();
			String tmpV = genRandomString();
			colT.put(tmpK, tmpV);
			kvMap.put(tmpK, tmpV);
		}
		colT.close();
		Instant end = Instant.now();
		long delta = Duration.between(start, end).toMillis();
		System.out.println("Small IO 20000 write time: " + delta);
		
		start = Instant.now();
		for (String key : kvMap.keySet()) {
			assertEquals(kvMap.get(key), colT.get(key));
		}
		end = Instant.now();
		delta = Duration.between(start, end).toMillis();
		System.out.println("Small IO 20000 read time: " + delta);
		System.out.println("");
	}
	
	@Test
	public void testIOWithOutCompaction() throws IOException, ClassNotFoundException {
		clearDir("/tmp/tiny/table1/col1");
		ColTable colT = new ColTable("col1", "/tmp/tiny/table1/");
		HashMap<String, String> kvMap = new HashMap<String, String>();
		int testNum = 40000;
		Instant start = Instant.now();
		for (int i = 0; i < testNum; i++) {
			String tmpK = genRandomString();
			String tmpV = genRandomString();
			colT.put(tmpK, tmpV);
			kvMap.put(tmpK, tmpV);
		}
		colT.close();
		Instant end = Instant.now();
		long delta = Duration.between(start, end).toMillis();
		System.out.println("IO 40000 without compaction write time: " + delta);
		
		start = Instant.now();
		for (String key : kvMap.keySet()) {
			assertEquals(kvMap.get(key), colT.get(key));
		}
		end = Instant.now();
		delta = Duration.between(start, end).toMillis();
		System.out.println("IO 40000 without compaction read time: " + delta);
		System.out.println("");
	}
	
	@Test
	public void testLargerIO() throws IOException, ClassNotFoundException {
		clearDir("/tmp/tiny/table1/col1");
		ColTable colT = new ColTable("col1", "/tmp/tiny/table1/");
		HashMap<String, String> kvMap = new HashMap<String, String>();
		int testNum = 80000;
		Instant start = Instant.now();
		for (int i = 0; i < testNum; i++) {
			String tmpK = genRandomString();
			String tmpV = genRandomString();
			colT.put(tmpK, tmpV);
			kvMap.put(tmpK, tmpV);
		}
		colT.close();
		Instant end = Instant.now();
		long delta = Duration.between(start, end).toMillis();
		System.out.println("Large IO 80000  write time: " + delta);
		
		start = Instant.now();
		for (String key : kvMap.keySet()) {
			assertEquals(kvMap.get(key), colT.get(key));
		}
		end = Instant.now();
		delta = Duration.between(start, end).toMillis();
		System.out.println("Large IO 80000 read time: " + delta);
		System.out.println("");
	}
	
	
	@Test
	public void testIOWithCompaction() throws IOException, ClassNotFoundException {
		clearDir("/tmp/tiny/table1/col1");
		ColTable colT = new ColTable("col1", "/tmp/tiny/table1/");
		HashMap<String, String> kvMap = new HashMap<String, String>();
		int testNum = 100000;
		Instant start = Instant.now();
		for (int i = 0; i < testNum; i++) {
			String tmpK = genRandomString();
			String tmpV = genRandomString();
			colT.put(tmpK, tmpV);
			kvMap.put(tmpK, tmpV);
		}
		colT.close();
		Instant end = Instant.now();
		long delta = Duration.between(start, end).toMillis();
		System.out.println("Large IO 100000 with compaction write time: " + delta);
		
		start = Instant.now();
		for (String key : kvMap.keySet()) {
			assertEquals(kvMap.get(key), colT.get(key));
		}
		end = Instant.now();
		delta = Duration.between(start, end).toMillis();
		System.out.println("Large IO 100000 with compaction read time: " + delta);
		System.out.println("");
	}
	

}
