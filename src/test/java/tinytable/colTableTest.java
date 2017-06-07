package tinytable;

import static org.junit.Assert.*;

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
		int len = rn.nextInt(10) + 1;
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
		int testNum = 5000;
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

}
