package tinytable;

import org.junit.Test;
import org.tinytable.db.SQL.Parse;
import org.tinytable.db.TinyCol;
import org.tinytable.db.TinyDB;
import org.tinytable.db.TinyTable;
import org.tinytable.table.ColTable;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Created by andy on 6/11/17.
 */
public class QueryTest {
    @Test
    public void test1() throws IOException, ClassNotFoundException {

        TinyDB tinyDB = new TinyDB("db1","/tmp/tiny/");

        String tmpK = "999";
        String tmpV = "Sam";
        TinyTable tinyTable = tinyDB.createTable("SID");
        TinyCol tinyCol  = tinyTable.createCol("Name");

        tinyCol.colTable.put(tmpK,tmpV);

        String SQL = "SELECT Name FROM db1 WHERE SID IS 999";
        Parse p = new Parse();
        assertEquals(tinyCol.get(tmpK), p.parseQuery(SQL, tinyDB));
    }
}
