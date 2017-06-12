package org.tinytable.db.SQL;

import org.tinytable.db.TinyCol;
import org.tinytable.db.TinyDB;
import org.tinytable.db.TinyTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by andy on 6/12/17.
 */
public class INSERT {

    public static String parse(String[] args, TinyDB td) throws IOException, ClassNotFoundException {
        int len = args.length;

        String pair = args[1];
        String[] items = pair.split("\\(,\\)");
        String k = items[0], v = items[1];
        String tableName = args[3];
        String colName = args[4];

        TinyTable tingyTable = td.createTable(tableName);

        TinyCol column = tingyTable.createCol(colName);

        return column.put(k,v);

    }
}
