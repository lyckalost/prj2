package org.tinytable.db.SQL;

import org.tinytable.db.TinyDB;
import org.tinytable.db.TinyTable;
import org.tinytable.table.BlockEntry;
import org.tinytable.table.MemTable;
import org.tinytable.table.SSTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Created by andy on 6/11/17.
 */
public class CREATE {
    public static TinyTable create(String name, TinyDB td) throws IOException, ClassNotFoundException{
        TinyTable table = null;
    try {
        table = td.createTable(name);
    }catch (NullPointerException | IllegalArgumentException e) {
        throw new NullPointerException();
    }
        return table;
    }
}
