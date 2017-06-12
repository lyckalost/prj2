package org.tinytable.db.SQL;

import org.tinytable.db.TinyCol;
import org.tinytable.db.TinyDB;
import org.tinytable.db.TinyTable;
import org.tinytable.db.entry;
import org.tinytable.table.ColTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by andy on 6/11/17.
 */
public class SELECT {
    public static ArrayList<String> parse(String[] args, TinyDB td) throws IOException, ClassNotFoundException {
        int len = args.length;
        HashMap<String, Integer> cmdMap =  new HashMap<>();
        for(int i = 0; i < len; i++){
            cmdMap.put(args[i], i);
        }
        int fromIdx = cmdMap.get("FROM");
        int whereIdx = cmdMap.get("WHERE");

        int equalIdx = cmdMap.get("IS");

        String[] cols = new String[fromIdx - 1];
        for(int j = 1; j < fromIdx; j++)
            cols[j-1] = args[j];
        String[] tables = new String[whereIdx - fromIdx -1];
        for(int j = fromIdx+1; j < whereIdx; j ++)
            tables[j - fromIdx -1] = args[j];
        String key = args[equalIdx -1];
        String value = args[equalIdx +1];

        ArrayList<TinyTable> Ts = new ArrayList<>();
        for(String tableName: tables){
            Ts.add(td.createTable(tableName));
        }


        ArrayList<TinyCol> colT = new ArrayList<>();
        for(TinyTable table: Ts){
            for (String col: cols)
               colT.add(table.createCol(col));
        }

        ArrayList<String> result = new ArrayList<>();

        for(TinyCol col: colT){
             if(col.get(value)!= null)
                 result.add(col.get(value));
        }
        return result;

    }
}
