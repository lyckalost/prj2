package org.tinytable.db.SQL;

/**
 * Created by andy on 6/11/17.
 */
import org.tinytable.db.SQL.Grammer.*;
import org.tinytable.db.TinyDB;

import java.io.IOException;

public class Parse {
    public static String parseQuery(String query, TinyDB td) throws IOException, ClassNotFoundException {
        String[] args = query.split("\\s+");
        String beginString = args[0];
        String result = "";
        grammer g = null;
        try {

            g = grammer.valueOf(beginString.toUpperCase());
        } catch (IllegalArgumentException e) {
            g = grammer.NULL;
        }
        switch(g){
            case SELECT:
                result = SELECT.parse(args, td).toString();
                break;
            case INSERT:
                break;
            case DELETE:
                break;
            case UPDATE:
                break;
            case CREATE:
                CREATE c = new CREATE();
                result = c.create(args[1], td).getTableName();
                break;
            default:
                result = "Unknown Command: " + beginString;
                break;
        }

        return result;
    }
}
