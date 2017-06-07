package org.tinytable.db;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class ObjectSizeFetcher {

    public static int getObjectSize(Object o) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(o);
        return bos.size();
    }
}
