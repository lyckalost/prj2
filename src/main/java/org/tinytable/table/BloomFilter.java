package org.tinytable.table;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by andy on 6/12/17.
 */
public class BloomFilter {

    private byte[] set;
    private int keySize, setSize, size;
    private MessageDigest md;

    public BloomFilter(int capacity, int k){
        setSize = capacity;
        set = new byte[setSize];
        keySize = k;
        size = 0;
        try {
            md = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Hash not found");
        }
    }
    public void clear() {
        set = new byte[setSize];
        size = 0;
        try
        {
            md = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IllegalArgumentException("Hash not found");
        }
    }

    public int getSize()
    {
        return size;
    }

    private int getHash(int i) {
        md.reset();
        byte[] bytes = ByteBuffer.allocate(4).putInt(i).array();
        md.update(bytes, 0, bytes.length);
        return Math.abs(new BigInteger(1, md.digest()).intValue()) % (set.length - 1);
    }

    private int[] getSetArray(BlockEntry entry)
    {
        int[] tmpset = new int[keySize];
        tmpset[0] = getHash(entry.hashCode());
        for (int i = 1; i < keySize; i++)
            tmpset[i] = (getHash(tmpset[i - 1]));
        return tmpset;
    }

    public void add(BlockEntry entry) {
        int[] tmpset = getSetArray(entry);
        for (int i : tmpset)
            set[i] = 1;
        size++;
    }


    public boolean contains(BlockEntry entry)
    {
        int[] tmpset = getSetArray(entry);
        for (int i : tmpset)
            if (set[i] != 1)
                return false;
        return true;
    }




}
