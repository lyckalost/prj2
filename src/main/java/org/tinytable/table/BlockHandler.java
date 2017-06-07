package org.tinytable.table;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class BlockHandler {

	public BlockHandler(SSTable sst) {
		// TODO Auto-generated constructor stub
		belongSST = sst;
		sstFullPath = belongSST.getFullPath();
		BLOCKSIZE = belongSST.BLOCKSIZE;
	}

	public void writeDataBlock(byte[] buf) throws IOException {
		RandomAccessFile file = belongSST.getSSTFile();
		file.seek(dataBlockOffset * BLOCKSIZE);
		file.write(buf);
		dataBlockOffset++;
	}
	
	public void writeMetaBlock(ArrayList<MetaBlockEntry> metaBlockAr) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		
		for (MetaBlockEntry entry : metaBlockAr) {
			oos.writeObject(entry);
		}
		
		byte[] buf = bos.toByteArray();
		RandomAccessFile file = belongSST.getSSTFile();
		file.seek(1 * BLOCKSIZE);
		file.write(buf);
	}
	
	public void writeHeaderBlock(int blockCounter) throws IOException {
		RandomAccessFile file = belongSST.getSSTFile();
		file.seek(0);
		byte[] buf = ByteBuffer.allocate(4).putInt(blockCounter).array();
		file.write(buf);
	}
	
	public SSTable belongSST;
	public String sstFullPath;
	private int dataBlockOffset = 2;
	private int BLOCKSIZE = 4 * 1024;
}
