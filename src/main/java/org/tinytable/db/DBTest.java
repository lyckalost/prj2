package org.tinytable.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

import org.xerial.snappy.Snappy;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.lang.instrument.Instrumentation;

public class DBTest {

/*
	public static void main(String[] args) throws UnsupportedEncodingException, IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
//		TinyDB db1 = new TinyDB("tiny");
//		TinyTable tb1 = db1.createTable("table1");
//		TinyTable tb2 = db1.createTable("table2");
//		TinyCol col1 = tb1.createCol("col1");
//		TinyCol col2 = tb1.createCol("col2");
//		TinyCol col3 = tb1.createCol("col3");
//		String testStr = "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
//			     + "Snappy, a fast compresser/decompresser.";
//		 byte[] compressed = Snappy.compress(testStr.getBytes("UTF-8"));
//		 byte[] uncompressed = Snappy.uncompress(compressed);
//		 System.out.println(testStr.getBytes().length);
//		 System.out.println(compressed.length);
//		 String res = new String(uncompressed, "utf-8");
//		 System.out.println(res);
//		 
//		 Timestamp timestamp = new Timestamp(System.currentTimeMillis());
//		 System.out.println(timestamp);
//		 long timeLong = timestamp.getTime();
//		 System.out.println(timeLong);
		ArrayList<String> al = new ArrayList<String>();
		ArrayList<entry> el = new ArrayList<entry>();
//		System.out.println(ObjectSizeFetcher.getObjectSize(el));

		el.add(new entry("abc", "12asdfadsf3"));
//		System.out.println(ObjectSizeFetcher.getObjectSize(new entry("abc", "12asdfadsf3")));
//		System.out.println("###" + ObjectSizeFetcher.getObjectSize(el));
		
		el.add(new entry("ipnijiolij", "123jdfdsf"));
//		System.out.println(ObjectSizeFetcher.getObjectSize(new entry("ipnijiolij", "123jdfdsf")));
//		System.out.println("###" + ObjectSizeFetcher.getObjectSize(el));
		
		el.add(new entry("deadff", "45123asdfsd6"));
//		System.out.println(ObjectSizeFetcher.getObjectSize(new entry("deadff", "45123asdfsd6")));
//		System.out.println("###" + ObjectSizeFetcher.getObjectSize(el));
		
		el.add(new entry("ipnlij", "123jdfdsf"));
//		System.out.println(ObjectSizeFetcher.getObjectSize(new entry("ipnlij", "123jdfdsf")));
//		System.out.println("###" + ObjectSizeFetcher.getObjectSize(el));
		
		el.add(new entry("ip123123olij", "123jdfdsf"));
//		System.out.println(ObjectSizeFetcher.getObjectSize(new entry("ip123123olij", "123jdfdsf")));
//		System.out.println("###" + ObjectSizeFetcher.getObjectSize(el));
		
	
		al.add("abc:" + "123"); 
		al.add("def:" + "456");
		al.add("efg:" + "789");
		al.add("adf:" + "78sad9");
		al.add("efdafg:" + "7szz89");
		al.add("efdsfsdg:" + "78syyd9");
		al.add("efasdasdg:" + "78asd9abcaaaaa");
		

		byte[] buffer;
//		FileOutputStream fos = new FileOutputStream("/tmp/test0");
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(al);
		System.out.println(bos.size());
		buffer = bos.toByteArray();
		
		bos.close();
		oos.close();
		
		
		
		System.out.println(buffer.length);
		ByteArrayInputStream bis = new ByteArrayInputStream(buffer);
		ObjectInputStream ois = new ObjectInputStream(bis);
		al.clear();
		al = (ArrayList<String>) ois.readObject();
		for (String str : al)
			System.out.println(str);
		
//		el = (ArrayList<entry>) ois.readObject();
//		for (entry str : el)
//			System.out.println(str.k + "+" + str.v);
			

	}
*/
	
/*	
   public static void main(String[] args) {
	      String s = "Hello world!";
	      int i = 897648764;
	      entry entry1 = new entry("abc", "123");
	      entry entry2 = new entry("def", "456");
	      
	      try {

	    	 byte[] buffer;
	         // create a new file with an ObjectOutputStream
	         ByteArrayOutputStream out = new ByteArrayOutputStream();
	         ObjectOutputStream oout = new ObjectOutputStream(out);

	         // write something in the file
	         System.out.println(out.size());
	     
	         oout.writeObject(entry1);
	         System.out.println(out.size());
	         System.out.println("###" + ObjectSizeFetcher.getObjectSize(entry1));
	         byte[] buf1 = out.toByteArray();
	         out = new ByteArrayOutputStream();
	         oout = new ObjectOutputStream(out);
	         
	         oout.writeObject(entry2);
	         System.out.println(out.size());
	         System.out.println("###" + ObjectSizeFetcher.getObjectSize(entry2));
	         byte[] buf2 = out.toByteArray();
	         out = new ByteArrayOutputStream();
	         oout = new ObjectOutputStream(out);
	         
	         buffer = out.toByteArray();
	         System.out.println("buf1 size: " + buf1.length);
	         System.out.println("buf2 size: " + buf2.length);
	         System.out.println("buffer size: " + buffer.length);

	         
	      
	 
	         System.out.println(buffer.length);
	         ByteArrayInputStream bis = new ByteArrayInputStream(buf2);
	         // close the stream
	         oout.close();

	         // create an ObjectInputStream for the file we created before
	         ObjectInputStream ois = new ObjectInputStream(bis);

	         // read and print what we wrote before
	         entry e1 = (entry)ois.readObject();
//	         entry e2 = (entry)ois.readObject();
	         System.out.println(e1.k + ":" + e1.v);
//	         System.out.println(e2.k + ":" + e2.v);
	       
	         
	      } catch (Exception ex) {
	         ex.printStackTrace();
	      }

	   }	
	   */
	/*
	public static void main(String[] args) throws IOException {
		RandomAccessFile file = new RandomAccessFile("/tmp/test0", "rw");
		byte[] buf = "abcdefg".getBytes();
		System.out.println(buf.length);
		file.write(buf);
		file.close();
		
		buf = "xyz".getBytes();
		System.out.println(buf.length);
		file = new RandomAccessFile("/tmp/test0", "rw");
		file.setLength(200);
		file.seek(16);
		file.write(buf);
//		file.write(buf, 2, 2);
		file.close();
		
		buf = new byte[3];
		file = new RandomAccessFile("/tmp/test0", "rw");
		file.seek(16);
		file.read(buf);
		String tmp = new String(buf);
		System.out.println(tmp + "##");
		file.close();
		
	}
	*/
	
	public static void main(String[] args) {
		File dir = new File("/tmp/tiny/table1/col1");
		for (String str : dir.list())
			System.out.println(str);
	}

}
