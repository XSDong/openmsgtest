package test_write_read;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.Map;

public class Test_write_read {

	static int index =0;
	static RandomAccessFile raf ;
	static FileChannel inFileChannel;
	static ByteArrayOutputStream bytesarray;
	static byte[] arr;
	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException {

		//raf = new RandomAccessFile("bytes_single","rw");
		raf = new RandomAccessFile("bytes_multimap","rw");
		inFileChannel = raf.getChannel();  
		System.out.println("file size is: " + raf.length() +", size is: "+inFileChannel.size());
		raf.setLength(1000*Value.numOfArraysSerial );
		System.out.println("file size is: " + raf.length() +", size is: "+inFileChannel.size());
		
		bytesarray = new ByteArrayOutputStream();
		for(int i=0;i<1000;i++){
			bytesarray.write('7');
		}
		arr = bytesarray.toByteArray();
		//single_write();
		//multi_write();
		multi_write_mmap();
		//while(true){}
		
//		RandomAccessFile outraf = new RandomAccessFile("outrandomfile","rw");
//		for(int i=0;i<Value.numOfArraysSerial*1000;i++){
//			outraf.writeByte(raf.readByte());
//		}
	}
	
	public static void open_file_time() throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException {
		long startConsumer = System.currentTimeMillis();
		RandomAccessFile[] rafs = new RandomAccessFile[10];
		for(int i=0;i<10;i++){
			rafs[i] = new RandomAccessFile("bytes_multi","rw");
		}
		long endConsumer = System.currentTimeMillis();
        long T = endConsumer - startConsumer;
        System.out.println("time is: "+T);
	}
	
	public static void multi_write_mmap() throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException {
		long startConsumer = System.currentTimeMillis();
		Thread[] mythreads = new Thread[Value.threadNum];
		for(int i=0;i<Value.threadNum;i++){
			mythreads[i] = new Thread(new mythread(i,inFileChannel,arr));
			mythreads[i].start();
		}
		for(int i=0;i<Value.threadNum;i++){
			mythreads[i].join();
		}
		long endConsumer = System.currentTimeMillis();
        long T = endConsumer - startConsumer;
        System.out.println("time is: "+T);		
	}
	
	public static void multi_write() throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException {
		long startConsumer = System.currentTimeMillis();
		Thread[] mythreads = new Thread[10];
		for(int i=0;i<10;i++){
			mythreads[i] = new Thread(new mythread(i,inFileChannel,arr));
			mythreads[i].start();
		}
		for(int i=0;i<10;i++){
			mythreads[i].join();
		}
		long endConsumer = System.currentTimeMillis();
        long T = endConsumer - startConsumer;
        System.out.println("time is: "+T);
	}
	
	public static void randomaccess() throws FileNotFoundException, IOException, ClassNotFoundException {
		byte[] arr1 = new byte[10];
		byte[] arr2 = new byte[10];
		for(int i=0;i<10;i++){
			arr1[i] = '1';
			arr2[i] = '2';
		}
		//RandomAccessFile raf = new RandomAccessFile("randomaccess","rw");
		
		raf.seek(10);
		raf.write(arr1);
		raf.seek(0);
		raf.write(arr2);
		raf.seek(100);
		raf.write(arr1);
		while(true){}
	}
	public static void single_write() throws FileNotFoundException, IOException, ClassNotFoundException {
//		ByteArrayOutputStream bytesarray = new ByteArrayOutputStream();
//		for(int i=0;i<1000;i++){
//			bytesarray.write('1');
//		}
//		byte[] arr = bytesarray.toByteArray();
	//	RandomAccessFile raf = new RandomAccessFile("bytes_single","rw");
		long startConsumer = System.currentTimeMillis();
		raf.seek(0);
		for(int i=0;i<Value.numOfArraysSerial;i++){
			raf.write(arr);
		}
		long endConsumer = System.currentTimeMillis();
        long T = endConsumer - startConsumer;
        System.out.println("time is: "+T);
	}
		
	public static void objectoutputbytes() throws FileNotFoundException, IOException, ClassNotFoundException {
		String path = "bytes.txt";
		ByteArrayOutputStream bytesarray = new ByteArrayOutputStream();
		
		 ObjectOutputStream out = new ObjectOutputStream(bytesarray);
		 Test test = new Test();
		 out.writeObject(test);
		 test.kvs.put("poj", "peking");
		 out.writeObject(test);
		 out.flush();
		
		FileOutputStream file = new FileOutputStream(path);
		bytesarray.writeTo(file);
		bytesarray.flush();
		//while(true){}
	}
	
	public static void objectoutputtest() throws FileNotFoundException, IOException, ClassNotFoundException {

		 String path = "result.txt";
		 System.out.println(path);
		 
		 ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(path));
		 Test test = new Test();

		
		 test.kvs = null;
		 out.writeObject(test);
		 out.flush();
		 System.out.println(new File(path).length());
		 //test.kvs.put("poj", "peking");
		 //System.out.println(test.kvs.size());
		 
		 Test test2 = new Test();
		 out.writeObject(test2);
		 System.out.println(new File(path).length());
		 
		 out.writeObject(test);
		 out.close();
		 System.out.println(new File(path).length());
		 

		 ObjectInputStream oin = new ObjectInputStream(new FileInputStream(path));
		 //从文件依次读出两个文件
		 
		Test t1 = (Test) oin.readObject();
		Test t2 = (Test) oin.readObject();
		oin.close();
		 //判断两个引用是否指向同一个对象
		 System.out.println(t1 == t2);
		 //System.out.println(t1.kvs.size() + " " + t2.kvs.size());
	}

}

class Test implements Serializable{
	private static final long serialVersionUID = 1L;
	public Map<String, Object> kvs = new HashMap<>();
	//byte[] content = new byte[100];
	public Test(){
		kvs.put("topic","education");
		kvs.put("grade",98);
	}
}

class mythread implements Runnable{
	private int index;
	private FileChannel inFileChannel;
	private byte[] arr;
	public mythread(int index,FileChannel inFileChannel,byte[] arr){
		this.index = index;
		this.inFileChannel = inFileChannel;
		this.arr = arr;
	}
	public void run() {
		try {
			System.out.println("thread id2:"+index);
			for(int i=0;i<Value.numOfArraysThread;i++){
				MappedByteBuffer inMappedByteBuffer = inFileChannel.map(
					 MapMode.READ_WRITE, index*1000*Value.numOfArraysThread+i*1000,
					 1000); 	
				inMappedByteBuffer.put(arr);
			}
			System.out.println("thread id:"+index);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}	
}

class Value{
	public static int threadNum = 10;
	public static int numOfArraysThread = 500000;
	public static int numOfArraysSerial = numOfArraysThread*threadNum;
}

