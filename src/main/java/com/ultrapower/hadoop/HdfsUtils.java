package com.ultrapower.hadoop;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Hello world!
 *
 */
public class HdfsUtils {
	public static void main(String[] args) throws ClassNotFoundException, IOException {
//		Configuration cf = new Configuration();
//		String downHdfsFilePath = "hdfs://spark1:9000/jack/test1.txt";
//		String localStoregePath = "D:\\\\logs\\\\hdfs.txt";
//		HdfsUtils.getFromHDFS(downHdfsFilePath, localStoregePath, cf);
//		String fileName = "D:\\logs\\test1.txt"; 
//		String compressMethod = "org.apache.hadoop.io.compress.BZip2Codec";
//		
//		Configuration cf = new Configuration();
//
//		Class codecClass = Class.forName(compressMethod);
//
//		String hdfsDir = "hdfs://spark1:9000/jack/test1.bz2";
//
//		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, cf);
//		
////		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
//		
//
//		File fileIn = new File(fileName);
//		InputStream in = new FileInputStream(fileIn);
//  
//		// 该压缩方法对应的文件扩展名
//		File fileOut = new File(fileName + codec.getDefaultExtension());
//		fileOut.delete();
//
//		OutputStream out = new FileOutputStream(fileOut);
//		CompressionOutputStream cout = codec.createOutputStream(out);
//
//		System.out.println("[" + new Date() + "]: start compressing ");
//		IOUtils.copyBytes(in, cout, 1024 * 1024 * 5, false); // 缓冲区设为5MB
//		System.out.println("[" + new Date() + "]: compressing finished ");
//
//		in.close();
//		cout.close();

//        try{
//            Path localPath = new Path(localDir);
//            Path hdfsPath = new Path(hdfsDir);
//            FileSystem hdfs = FileSystem.get(cf);
//            if(!hdfs.exists(hdfsPath)){
//                 hdfs.mkdirs(hdfsPath);
//             }
//             hdfs.copyFromLocalFile(localPath, hdfsPath);
//         }catch(Exception e){
//         e.printStackTrace();
//         }

	}

	/**
	 * @author 文件下载
	 * @param src
	 * @param dst
	 * @param conf
	 * @return
	 */
	public static boolean getFromHDFS(String src, String dst, Configuration conf) {
		Path dstPath = new Path(dst);
		try {
			FileSystem dhfs = dstPath.getFileSystem(conf);
			dhfs.copyToLocalFile(false, new Path(src), dstPath);
		} catch (IOException ie) {
			ie.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * @author 文件上传
	 * @param src
	 * @param dst
	 * @param conf
	 * @return
	 */
	public static boolean uploadLocalFileToHdfs(String localFile, String targetPath, Configuration conf) {
		try {
			Path localPath = new Path(localFile);
			Path hdfsPath = new Path(targetPath);
			FileSystem hdfs = FileSystem.get(conf);
			if (!hdfs.exists(hdfsPath)) {
				hdfs.mkdirs(hdfsPath);
			}
			hdfs.copyFromLocalFile(localPath, hdfsPath);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**@author 压缩上传
	 * 
	 * @param cpMethod
	 * @param localPath
	 * @param hdfsPath
	 * @param conf
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public static boolean uploadCompress(String cpMethod ,String localPath ,String hdfsPath, Configuration conf) throws ClassNotFoundException, IOException {
		 Class codecClass = Class.forName(cpMethod);
		 CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		 FileSystem fs = FileSystem.get(URI.create("hdfsPath"),conf);
		 InputStream ins = new BufferedInputStream(new FileInputStream(localPath));
		 OutputStream ous =  fs.create(new Path(hdfsPath));
		 CompressionOutputStream  cout = codec.createOutputStream(ous);
		 IOUtils.copyBytes(ins, cout,1024 ,true);
 
		return true;
	}
}