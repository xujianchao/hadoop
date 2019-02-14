package com.ultrapower.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class HdfsTest {
	private Configuration cf = null;

	@Before
	public void setUp() throws Exception {
		cf = new Configuration();

	}

	@Test
	public void testHdfsFileDownLoadLocalFile() {
//		String downHdfsFilePath = "hdfs://spark1:9000/jack/test1.txt";
//		String localStoregePath = "D:\\\\logs\\\\hdfs.txt";
//		HdfsUtils.getFromHDFS(downHdfsFilePath, localStoregePath, cf);
	}

	@Test
	public void testUpload() {
//		String downHdfsFilePath = "hdfs://spark1:9000/wordcount/";
//		String localStoregePath = "D:\\logs2\\access.log.1"; 
//		HdfsUtils.uploadLocalFileToHdfs(localStoregePath, downHdfsFilePath, cf);
	}

	@Test
	public void testCompressUploadToHdfs() throws ClassNotFoundException, IOException {
//		String cpMethod = "org.apache.hadoop.io.compress.BZip2Codec";
//		// 这里是你需要压缩的文件路径，我这里是在家目录下的sogou500w文件
//		String localPath = "D:\\\\logs2\\\\a.txt";
//		// 这里是你压缩过后上传到hdfs上的路径，这里要注意一下，由于我们压缩文件过后文件的名字发生改变（多了后缀名）因此我们要写压缩过后的文件名，也就是加上.bz2的文件名（这里我重命名了一下，将sogou500w重命名为sogou500w2）
//		String hdfsPath = "hdfs://spark1:9000/jack/as.bz2";
//		HdfsUtils.uploadCompress(cpMethod, localPath, hdfsPath, cf);
	}

}
