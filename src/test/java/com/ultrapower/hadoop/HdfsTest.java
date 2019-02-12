package com.ultrapower.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class HdfsTest {
	private Configuration cf=null;
	@Before
	public void setUp() throws Exception {
		cf = new Configuration();
		
	}

	@Test
	public void testHdfsFileDownLoadLocalFile() {
		String downHdfsFilePath="hdfs://spark1:9000/jack/test1.txt";
		String localStoregePath="D:\\\\logs\\\\hdfs.txt";
		HdfsUtils.getFromHDFS(downHdfsFilePath, localStoregePath, cf);
	}
	
	@Test
	public void testUpload() {
		String downHdfsFilePath="hdfs://spark1:9000/jack/test2.txt";
		String localStoregePath="D:\\\\logs\\\\hdfs.txt";
		HdfsUtils.uploadLocalFileToHdfs(localStoregePath, downHdfsFilePath, cf);
	}
}
