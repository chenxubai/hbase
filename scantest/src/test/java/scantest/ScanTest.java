package scantest;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ScanTest {
	private static final Logger logger = LoggerFactory
			.getLogger(ScanTest.class);
	private static final HBaseTestingUtility testUtility = new HBaseTestingUtility();

	private String tableName;
	private Connection connection;

	@BeforeClass
	public static void setUpOnce() throws Exception {
		logger.info("@BeforeClass 开始执行。。。。。。。。。。。。。。。。。");
		testUtility.startMiniCluster();
	}

	@AfterClass
	public static void tearDownOnce() throws Exception {
		logger.info("@AfterClass 执行了。。。。。。。。。。。。。。。。。。。。。");
		testUtility.shutdownMiniCluster();
	}

	@Before
	public void setUp() throws IOException {

		logger.info("hbase 开始建表。。。。。。。。。。。。。。。。。");
		tableName = "test_table";
		testUtility.createTable(tableName.getBytes(), "c".getBytes(),2);
		connection = testUtility.getConnection();
	}

	@After
	public void tearDown() throws IOException {
	}
	
	
	
	@Test
	public void testScan() {
		
		Table table = null;
		ResultScanner scanner = null;
		String geo = "geoabc";
		try {
			String cfinfo = "info";
			String cfwucha = "wucha";
			String cftime = "time";
			String tableName = "index4";
			byte[] bytes = Bytes.toBytes(cfinfo);
			byte[] bytes2 = Bytes.toBytes(cfwucha);
			byte[] bytes3 = Bytes.toBytes(cftime);
			testUtility.createTable(tableName.getBytes(),new byte[][]{bytes,bytes2,bytes3},2);
			table = connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(geo.getBytes());
			byte[] bytes4 = "guojia".getBytes();
			put.addColumn(cfinfo.getBytes(),bytes4,Bytes.toBytes("中国"));
			put.addColumn(cfinfo.getBytes(),"sheng".getBytes(),Bytes.toBytes("北京"));
			put.addColumn(cfinfo.getBytes(),"shi".getBytes(),Bytes.toBytes("海淀"));
			
			put.addColumn(cfwucha.getBytes(),"20161001".getBytes(),Bytes.toBytes(100));
			put.addColumn(cfwucha.getBytes(),"20161002".getBytes(),Bytes.toBytes(200));
			put.addColumn(cfwucha.getBytes(),"20161003".getBytes(),Bytes.toBytes(500));
			
			
			put.addColumn(cftime.getBytes(),"20161001".getBytes(),Bytes.toBytes(143323131321l));//1440
			put.addColumn(cftime.getBytes(),"20161001".getBytes(),Bytes.toBytes(143323131322l));
			put.addColumn(cftime.getBytes(),"20161002".getBytes(),Bytes.toBytes(143323131321l));
			put.addColumn(cftime.getBytes(),"20161003".getBytes(),Bytes.toBytes(143323131321l));
			table.put(put );

			Scan scan = new Scan();
			scanner = table.getScanner(scan);
			Result next = scanner.next();
			System.out.println(Bytes.toString(next.getValue(bytes, bytes4)));
			System.out.println("地区微信统计ERRO\n================\n geo["+geo+"]\n================");
		} catch (IOException e) {
			logger.error("地区微信统计ERRO\\n================\n geo["+geo+"]",e);
		} finally {
			
			if(null!=null) {
				try {
					table.close();
				} catch (IOException e) {
					
				}
			}
			
			if(null!=scanner) {
				scanner.close();
				
			}
			
		}
		
		
		
	}
	
	

}
