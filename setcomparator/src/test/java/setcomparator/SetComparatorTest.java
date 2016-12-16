package setcomparator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashSet;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chenxu.org.apache.hadoop.hbase.filter.SetComparator;

public class SetComparatorTest {
	private static final Logger logger = LoggerFactory
			.getLogger(SetComparatorTest.class);
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
	public void testComperator() throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		
		HashSet<String> dbGeoSet = new HashSet<String>();
		dbGeoSet.add("c");
		dbGeoSet.add("d");
		
		ByteArrayOutputStream dbBaos = new ByteArrayOutputStream();
		ObjectOutputStream dbOos = new ObjectOutputStream(dbBaos);
		dbOos.writeObject(dbGeoSet);
		dbOos.close();
		dbBaos.close();
		
		Put put = new Put("test".getBytes());
		put.addColumn("c".getBytes(),"min".getBytes(),Bytes.toBytes(10));
		put.addColumn("c".getBytes(),"max".getBytes(),Bytes.toBytes(20));
		put.addColumn("c".getBytes(),"set".getBytes(),dbBaos.toByteArray());
		table.put(put );
		table.close();
		
		Get getFalse = new Get("test".getBytes());
		getFalse.addColumn(Bytes.toBytes("c"), Bytes.toBytes("set"));
		ByteArrayComparable comparatorFalse = new SetComparator("c,d,e");
		Filter falseFilter = new SingleColumnValueFilter("c".getBytes(),"set".getBytes(),CompareOp.LESS_OR_EQUAL,comparatorFalse);
		getFalse.setFilter(falseFilter);
		Assert.assertEquals(false, table.exists(getFalse));
		
		
		Get getTrue_cd = new Get("test".getBytes());
		getTrue_cd.addColumn(Bytes.toBytes("c"), Bytes.toBytes("set"));
		ByteArrayComparable comparatorTrue_cd = new SetComparator("c,d");
		Filter trueFilter_cd = new SingleColumnValueFilter("c".getBytes(),"set".getBytes(),CompareOp.LESS_OR_EQUAL,comparatorTrue_cd);
		getTrue_cd.setFilter(trueFilter_cd);
		Assert.assertEquals(true, table.exists(getTrue_cd));
		
		Get getTrue_c = new Get("test".getBytes());
		getTrue_c.addColumn(Bytes.toBytes("c"), Bytes.toBytes("set"));
		ByteArrayComparable comparatorTrue_c = new SetComparator("c");
		Filter trueFilter_c = new SingleColumnValueFilter("c".getBytes(),"set".getBytes(),CompareOp.LESS_OR_EQUAL,comparatorTrue_c);
		getTrue_c.setFilter(trueFilter_c);
		Assert.assertEquals(true, table.exists(getTrue_c));
		
		Get getTrue_d = new Get("test".getBytes());
		getTrue_d.addColumn(Bytes.toBytes("c"), Bytes.toBytes("set"));
		ByteArrayComparable comparatorTrue_d = new SetComparator("d");
		Filter trueFilter_d = new SingleColumnValueFilter("c".getBytes(),"set".getBytes(),CompareOp.LESS_OR_EQUAL,comparatorTrue_d);
		getTrue_d.setFilter(trueFilter_d);
		Assert.assertEquals(true, table.exists(getTrue_d));
		
		table.close();
		
	}
	

}
