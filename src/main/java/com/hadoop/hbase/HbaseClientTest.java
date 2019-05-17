package com.hadoop.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseClientTest {
	
	Connection conn = null;
	
	@Before
	public void getConn() throws Exception{
		// 构建一个连接对象
		Configuration conf = HBaseConfiguration.create(); // 会自动加载hbase-site.xml
		conf.set("hbase.zookeeper.quorum", "mac:2181,ubuntu1:2181,ubuntu2:2181");
		conn = ConnectionFactory.createConnection(conf);
	}
	
	/**
	 * DDL
	 * @throws Exception
	 */
	@Test
	public void testCreateTable() throws Exception{
		
		// 从连接中构造一个DDL操作器
		Admin admin = conn.getAdmin();
		
		// 创建一个表定义描述对象
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info"));
		
		// 创建列族定义描述对象
		HColumnDescriptor hColumnDescriptor_1 = new HColumnDescriptor("base_info");
		hColumnDescriptor_1.setMaxVersions(3); // 设置该列族中存储数据的最大版本数,默认是1
		
		HColumnDescriptor hColumnDescriptor_2 = new HColumnDescriptor("extra_info");
		
		// 将列族定义信息对象放入表定义对象中
		hTableDescriptor.addFamily(hColumnDescriptor_1);
		hTableDescriptor.addFamily(hColumnDescriptor_2);
		
		
		// 用ddl操作器对象：admin 来建表
		admin.createTable(hTableDescriptor);
		
		// 关闭连接
		admin.close();
		conn.close();
		
	}
	
	
	/**
	 * 删除表
	 * @throws Exception
	 */
	@Test
	public void testDropTable() throws Exception{
		
		Admin admin = conn.getAdmin();
		
		// 停用表
		admin.disableTable(TableName.valueOf("user_info"));
		// 删除表
		admin.deleteTable(TableName.valueOf("user_info"));
		
		
		admin.close();
		conn.close();
	}
	
	// 修改表定义--添加一个列族
	@Test
	public void testAlterTable() throws Exception{
		
		Admin admin = conn.getAdmin();
		
		// 取出旧的表定义信息
		HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info"));
		
		
		// 新构造一个列族定义
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info");
		hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL); // 设置该列族的布隆过滤器类型
		
		// 将列族定义添加到表定义对象中
		tableDescriptor.addFamily(hColumnDescriptor);
		
		
		// 将修改过的表定义交给admin去提交
		admin.modifyTable(TableName.valueOf("user_info"), tableDescriptor);
		
		
		admin.close();
		conn.close();
		
	}
	
	
	/**
	 * DML -- 数据的增删改查
	 */
	
	/**
	 * 增加
	 */
	@Test
	public void testPut() throws IOException {
		// 获取table对象
		Table table = conn.getTable(TableName.valueOf("user_info"));
		// 构造要插入的数据为一个Put类型的对象
		// 1个put对象1个行键 也就是1个rowkey
		
		List<Put> list = new ArrayList<>();
		
		Put put = new Put(Bytes.toBytes("002"));
		put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("username"),Bytes.toBytes("何超杰"));
		put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes(18));
		put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("sex"),Bytes.toBytes("男"));
		
		Put put2 = new Put(Bytes.toBytes("003"));
		put2.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("username"),Bytes.toBytes("何超杰"));
		put2.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes(18));
		put2.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("sex"),Bytes.toBytes("男"));
		
		
		list.add(put);
		list.add(put2);
		
		table.put(list);
		
		table.close();
		conn.close();
	}
	
	/**
	 * 删除
	 */
	@Test
	public void testDelete() throws IOException {
		// 获取table对象
		Table table = conn.getTable(TableName.valueOf("user_info"));
		
		List<Delete> list = new ArrayList<>();
		
		// 删除001行
		Delete delete = new Delete(Bytes.toBytes("001"));
		
		// 只删除002行的 base_info列族的一个属性
		Delete delete2 = new Delete(Bytes.toBytes("002"));
		delete2.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("username"));
		
		list.add(delete);
		list.add(delete2);
		
		table.delete(list);
		
		table.close();
		conn.close();
	}
	
	/**
	 * 查询
	 */
	@Test
	public void testGet() throws IOException {
		// 获取table对象
		Table table = conn.getTable(TableName.valueOf("user_info"));
		
		Get get = new Get(Bytes.toBytes("002"));
		
		Result result = table.get(get);
		
		byte[] values = result.getValue(Bytes.toBytes("base_info"),Bytes.toBytes("username"));
		System.out.println(new String(values));
		
		byte[] rowkey = result.getRow();
		System.out.println(new String(rowkey));
		// 遍历整行结果中的所有kv单元格
		CellScanner cellScannable = result.cellScanner();
		while(cellScannable.advance()){
			Cell cell = cellScannable.current();
			
			byte[] rowArray = cell.getRowArray();
			System.out.println(new String(rowArray,cell.getRowOffset(),cell.getRowLength()));
			
			byte[] familys = cell.getFamilyArray(); // 列族名的字节数组
			System.out.println(new String(familys,cell.getFamilyOffset(),cell.getFamilyLength()));
			byte[] qualifiers =  cell.getQualifierArray(); // 列名的字节数组
			System.out.println(new String(qualifiers,cell.getQualifierOffset(),cell.getQualifierLength()));
			
			byte[] valueArray =  cell.getValueArray(); // 数据的字节数组
			System.out.println(new String(valueArray,cell.getValueOffset(),cell.getValueLength()));
		}
		
		table.get(get);
		table.close();
		conn.close();
	}
	
	
}
