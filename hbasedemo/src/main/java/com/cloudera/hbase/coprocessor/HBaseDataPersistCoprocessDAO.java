//package com.cloudera.hbase.coprocessor;
//
//import com.cmbchina.graph.graphsearchcustomized.hbase.constant.ResultStatus;
//import com.cmbchina.graph.graphsearchcustomized.hbase.util.ThreadPoolUtil;
//import com.cmbchina.graph.graphsearchcustomized.log.GLog;
//import com.cmbchina.graph.graphsearchcustomized.log.LogFactory;
//import com.google.common.util.concurrent.ThreadFactoryBuilder;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
//import org.apache.hadoop.hbase.client.coprocessor.BigDecimalColumnInterpreter;
//import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
//import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
//import org.apache.hadoop.hbase.coprocessor.AggregateImplementation;
//import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
//import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.PostConstruct;
//import java.io.IOException;
//import java.math.BigDecimal;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.*;
//
//@Component
//public class HBaseDataPersistCoprocessDAO<Entity>{
//
//	private static final GLog LOG = LogFactory.getLogger(HBaseDataPersistCoprocessDAO.class);
//
//	// TODO consider to change Map<String, ConcurrentArrayList<Connection>>, in
//	// general, assign a connection for per thread
//	private static Map<String, Connection> connectionMap = new ConcurrentHashMap<String, Connection>();
//
//	@Value("${hbase.url}")
//	private String hbaseUrl;
//
//	@PostConstruct
//	public void initConnection(){
//		String key = hbaseUrl;
//		String[] arrays = hbaseUrl.split(":");
//		String port = arrays[2];
//		String quorum_url = arrays[1].split("//")[1];
//		Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.property.clientPort", String.valueOf(port));
//		conf.set("hbase.zookeeper.quorum", quorum_url);
//		ThreadPoolUtil threadPoolUtil = ThreadPoolUtil.init();
//		// TODO table, admin thread safe
//		// https://stackoverflow.com/questions/41499087/can-we-reuse-the-hbase-java-client-connection-between-multi-threads
//		try {
//			connectionMap.put(key, ConnectionFactory.createConnection(conf, threadPoolUtil.getExecutor()));
//		} catch (IOException e) {
//			LOG.info(ResultStatus.ERROR_INVALID_SERVER, "HBase 建立链接失败 ", e);
//		}
//	}
//
//	public static Connection getConnection(String url) {
//		String key = url;
//		Connection connection = connectionMap.getOrDefault(key, null);
//		try {
//
//			if (connection != null && !connection.isClosed()) {
//				return connection;
//			}
//			if (connection != null && connection.isClosed()) {
//				connectionMap.remove(key);
//				connection = null;
//			}
//
//			if (connection == null) {
//				String[] arrays = url.split(":");
//				String port = arrays[2];
//				String quorum_url = arrays[1].split("//")[1];
//				//Configuration conf = HBaseConf.create();
//				Configuration conf = HBaseConfiguration.create();
//				conf.set("hbase.zookeeper.property.clientPort", String.valueOf(port));
//				conf.set("hbase.zookeeper.quorum", quorum_url);
//				ThreadPoolUtil threadPoolUtil = ThreadPoolUtil.init();
//				// TODO table, admin thread safe
//				// https://stackoverflow.com/questions/41499087/can-we-reuse-the-hbase-java-client-connection-between-multi-threads
//				connection = ConnectionFactory.createConnection(conf, threadPoolUtil.getExecutor());
//				connectionMap.put(key, connection);
//			}
//
//		} catch (ArrayIndexOutOfBoundsException ae) {
//			LOG.logError(ResultStatus.ERROR_DB_USAGE_EXCEPTION, "给定的连接字符串不符合格式，正确的格式应是url:port", ae);
//			// TODO throw ae
//		} catch (IOException e) {
//			LOG.logError(ResultStatus.ERROR_INVALID_SERVER, "HBase 建立链接失败 ", e);
//		}
//		return connection;
//	}
//
//	/**
//	 * 并发scan，利用AggregationClient直接查询聚合后的结果，减少数据的网络传输
//	 * @return 每个实体 --> 每个mergesetting --> Pair: <resultFieldName,value>
//	 * */
//    public static List<List<Pair<String, String>>> ParallelQueryByTimeAndSchema(String url, String tableName, List<Pair<Pair<String, String>, List<Pair<String, String>>>> keyAndFieldPairList) {
//		List<List<Pair<String, String>>> result;
//
//		//针对每个实体对的每个mergesetting配置，都需要一个scan和对应的聚合运算类型
//		List<List<Pair<Scan,String>>> relationList = new ArrayList<List<Pair<Scan,String>>>();
//        //relationPair数据结构：< <startRowkey,stopRowKey>, [<originField1,aggregateTypeString~resultFieldName>,<originField2,aggregateTypeString~resultFieldName>,...] >
//		keyAndFieldPairList.forEach(relationPair -> {
//			List<Pair<Scan,String>> scanList = new ArrayList<Pair<Scan,String>>();
//			String startRowKey = relationPair.getLeft().getLeft();
//			String endRowKey = relationPair.getLeft().getRight();
//			for (Pair<String, String> scanPair : relationPair.getRight()) {
//				Scan scan = new Scan();
//				scan.setStartRow(Bytes.toBytes(startRowKey));
//				scan.setStopRow(Bytes.toBytes(endRowKey));
//				scan.setCaching(2000);
//				scan.addColumn(Bytes.toBytes("objects"), Bytes.toBytes(scanPair.getLeft()));
//				scanList.add(Pair.of(scan, scanPair.getRight()));
//			}
//			relationList.add(scanList);
//		});
//		result=parallelScan(url, tableName, relationList);
//		return result;
//	}
//
//	private static List<List<Pair<String, String>>> parallelScan(String url, String tableName, List<List<Pair<Scan,String>>> relationList){
//
//		List<List<Pair<String, String>>> ret = new ArrayList<>();
//		List<Future<List<Pair<String, String>>>> futures = new ArrayList<Future<List<Pair<String, String>>>>();
//
//		String[] arrays = url.split(":");
//		String port = arrays[2];
//		String quorum_url = arrays[1].split("//")[1];
//		Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.property.clientPort", String.valueOf(port));
//		conf.set("hbase.zookeeper.quorum", quorum_url);
//
//		// 添加协处理器
//		try {
//			HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
//			TableName table = TableName.valueOf(tableName);
//			HTableDescriptor htd = hbaseAdmin.getTableDescriptor(table);
//			Boolean hasAgg = false;
//			for (String copStr : htd.getCoprocessors()) {
//				if(copStr.contains("AggregateImplementation")){
//					hasAgg = true;
//				}
//			}
//			if (!hasAgg) {
////				hbaseAdmin.disableTable(table);
//				htd.addCoprocessor(AggregateImplementation.class.getName());
//				hbaseAdmin.modifyTable(table, htd);
//				LOG.info("HBase 建立链接，为table（{0}）设置协处理器成功 ", tableName);
////				hbaseAdmin.enableTable(table);
//			}
//			hbaseAdmin.close();
//		} catch (IOException e) {
//			LOG.info(ResultStatus.ERROR_INVALID_SERVER, "HBase 建立链接，为table设置协处理器失败 ", e);
//		}
//
//		ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
//		builder.setNameFormat("parallelScan-pool-%d");
//		ThreadFactory factory = builder.build();
//		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(relationList.size(), factory);
//		relationList.forEach(relationEach -> {
//			Callable<List<Pair<String, String>>> callable = new ScanCallable(conf, tableName, relationEach);
//			FutureTask<List<Pair<String, String>>> futureTask = (FutureTask<List<Pair<String, String>>>)executor.submit(callable);
//			futures.add(futureTask);
//		});
//		executor.shutdown();
//
//
//		// Wait for all the tasks to finish
//		try {
//			boolean stillRunning = !executor.awaitTermination(
//					3600000, TimeUnit.MILLISECONDS);
//			if (stillRunning) {
//				try {
//					executor.shutdownNow();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
//		} catch (InterruptedException e) {
//			try {
//				Thread.currentThread().interrupt();
//			} catch (Exception e1) {
//				e1.printStackTrace();
//			}
//		}
//
//		// Look for any exception
//		for (Future<List<Pair<String, String>>> f : futures) {
//			try {
//				if(f.get() != null)
//				{
//					ret.add(f.get());
//				}
//			} catch (InterruptedException e) {
//				try {
//					Thread.currentThread().interrupt();
//				} catch (Exception e1) {
//					e1.printStackTrace();
//				}
//			} catch (ExecutionException e) {
//				e.printStackTrace();
//			}
//		}
//		return ret;
//	}
//
//	static class ScanCallable implements Callable<List<Pair<String, String>>>{
//
//		private Configuration configuration;
//		private String tableName;
//		private List<Pair<Scan,String>> scanList;
//
//		ScanCallable(Configuration configuration, String tableName, List<Pair<Scan,String>> scanList) {
//			this.configuration = configuration;
//			this.tableName = tableName;
//			this.scanList = scanList;
//		}
//
//		@Override
//		public List<Pair<String, String>> call() throws Exception {
//			List<Pair<String, String>> kv = new ArrayList<>();
//			long tstartTime = System.currentTimeMillis();
//			try{
//                AggregationClient aClient = new AggregationClient(configuration);
//                final ColumnInterpreter<Double, Double, HBaseProtos.EmptyMsg, HBaseProtos.DoubleMsg, HBaseProtos.DoubleMsg> ci = new DoubleColumnInterpreter();
////				final ColumnInterpreter<BigDecimal, BigDecimal, HBaseProtos.EmptyMsg, HBaseProtos.BigDecimalMsg, HBaseProtos.BigDecimalMsg> ci = new BigDecimalColumnInterpreter();
////				final ColumnInterpreter<Long, Long, HBaseProtos.EmptyMsg, HBaseProtos.LongMsg, HBaseProtos.LongMsg> ci = new LongColumnInterpreter();
//				TableName table = TableName.valueOf(tableName);
//				for (Pair<Scan, String> scanPair : scanList) {
//					//<resultField, aggregateResult>
//					Pair<String, String> retPair;
//					Scan scan  = scanPair.getLeft();
//					String aggreStr = scanPair.getRight().split("~")[0];
//					String resultFieldName = scanPair.getRight().split("~")[1];
//					Object aggResult = null;
//					switch (aggreStr.toUpperCase()){
//						case "COUNT":
////							final ColumnInterpreter<Long, Long, HBaseProtos.EmptyMsg, HBaseProtos.LongMsg, HBaseProtos.LongMsg> ci_count = new LongColumnInterpreter();
//							aggResult = aClient.rowCount(table, ci, scan);
//							retPair = Pair.of(resultFieldName, String.valueOf(aggResult));
//							kv.add(retPair);
//							break;
//						case "AVG":
//							aggResult = aClient.avg(table, ci, scan);
//							retPair = Pair.of(resultFieldName, String.valueOf(aggResult));
//							kv.add(retPair);
//							break;
//						case "SUM":
//							aggResult = aClient.sum(table, ci, scan);
//							retPair = Pair.of(resultFieldName, String.valueOf(aggResult));
//							kv.add(retPair);
//							break;
//						case "MAX":
//							aggResult = aClient.max(table, ci, scan);
//							retPair = Pair.of(resultFieldName, String.valueOf(aggResult));
//							kv.add(retPair);
//							break;
//						case "MIN":
//							aggResult = aClient.min(table, ci, scan);
//							retPair = Pair.of(resultFieldName, String.valueOf(aggResult));
//							kv.add(retPair);
//							break;
//						default:
//							LOG.info("不存在该算子:{0},请输入正确的算子：count,avg,sum,max或min", aggreStr.toUpperCase());
//							break;
//
//					}
//				}
//                LOG.info("线程({0})聚合计算({1}_耗时({2}))", Thread.currentThread().getName(), kv.size(), (System.currentTimeMillis() - tstartTime));
//
//			}catch (Throwable e){
//				LOG.logError(e, ResultStatus.ERROR_DB_USAGE_EXCEPTION, "HBase查询异常");
//			}
//			return kv;
//		}
//	}
//
//}
