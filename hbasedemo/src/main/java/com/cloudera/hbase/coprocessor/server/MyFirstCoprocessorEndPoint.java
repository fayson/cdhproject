package com.cloudera.hbase.coprocessor.server;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * package: com.cloudera.hbase.coprocessor.server
 * describe: HBase RegionServer上Endpoint Coprocessor实现，主要实现对指定列的Count、MAX、MIN、SUM聚合操作
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/5/13
 * creat_time: 下午11:11
 * 公众号：Hadoop实操
 */
public class MyFirstCoprocessorEndPoint extends MyFirstCoprocessor.AggregationService implements Coprocessor, CoprocessorService {
    protected static final Log log = LogFactory.getLog(MyFirstCoprocessorEndPoint.class);

    private RegionCoprocessorEnvironment env;

    @Override
    public void getAggregation(RpcController controller, MyFirstCoprocessor.MyCoprocessRequest request, RpcCallback<MyFirstCoprocessor.MyCoprocessResponse> done) {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(request.getFamily()));

        //传入列的方式   sales:MAX,sales:MIN,sales:AVG,slaes:SUM,sales:COUNT
        String colums = request.getColumns();
        //记录所有要扫描的列
        Map<String, List<String>> columnMaps = new HashedMap();

        for (String columnAndType : colums.split(",")) {
            String column = columnAndType.split(":")[0];
            String type = columnAndType.split(":")[1];
            List<String> typeList = null;
            if (columnMaps.containsKey(column)) {
                typeList = columnMaps.get(column);
            } else {
                typeList = new ArrayList<>();
                //将column添加到Scan中
                scan.addColumn(Bytes.toBytes(request.getFamily()), Bytes.toBytes(column));
            }
            typeList.add(type);
            columnMaps.put(column, typeList);
        }

        InternalScanner scanner = null;
        MyFirstCoprocessor.MyCoprocessResponse response = null;
        Double max = null;
        Double min = null;
        Double sumVal = null;
        long counter = 0L;

        try {
            scanner = this.env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<>();

            boolean hasMore = false;
            scanner = env.getRegion().getScanner(scan);
            do {
                hasMore = scanner.next(results);

                if (results.size() > 0) {
                    ++counter;
                }
                log.info("counter:" + counter);
                log.info("results size:" + results.size());
                for (Cell cell : results) {
                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                    log.info("Column Name: " + column);
                    log.info("Cell Value:" + new String(CellUtil.cloneValue(cell)));
                    Double temp = Double.parseDouble(new String(CellUtil.cloneValue(cell)));
                    if (columnMaps.containsKey(column)) {
                        List<String> types = columnMaps.get(column);
                        for (String type : types) {
                            switch (type.toUpperCase()) {
                                case "MIN":
                                    min = min != null && (temp == null || compare(temp, min) >= 0) ? min : temp;
                                    log.info("MIN Value: " + min.doubleValue());
                                    break;
                                case "MAX":
                                    max = max != null && (temp == null || compare(temp, max) <= 0) ? max : temp;
                                    break;
                                case "SUM":
                                    if (temp != null) {
                                        sumVal = add(sumVal, temp);
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }

                    }
                }
                results.clear();
            } while (hasMore);

            response = MyFirstCoprocessor.MyCoprocessResponse.newBuilder()
                    .setMaxnum(max!=null?max.doubleValue():Double.MAX_VALUE)
                    .setMinnum(min!=null?min.doubleValue():Double.MIN_NORMAL)
                    .setCount(counter)
                    .setSumnum(sumVal!=null?sumVal.doubleValue():Double.MIN_NORMAL).build();

        } catch (IOException e) {
            e.printStackTrace();
            ResponseConverter.setControllerException(controller, e);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        done.run(response);
    }

    public static int compare(Double l1, Double l2) {
        if (l1 == null ^ l2 == null) {
            return l1 == null ? -1 : 1; // either of one is null.
        } else if (l1 == null)
            return 0; // both are null
        return l1.compareTo(l2); // natural ordering.
    }

    public double divideForAvg(Double d1, Long l2) {
        return l2 != null && d1 != null?d1.doubleValue() / l2.doubleValue():0.0D / 0.0;
    }

    public Double add(Double d1, Double d2) {
        return d1 != null && d2 != null ? Double.valueOf(d1.doubleValue() + d2.doubleValue()) : (d1 == null ? d2 : d1);
    }


    @Override
    public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        if (coprocessorEnvironment instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) coprocessorEnvironment;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {

    }

    @Override
    public Service getService() {
        return this;
    }
}
