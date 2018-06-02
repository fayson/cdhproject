package com.cloudera.kudu;

import com.google.common.collect.Lists;
import com.cloudera.utils.JsonStr2Map;
import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.flume.sink.KuduOperationsProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * package: com.cloudera.kudu
 * describe: 自定义的KuduSink用于解析JSON格式数据
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/6/2
 * creat_time: 下午11:07
 * 公众号：Hadoop实操
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class JsonKuduOperationsProducer implements KuduOperationsProducer {
    private static final Logger logger = LoggerFactory.getLogger(JsonKuduOperationsProducer.class);
    private static final String INSERT = "insert";
    private static final String UPSERT = "upsert";
    private static final List<String> validOperations = Lists.newArrayList(UPSERT, INSERT);

    public static final String ENCODING_PROP = "encoding";
    public static final String DEFAULT_ENCODING = "utf-8";
    public static final String OPERATION_PROP = "operation";
    public static final String DEFAULT_OPERATION = UPSERT;
    public static final String SKIP_MISSING_COLUMN_PROP = "skipMissingColumn";
    public static final boolean DEFAULT_SKIP_MISSING_COLUMN = false;
    public static final String SKIP_BAD_COLUMN_VALUE_PROP = "skipBadColumnValue";
    public static final boolean DEFAULT_SKIP_BAD_COLUMN_VALUE = false;
    public static final String WARN_UNMATCHED_ROWS_PROP = "skipUnmatchedRows";
    public static final boolean DEFAULT_WARN_UNMATCHED_ROWS = true;

    private KuduTable table;
    private Charset charset;
    private String operation;
    private boolean skipMissingColumn;
    private boolean skipBadColumnValue;
    private boolean warnUnmatchedRows;

    public JsonKuduOperationsProducer() {
    }

    @Override
    public void configure(Context context) {
        String charsetName = context.getString(ENCODING_PROP, DEFAULT_ENCODING);
        try {
            charset = Charset.forName(charsetName);
        } catch (IllegalArgumentException e) {
            throw new FlumeException(
                    String.format("Invalid or unsupported charset %s", charsetName), e);
        }
        operation = context.getString(OPERATION_PROP, DEFAULT_OPERATION).toLowerCase();
        Preconditions.checkArgument(
                validOperations.contains(operation),
                "Unrecognized operation '%s'",
                operation);
        skipMissingColumn = context.getBoolean(SKIP_MISSING_COLUMN_PROP,
                DEFAULT_SKIP_MISSING_COLUMN);
        skipBadColumnValue = context.getBoolean(SKIP_BAD_COLUMN_VALUE_PROP,
                DEFAULT_SKIP_BAD_COLUMN_VALUE);
        warnUnmatchedRows = context.getBoolean(WARN_UNMATCHED_ROWS_PROP,
                DEFAULT_WARN_UNMATCHED_ROWS);
    }

    @Override
    public void initialize(KuduTable table) {
        this.table = table;
    }

    @Override
    public List<Operation> getOperations(Event event) throws FlumeException {
        String raw = new String(event.getBody(), charset);
        Map<String, String> rawMap = JsonStr2Map.jsonStr2Map(raw);

        Schema schema = table.getSchema();
        List<Operation> ops = Lists.newArrayList();
        if(raw != null && !raw.isEmpty()) {
            Operation op;
            switch (operation) {
                case UPSERT:
                    op = table.newUpsert();
                    break;
                case INSERT:
                    op = table.newInsert();
                    break;
                default:
                    throw new FlumeException(
                            String.format("Unrecognized operation type '%s' in getOperations(): " +
                                    "this should never happen!", operation));
            }
            PartialRow row = op.getRow();
            for (ColumnSchema col : schema.getColumns()) {
                logger.error("Column:" + col.getName() + "----" + rawMap.get(col.getName()));
                try {
                    coerceAndSet(rawMap.get(col.getName()), col.getName(), col.getType(), row);
                } catch (NumberFormatException e) {
                    String msg = String.format(
                            "Raw value '%s' couldn't be parsed to type %s for column '%s'",
                            raw, col.getType(), col.getName());
                    logOrThrow(skipBadColumnValue, msg, e);
                } catch (IllegalArgumentException e) {
                    String msg = String.format(
                            "Column '%s' has no matching group in '%s'",
                            col.getName(), raw);
                    logOrThrow(skipMissingColumn, msg, e);
                } catch (Exception e) {
                    throw new FlumeException("Failed to create Kudu operation", e);
                }
            }
            ops.add(op);

        }
        return ops;
    }

    /**
     * Coerces the string `rawVal` to the type `type` and sets the resulting
     * value for column `colName` in `row`.
     *
     * @param rawVal the raw string column value
     * @param colName the name of the column
     * @param type the Kudu type to convert `rawVal` to
     * @param row the row to set the value in
     * @throws NumberFormatException if `rawVal` cannot be cast as `type`.
     */
    private void coerceAndSet(String rawVal, String colName, Type type, PartialRow row)
            throws NumberFormatException {
        switch (type) {
            case INT8:
                row.addByte(colName, Byte.parseByte(rawVal));
                break;
            case INT16:
                row.addShort(colName, Short.parseShort(rawVal));
                break;
            case INT32:
                row.addInt(colName, Integer.parseInt(rawVal));
                break;
            case INT64:
                row.addLong(colName, Long.parseLong(rawVal));
                break;
            case BINARY:
                row.addBinary(colName, rawVal.getBytes(charset));
                break;
            case STRING:
                row.addString(colName, rawVal==null?"":rawVal);
                break;
            case BOOL:
                row.addBoolean(colName, Boolean.parseBoolean(rawVal));
                break;
            case FLOAT:
                row.addFloat(colName, Float.parseFloat(rawVal));
                break;
            case DOUBLE:
                row.addDouble(colName, Double.parseDouble(rawVal));
                break;
            case UNIXTIME_MICROS:
                row.addLong(colName, Long.parseLong(rawVal));
                break;
            default:
                logger.warn("got unknown type {} for column '{}'-- ignoring this column", type, colName);
        }
    }

    private void logOrThrow(boolean log, String msg, Exception e)
            throws FlumeException {
        if (log) {
            logger.warn(msg, e);
        } else {
            throw new FlumeException(msg, e);
        }
    }

    @Override
    public void close() {
    }
}