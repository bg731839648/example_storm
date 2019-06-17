package storm.lesson.partitionTransactionalOpaque.bolt;


import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class MyOpaquePtTxCommitterBolt extends BaseTransactionalBolt implements ICommitter {

    private static final long serialVersionUID = -2524702063291167257L;
    private static Map<String, DbValue> dbMap = new HashMap<String, DbValue>();

    private static Map<String, Integer> countMap = new HashMap<String, Integer>();
    private TransactionAttempt id;
    private String today = "2019-06-03";

    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector collector, TransactionAttempt transactionAttempt) {
//        System.out.println("执行 MyOpaquePtTxCommitterBolt.prepare");
        this.id = transactionAttempt;
    }

    @Override
    public void execute(Tuple tuple) {
//        System.out.println("执行 MyOpaquePtTxCommitterBolt.execute");
        String tupleToday = tuple.getString(1);
        Integer count = tuple.getInteger(2);
        if (tupleToday != null && count != null) {

            if (today.equals(tupleToday)) {
                Integer batchCount = countMap.get(today);
                if (batchCount == null) {
                    batchCount = 0;
                }
                batchCount += count;
                countMap.put(today, batchCount);
            }
        }
    }

    @Override
    public void finishBatch() {
//        System.out.println("执行 MyOpaquePtTxCommitterBolt.finishBatch");
        if (countMap.size() > 0) {
            DbValue value = dbMap.get(today);
            DbValue newValue;
            if (value == null || !value.txId.equals(id.getTransactionId())) {
                // 更新數據庫
                newValue = new DbValue();
                newValue.txId = id.getTransactionId();
                newValue.dateStr = today;
                newValue.count = countMap.get(today);

                if (value == null) {
                    newValue.preCount = 0;
                } else {
                    newValue.preCount = value.count;
                }
                dbMap.put(today, newValue);
            }
            System.err.println("total======================:" + dbMap.get(today).count);
        }
        // collector.emit(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        System.out.println("执行 MyOpaquePtTxCommitterBolt.declareOutputFields");
    }

    public static class DbValue implements Serializable {

        private static final long serialVersionUID = 5010991727124701731L;

        BigInteger txId;

        int count = 0;

        String dateStr;

        int preCount;

        public BigInteger getTxId() {
            return txId;
        }

        public void setTxId(BigInteger txId) {
            this.txId = txId;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public String getDateStr() {
            return dateStr;
        }

        public void setDateStr(String dateStr) {
            this.dateStr = dateStr;
        }

        public int getPreCount() {
            return preCount;
        }

        public void setPreCount(int preCount) {
            this.preCount = preCount;
        }

        @Override
        public String toString() {
            return "DbValue{" +
                    "txId=" + txId +
                    ", count=" + count +
                    ", dateStr='" + dateStr + '\'' +
                    ", preCount=" + preCount +
                    '}';
        }
    }
}
