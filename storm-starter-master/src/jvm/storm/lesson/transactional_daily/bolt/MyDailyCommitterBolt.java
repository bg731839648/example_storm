package storm.lesson.transactional_daily.bolt;


import com.alibaba.fastjson.JSON;
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

public class MyDailyCommitterBolt extends BaseTransactionalBolt implements ICommitter {

    private static final long serialVersionUID = -2524702063291167257L;

    private static final String GLOBAL_KEY = "GLOBAL_KEY";
    private static Map<String, DbValue> dbMap = new HashMap<String, DbValue>();
    private static Map<String, Integer> countMap = new HashMap<String, Integer>();

    private TransactionAttempt tx;
    private String today = "2019-06-03";

    @Override

    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector collector, TransactionAttempt tx) {
        this.tx = tx;
    }

    @Override
    public void execute(Tuple tuple) {
        System.err.println("tuple: " + JSON.toJSONString(tuple));
        String tupleToday = tuple.getString(1);
        Integer count = tuple.getInteger(2);

        if (tupleToday != null && count != null) {
            // 仅处理当天数据
            if (today.equals(tupleToday)){
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
        System.err.println("countMap: " + JSON.toJSONString(countMap));
        System.err.println("dbMap: " + JSON.toJSONString(dbMap));
        System.out.println("\t");

        if (countMap.size() > 0) {
            DbValue value = dbMap.get(GLOBAL_KEY);
            DbValue newValue;

            if (value == null || !value.txId.equals(tx.getTransactionId())) {

                // 更新數據庫
                newValue = new DbValue();
                newValue.txId = tx.getTransactionId();
                newValue.dateStr = today;

                if (today != null){
                    newValue.count = countMap.get(today);
                    dbMap.put(GLOBAL_KEY, newValue);
                }
                System.err.println("total======================:" + dbMap.get(GLOBAL_KEY).count);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public static class DbValue implements Serializable {
        private static final long serialVersionUID = 5010991727124701731L;
        BigInteger txId;
        int count = 0;
        String dateStr;

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

        @Override
        public String toString() {
            return "DbValue{" +
                    "txId=" + txId +
                    ", count=" + count +
                    ", dateStr='" + dateStr + '\'' +
                    '}';
        }
    }

}
