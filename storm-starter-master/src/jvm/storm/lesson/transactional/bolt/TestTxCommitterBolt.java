package storm.lesson.transactional.bolt;


import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Tuple;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class TestTxCommitterBolt extends BaseTransactionalBolt implements ICommitter {

    private static final long serialVersionUID = -3353337978838911701L;

    private static final String GLOBAL_KEY = "GLOBAL_KEY";

    private static Map<String, DbValue> dbMap = new HashMap<String, DbValue>();

    private TransactionAttempt tx;

    private int sum = 0;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector collector, TransactionAttempt tx) {
        this.tx = tx;
    }

    @Override
    public void execute(Tuple tuple) {
        sum += tuple.getInteger(1);
    }

    @Override
    public void finishBatch() {

        DbValue value = dbMap.get(GLOBAL_KEY);
        DbValue newValue;
        if (value == null || !value.txId.equals(tx.getTransactionId())) {

            // 更新數據庫
            newValue = new DbValue();
            newValue.txId = tx.getTransactionId();

            if (value == null) {
                newValue.count = sum;
            } else {
                newValue.count = value.count + sum;
            }

            dbMap.put(GLOBAL_KEY, newValue);
        }
        System.err.println("total===========================: " + dbMap.get(GLOBAL_KEY).count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private static class DbValue {
        BigInteger txId;
        int count = 0;
    }

}
