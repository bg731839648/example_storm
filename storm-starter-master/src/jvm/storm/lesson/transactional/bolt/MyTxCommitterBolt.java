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

public class MyTxCommitterBolt extends BaseTransactionalBolt implements ICommitter {

    private static final long serialVersionUID = -2524702063291167257L;

    private static final String GLOBAL_KEY = "GLOBAL_KEY";
    private static Map<String, DbValue> dbMap = new HashMap<String, DbValue>();
    private TransactionAttempt id;
    private int sum = 0;
    private BatchOutputCollector batchOutputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, TransactionAttempt transactionAttempt) {
        this.id = transactionAttempt;
        this.batchOutputCollector = batchOutputCollector;
    }


    @Override
    public void execute(Tuple tuple) {
        sum += tuple.getInteger(1);
    }

    @Override
    public void finishBatch() {
        DbValue value = dbMap.get(GLOBAL_KEY);
        DbValue newValue;
        if (value == null || !value.txId.equals(id.getTransactionId())) {

            // 更新數據庫
            newValue = new DbValue();
            newValue.txId = id.getTransactionId();

            if (value == null) {
                newValue.count = sum;
            } else {
                newValue.count = value.count + sum;
            }

            dbMap.put(GLOBAL_KEY, newValue);
        } else {
            newValue = value;
        }

        System.err.println("total===========================: " + dbMap.get(GLOBAL_KEY).count);

        // batchOutputCollector.emit(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public static class DbValue {
        BigInteger txId;
        int count = 0;
    }

}
