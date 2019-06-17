package storm.lesson.transactional.bolt;


import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class TestTxBolt extends BaseTransactionalBolt {

    private static final long serialVersionUID = 6724626815993169937L;

    Integer count = 0;

    BatchOutputCollector collector;

    TransactionAttempt tx;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector collector, TransactionAttempt tx) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        tx = (TransactionAttempt) tuple.getValue(0);
        String log = tuple.getString(1);
        System.err.println("# " + Thread.currentThread().getName() + " ,tx-TransactionId: " + tx.getTransactionId() + " ,tx-AttemptId():" + tx.getAttemptId() + " ,log: " + log);
        if (log != null) {
            count++;
        }
    }

    @Override
    public void finishBatch() {
        // System.err.println("finishBatch: " + count);
        collector.emit(new Values(tx, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tx", "count"));
    }
}
