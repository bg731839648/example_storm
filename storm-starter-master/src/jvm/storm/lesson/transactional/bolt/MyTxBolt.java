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

public class MyTxBolt extends BaseTransactionalBolt {

    private static final long serialVersionUID = 1061267224211302418L;

    Integer count = 0;

    BatchOutputCollector batchOutputCollector;

    TransactionAttempt tx;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, TransactionAttempt tx) {
        System.err.println("MyTxBolt.prepare TransactionAttempt: "
                + tx.getTransactionId()
                + ", attemptId: "
                + tx.getAttemptId());
        this.batchOutputCollector = batchOutputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        tx = (TransactionAttempt) tuple.getValue(0);
        System.err.println("MyTxBolt.execute TransactionAttempt: "
                + tx.getTransactionId()
                + ", attemptId: "
                + tx.getAttemptId());

        String log = tuple.getString(1);

        if (log != null && log.length() > 0) {
            count++;
        }
    }

    @Override
    public void finishBatch() {
        System.err.println("finishBatch " + count);
        batchOutputCollector.emit(new Values(tx, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tx", "count"));
    }
}
