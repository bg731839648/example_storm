package storm.lesson.trident.spout;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.lesson.transactional.spout.mata.MyMata;

import java.util.Map;

public class MyTridentSpout implements ITridentSpout<MyMata> {

    private static final long serialVersionUID = 1472463241798439927L;

    @Override
    public BatchCoordinator<MyMata> getCoordinator(String s, Map map, TopologyContext topologyContext) {
        return new MyBatchCoordinator();
    }

    @Override
    public Emitter<MyMata> getEmitter(String s, Map map, TopologyContext topologyContext) {
        return new MyEmitter();
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return null;
    }

    public class MyBatchCoordinator implements BatchCoordinator<MyMata> {

        @Override
        public MyMata initializeTransaction(long l, MyMata myMata, MyMata x1) {
            return null;
        }

        @Override
        public void success(long l) {

        }

        @Override
        public boolean isReady(long l) {
            return true;
        }

        @Override
        public void close() {

        }
    }

    public class MyEmitter implements Emitter<MyMata> {

        @Override
        public void emitBatch(TransactionAttempt transactionAttempt, MyMata myMata, TridentCollector collector) {
            for (int i = 0; i < 100; i++) {
                collector.emit(new Values(i, "www.jd.com 11111111111111111 2019-06-03 8:45:12"));
            }
        }

        @Override
        public void success(TransactionAttempt transactionAttempt) {

        }

        @Override
        public void close() {

        }
    }
}
