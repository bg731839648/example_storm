package storm.lesson.partitionTransactional.spout;


import com.alibaba.fastjson.JSON;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import storm.lesson.transactional.spout.mata.MyMata;

import java.util.HashMap;
import java.util.Map;

public class MyPtTxSpout implements IPartitionedTransactionalSpout<MyMata> {

    private static final long serialVersionUID = 9159559796647862398L;

    private static final int BATCH_NUM = 5;

    private Map<Integer, Map<Long, String>> PT_DATA_MP
            = new HashMap<Integer, Map<Long, String>>();


    String[] sentences = new String[]{
            "www.jd.com 11111111111111111 2019-06-03 8:45:12",
            "www.jd.com 22222222222222222 2019-06-03 9:45:12",
            "www.jd.com 33333333333333333 2019-06-03 10:45:12",
            "www.jd.com 44444444444444444 2019-06-03 11:45:12",
            "www.jd.com 55555555555555555 2019-06-03 12:45:12",
            "www.jd.com 66666666666666666 2019-06-03 13:45:12",
            "www.jd.com 77777777777777777 2019-06-03 14:45:12",
            "www.jd.com 88888888888888888 2019-06-03 15:45:12",
            "www.jd.com 99999999999999999 2019-06-03 16:45:12",
            "www.jd.com 00000000000000000 2019-06-03 17:45:12",
            "www.jd.com 11111111111111111 2019-06-03 8:45:12",
            "www.jd.com 22222222222222222 2019-06-03 9:45:12",
            "www.jd.com 33333333333333333 2019-06-03 10:45:12",
            "www.jd.com 44444444444444444 2019-06-03 11:45:12",
            "www.jd.com 55555555555555555 2019-06-03 12:45:12",
            "www.jd.com 66666666666666666 2019-06-03 13:45:12",
            "www.jd.com 77777777777777777 2019-06-03 14:45:12",
            "www.jd.com 88888888888888888 2019-06-03 15:45:12",
            "www.jd.com 99999999999999999 2019-06-03 16:45:12",
            "www.jd.com 00000000000000000 2019-06-03 17:45:12",
            "www.jd.com 11111111111111111 2019-06-03 8:45:12",
            "www.jd.com 22222222222222222 2019-06-03 9:45:12",
            "www.jd.com 33333333333333333 2019-06-03 10:45:12",
            "www.jd.com 44444444444444444 2019-06-03 11:45:12",
            "www.jd.com 55555555555555555 2019-06-03 12:45:12",
            "www.jd.com 66666666666666666 2019-06-03 13:45:12",
            "www.jd.com 77777777777777777 2019-06-03 14:45:12",
            "www.jd.com 88888888888888888 2019-06-03 15:45:12",
            "www.jd.com 99999999999999999 2019-06-03 16:45:12",
            "www.jd.com 00000000000000000 2019-06-03 17:45:12",
            "www.jd.com 11111111111111111 2019-06-03 8:45:12",
            "www.jd.com 22222222222222222 2019-06-03 9:45:12",
            "www.jd.com 33333333333333333 2019-06-03 10:45:12",
            "www.jd.com 44444444444444444 2019-06-03 11:45:12",
            "www.jd.com 55555555555555555 2019-06-03 12:45:12",
            "www.jd.com 66666666666666666 2019-06-03 13:45:12",
            "www.jd.com 77777777777777777 2019-06-03 14:45:12",
            "www.jd.com 88888888888888888 2019-06-03 15:45:12",
            "www.jd.com 99999999999999999 2019-06-03 16:45:12",
            "www.jd.com 00000000000000000 2019-06-03 17:45:12"};

    public MyPtTxSpout() {

        Map<Long, String> dbMap;
        for (int j = 0; j < 5; j++) {
            dbMap = new HashMap<Long, String>();
            for (int i = 0; i < sentences.length; i++) {
                dbMap.put(Long.valueOf(i), sentences[i]);
            }
            PT_DATA_MP.put(j, dbMap);
        }
    }

    @Override
    public Coordinator getCoordinator(Map map, TopologyContext topologyContext) {
        return new MyCoordinator();
    }

    @Override
    public Emitter<MyMata> getEmitter(Map map, TopologyContext topologyContext) {
        return new MyEmitter();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tx", "log"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public class MyCoordinator implements IPartitionedTransactionalSpout.Coordinator {

        @Override
        public int numPartitions() {
            return 5;
        }

        @Override
        public boolean isReady() {
            Utils.sleep(1000);
            return true;
        }

        @Override
        public void close() {

        }
    }

    public class MyEmitter implements IPartitionedTransactionalSpout.Emitter<MyMata> {

        @Override
        public MyMata emitPartitionBatchNew(TransactionAttempt transactionAttempt,
                                            BatchOutputCollector batchOutputCollector,
                                            int partition,
                                            MyMata lastMyMata) {

            long beginPoint;

            if (lastMyMata == null) {
                beginPoint = 0L;
            } else {
                beginPoint = lastMyMata.getBeginPoint() + lastMyMata.getNum();
            }

            MyMata mata = new MyMata();
            mata.setBeginPoint(beginPoint);
            mata.setNum(BATCH_NUM);

            emitPartitionBatch(transactionAttempt, batchOutputCollector, partition, mata);

            System.err.println("启动一个事务 mata: " + JSON.toJSONString(mata));

            return mata;
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt transactionAttempt,
                                       BatchOutputCollector batchOutputCollector,
                                       int partition,
                                       MyMata lastMyMata) {

            System.err.println("emitPartitionBatch partition:" + partition);
            long beginPoint = lastMyMata.getBeginPoint();
            int num = lastMyMata.getNum();

            Map<Long, String> batchMap = PT_DATA_MP.get(partition);
            for (long i = beginPoint; i < num + beginPoint; i++) {

                if (batchMap.get(i) != null) {
                    batchOutputCollector.emit(new Values(transactionAttempt, batchMap.get(i)));
                }
            }
        }

        @Override
        public void close() {

        }
    }
}
