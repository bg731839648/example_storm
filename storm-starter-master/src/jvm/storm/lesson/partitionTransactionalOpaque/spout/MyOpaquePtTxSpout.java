package storm.lesson.partitionTransactionalOpaque.spout;


import com.alibaba.fastjson.JSON;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.lesson.transactional.spout.mata.MyMata;

import java.util.HashMap;
import java.util.Map;

public class MyOpaquePtTxSpout implements IOpaquePartitionedTransactionalSpout<MyMata> {

    private static final long serialVersionUID = -2285559987766933006L;

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

    public MyOpaquePtTxSpout() {
//        System.out.println("执行 MyOpaquePtTxSpout.MyOpaquePtTxSpout");
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
    public Emitter<MyMata> getEmitter(Map map, TopologyContext topologyContext) {
//        System.out.println("执行 MyOpaquePtTxSpout.Emitter");
        return new MyEmitter();
    }

    @Override
    public Coordinator getCoordinator(Map map, TopologyContext topologyContext) {
//        System.out.println("执行 MyOpaquePtTxSpout.Coordinator");
        return new MyCoordinator();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        System.out.println("执行 MyOpaquePtTxSpout.declareOutputFields");
        outputFieldsDeclarer.declare(new Fields("tx", "log"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
//        System.out.println("执行 MyOpaquePtTxSpout.getComponentConfiguration");
        return null;
    }

    public class MyCoordinator implements IOpaquePartitionedTransactionalSpout.Coordinator {

        @Override
        public boolean isReady() {
//            System.out.println("执行 MyOpaquePtTxSpout.MyCoordinator.isReady");
            return true;
        }

        @Override
        public void close() {
//            System.out.println("执行 MyOpaquePtTxSpout.MyCoordinator.close");
        }
    }

    public class MyEmitter implements IOpaquePartitionedTransactionalSpout.Emitter<MyMata> {

        @Override
        public MyMata emitPartitionBatch(TransactionAttempt tx,
                                         BatchOutputCollector collector,
                                         int partition,
                                         MyMata lastPartitionMeta) {
//            System.out.println("执行 MyOpaquePtTxSpout.MyEmitter.emitPartitionBatch");

            System.err.println("emitPartitionBatch partition:" + partition);
            long beginPoint;
            if (lastPartitionMeta == null) {
                beginPoint = 0L;
            } else {
                beginPoint = lastPartitionMeta.getBeginPoint() + lastPartitionMeta.getNum();
            }

            MyMata mata = new MyMata();
            mata.setBeginPoint(beginPoint);
            mata.setNum(BATCH_NUM);
            System.err.println("启动一个事务: " + JSON.toJSONString(mata));

            Map<Long, String> batchMap = PT_DATA_MP.get(partition);
            for (long i = mata.getBeginPoint(); i < mata.getBeginPoint() + mata.getNum(); i++) {
                if (batchMap == null || batchMap.size() <= 0) {
                    break;
                }
                collector.emit(new Values(tx, batchMap.get(i)));
            }

            return mata;
        }

        @Override
        public int numPartitions() {
//            System.out.println("执行 MyOpaquePtTxSpout.MyEmitter.numPartitions");
            return 5;
        }

        @Override
        public void close() {
//            System.out.println("执行 MyOpaquePtTxSpout.MyEmitter.close");
        }
    }

}
