package storm.lesson.partitionTransactionalOpaque.bolt;


import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.coordination.IBatchBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class MyOpaquePtTxBolt implements IBatchBolt<TransactionAttempt> {

    private static final long serialVersionUID = -2325340764796298952L;
    private Map<String, Integer> countMap = new HashMap<String, Integer>();
    private BatchOutputCollector collector;
    private Integer count = null;
    private String today = "2019-06-03";
    private TransactionAttempt tx = null;

    @Override
    public void prepare(Map map,
                        TopologyContext topologyContext,
                        BatchOutputCollector collector,
                        TransactionAttempt tx) {
//        System.out.println("执行 MyOpaquePtTxBolt.prepare");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
//        System.out.println("执行 MyOpaquePtTxBolt.execute");

        String log = tuple.getString(1);
        tx = (TransactionAttempt) tuple.getValue(0);
        if (log != null && !"".equals(log) && log.split(" ").length >= 3) {
            String tupleToday = log.split(" ")[2];
            if (today.equals(tupleToday)) {
                count = countMap.get(today);
                if (count == null) {
                    count = 0;
                }
                count++;
                countMap.put(today, count);
            }
        }
    }

    @Override
    public void finishBatch() {
//        System.out.println("执行 MyOpaquePtTxBolt.finishBatch");
        System.err.println(tx + "--" + today + "--" + count);
        collector.emit(new Values(tx, today, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        System.out.println("执行 MyOpaquePtTxBolt.declareOutputFields");
        outputFieldsDeclarer.declare(new Fields("tx", "date", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
//        System.out.println("执行 MyOpaquePtTxBolt.getComponentConfiguration");
        return null;
    }
}
