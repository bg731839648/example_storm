package storm.lesson.transactional_daily.bolt;



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

public class MyDailyBatchBolt implements IBatchBolt<TransactionAttempt> {

    private static final long serialVersionUID = -2325340764796298952L;

    private Map<String, Integer> countMap = new HashMap<String, Integer>();
    private Integer count = null;
    private String today = "2019-06-03";

    private TransactionAttempt tx;
    private BatchOutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, TransactionAttempt tx) {
        this.collector = batchOutputCollector;
        this.tx = tx;
    }

    @Override
    public void execute(Tuple tuple) {
        tx = (TransactionAttempt) tuple.getValue(0);
        String log = tuple.getString(1);
//        System.err.println("# " + Thread.currentThread().getName() + " ,tx-TransactionId: " + tx.getTransactionId() + " ,tx-AttemptId():" + tx.getAttemptId() + " ,log: " + log);

        if (log != null) {
            if (log.split(" ").length >= 3) {

                // 取出登陆态中的日期
                String tupleToday = log.split(" ")[2];

                if (today.equals(tupleToday)) {
                    // 获取该日期一个batch中PV的数量
                    count = countMap.get(today);
                    if (count == null) {
                        count = 0;
                    }
                    count++;
                    countMap.put(today, count);
                }
            }
        }
//        System.err.println("countMap: " + JSON.toJSONString(countMap));
    }

    @Override
    public void finishBatch() {
//        System.err.println(tx.getTransactionId() + "--" + tx.getAttemptId() + "--" + today + "--" + count);
        collector.emit(new Values(tx, today, count));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tx", "date", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
