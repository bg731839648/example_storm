package storm.lesson.wordCount.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MySumBolt implements IBasicBolt {

    private static final long serialVersionUID = 1980366476254132492L;

    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        try {

            long wordSum = 0L;// 总数
            long wordCount = 0L;// 去重后个数

            String wordString = tuple.getString(0);
            Integer countInteger = tuple.getInteger(1);
            counts.put(wordString, countInteger);

            // 获取总数,遍历counts 的values. 进行sum
            Iterator<Integer> iteratorI = counts.values().iterator();
            while (iteratorI.hasNext()) {
                wordSum += iteratorI.next();
            }

            // 获取work去重个数,遍历counts 的keySet,取count
            Iterator<String> iteratorS = counts.keySet().iterator();
            while (iteratorS.hasNext()) {
                String next = iteratorS.next();
                if (next != null) {
                    wordCount++;
                }
            }

            System.err.println(Thread.currentThread().getName() + " wordSum=" + wordSum + " wordCount=" + wordCount);

        } catch (FailedException e) {
            throw new FailedException("sum fail!");
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
