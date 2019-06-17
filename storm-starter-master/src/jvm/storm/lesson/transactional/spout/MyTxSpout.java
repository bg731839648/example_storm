package storm.lesson.transactional.spout;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.tuple.Fields;
import storm.lesson.transactional.spout.coordinator.MyCoordinator;
import storm.lesson.transactional.spout.eimtter.MyEimtter;
import storm.lesson.transactional.spout.mata.MyMata;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class MyTxSpout implements ITransactionalSpout<MyMata> {

    private static final long serialVersionUID = -302329645603573440L;

    Map<Long, String> dbMap;

    Random random;

    Integer dataCount = 1000;

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
            "www.jd.com 00000000000000000 2019-06-03 17:45:12"};

    public MyTxSpout() {
        random = new Random();
        dbMap = new HashMap<Long, String>();

        int j = 0;
        long k = 0L;

        for (int i = 0; i < dataCount; i++) {
            if (j / 10 == 1) {
                j = 0;
            }
            dbMap.put(++k, sentences[j]);
            j++;
        }
        System.out.println("入参: " + dbMap.size());
    }

    @Override
    public Coordinator<MyMata> getCoordinator(Map map, TopologyContext topologyContext) {
        return new MyCoordinator();
    }

    @Override
    public Emitter<MyMata> getEmitter(Map map, TopologyContext topologyContext) {
        return new MyEimtter(dbMap);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tx", "log"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
