package storm.lesson.visits.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.zookeeper.*;
import storm.common.constant.ZookeeperConstant;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class ZkBolt implements IRichBolt {

    private static final long serialVersionUID = 5533689750126813026L;

    public static final String zkPath = "/lock/storm/pv";

    private OutputCollector outputCollector;

    private String logString = null;

    private String sessionId = null;

    private ZooKeeper zooKeeper = null;

    private String lockData = null;

    private long pv = 0L;
    private long beginTime = System.currentTimeMillis();
    private long endTime = 0;

    @Override
    public void prepare(Map map,
                        TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        try {
            zooKeeper = new ZooKeeper(ZookeeperConstant.VIRTUAL_MACHINE_ADDRESS,
                    3000,
                    new Watcher() {
                        @Override
                        public void process(WatchedEvent watchedEvent) {
                            System.err.println("event: " + watchedEvent.getType());
                        }
                    });
            while (zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
                Thread.sleep(1000);
            }

            InetAddress address = InetAddress.getLocalHost();
            lockData = address.getHostAddress() + ":" + topologyContext.getThisTaskId();

            if (zooKeeper.exists(zkPath, false) == null) {
                zooKeeper.create(zkPath, lockData.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }

        } catch (Exception e) {
            e.printStackTrace();
            try {
                zooKeeper.close();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {

        try {
            endTime = System.currentTimeMillis();
            logString = tuple.getString(0);
            sessionId = logString.split(" ")[1];

            if (sessionId != null) {
                pv++;
            }

            if (endTime - beginTime >= 3 * 1000) {

                String zookeeperLockData = new String(zooKeeper.getData(zkPath, false, null));
                if (lockData.equals(zookeeperLockData)) {
                    System.out.println(Thread.currentThread().getName()
                            + " sessionId=" + sessionId
                            + " pv=" + pv * 4
                            + " time=" + new SimpleDateFormat("yyyy-mm-dd  HH:mm:ss").format(new Date()));
                }
                beginTime = System.currentTimeMillis();
            }

            outputCollector.ack(tuple);

        } catch (Exception e) {
            outputCollector.fail(tuple);
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("log"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
