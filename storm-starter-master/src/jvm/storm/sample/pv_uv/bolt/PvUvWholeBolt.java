package storm.sample.pv_uv.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.zookeeper.*;
import storm.common.constant.ZookeeperConstant;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Storm Bolt 汇总收集
 *
 * @author sunjiaxin
 * @date 2019-06-12 14:21
 */
public class PvUvWholeBolt implements IRichBolt {

    /**
     * 序列化
     */
    private static final long serialVersionUID = 5533689750126813026L;

    /**
     * lock 临时节点
     */
    private static final String zkPath = "/lock/pv";

    /**
     * 发射器
     */
    private OutputCollector collector;

    /**
     * 请求次数map
     */
    private Map<String, Long> counts = new HashMap<String, Long>();

    /**
     * zooKeeper
     */
    private ZooKeeper zooKeeper = null;

    /**
     * lockData
     */
    private String lockData = null;

    /**
     * lock beginTime
     */
    private long beginTime = System.currentTimeMillis();

    /**
     * lock endTime
     */
    private long endTime = 0L;

    @Override
    public void prepare(Map map,
                        TopologyContext topologyContext,
                        OutputCollector collector) {
        this.collector = collector;
        System.out.println("========链接zookeeper===========");

        try {
            zooKeeper = new ZooKeeper(ZookeeperConstant.TEST_ADDRESS,
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

        long pv = 0L;
        long uv = 0L;

        try {
            endTime = System.currentTimeMillis();
            String sessionId = tuple.getString(0);
            Long count = tuple.getLong(1);

            counts.put(sessionId, count);

            // 获取总数,遍历counts 的values. 进行sum
            Iterator<Long> iteratorI = counts.values().iterator();
            while (iteratorI.hasNext()) {
                pv += iteratorI.next();
            }

            // 获取work去重个数,遍历counts 的keySet,取count
            Iterator<String> iteratorS = counts.keySet().iterator();
            while (iteratorS.hasNext()) {
                String next = iteratorS.next();
                if (next != null) {
                    uv++;
                }
            }
            if (endTime - beginTime >= 3000) {
                String zookeeperLockData = new String(zooKeeper.getData(zkPath, false, null));
                if (lockData.equals(zookeeperLockData)) {
                    System.out.println(Thread.currentThread().getName() + ", pv_all= " + pv + ", uv_all=" + uv + ", time=" + new SimpleDateFormat("yyyy-mm-dd  HH:mm:ss").format(new Date()));
                }
                beginTime = System.currentTimeMillis();
            }

            collector.ack(tuple);
        } catch (Exception e) {

            collector.fail(tuple);
            e.printStackTrace();
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
