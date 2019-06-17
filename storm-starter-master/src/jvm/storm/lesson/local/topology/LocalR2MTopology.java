package storm.lesson.local.topology;


import com.wangyin.rediscluster.experimental.storm.R2mClusterInfo;
import com.wangyin.rediscluster.experimental.storm.R2mZkWatcherProvider;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import storm.lesson.local.blot.LocalBolt;
import storm.lesson.local.blot.LocalR2MBolt;
import storm.lesson.local.spout.LocalSpout;

public class LocalR2MTopology {

    public static void main(String[] args) {

        String APPID = "jedis_test";
        String ZKSERVER = "172.25.46.201:2181,172.25.46.221:2181,172.25.46.241:2181";

        R2mZkWatcherProvider zkProvider = new R2mZkWatcherProvider(APPID, ZKSERVER);
        R2mClusterInfo r2mClusterInfo = zkProvider.getR2mClusterInfo();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new LocalSpout(), 1);
        builder.setBolt("bolt", new LocalBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("r2mBolt", new LocalR2MBolt(r2mClusterInfo),1).fieldsGrouping("bolt", new Fields("sessionId"));


        Config config = new Config();
        config.put(Config.TOPOLOGY_WORKERS, 1);
        config.setDebug(false);


        try {
            if (args != null && args.length > 0) {
                // 集群提交模式
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } else {
                // 本地提交模式
                LocalCluster localCluster = new LocalCluster();
                localCluster.submitTopology("topology", config, builder.createTopology());

                Thread.sleep(10000);
                localCluster.shutdown();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
