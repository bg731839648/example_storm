package storm.lesson.local.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import storm.lesson.local.blot.LocalBolt;
import storm.lesson.local.spout.LocalSpout;

public class LocalTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        // parallelism_hint 指并发度,执行线程(executer)

        // parallelism_hint:数据源会读取两遍 重复读, MQ无此现象,理由MQ消息消费后不允许消费方再次消费
        builder.setSpout("spout", new LocalSpout(), 1);

        // parallelism_hint: 多线程执行, 同一数据源并行执行
        // *【shuffleGrouping 随机分发 保证负载均衡】
        builder.setBolt("bolt", new LocalBolt(), 2).shuffleGrouping("spout");

        // *【noneGrouping 随机分发 不保证负载均衡】
        // builder.setBolt("bolt", new LocalBolt(), 2).noneGrouping("spout");

        // *【fieldsGrouping 过滤特定字符串达到分组效果, 相同的tuple会发给同一个executer或task  去重|Join】
        // builder.setBolt("bolt", new LocalBolt(), 2).fieldsGrouping("spout", new Fields("log"));

        // 【allGrouping 广播,一对多  每个bolt获得全部的数据】
        // builder.setBolt("bolt", new LocalBolt(), 2).allGrouping("spout");

        // 【allGrouping 全局分组, 把tuple分配给id值最低的那个task】
        // builder.setBolt("bolt", new LocalBolt(), 2).globalGrouping("spout");

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
