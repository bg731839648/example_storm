package storm.r2m;


import com.wangyin.r2m.client.jedis.JedisPoolConfig;
import com.wangyin.rediscluster.client.CacheClusterClient;
import com.wangyin.rediscluster.client.OriginalCacheClusterClient;
import com.wangyin.rediscluster.provider.ZkProvider;


/**
 * 无Spring集成方案, 直接使用OriginalCacheClusterClient
 * R2m Client component
 * Created by Timothy Zhang on 16-5-18.
 */
public class DemoWithoutSpring {


    public static void main(String[] args) throws Exception {

        String APPID = "jedis_test";
        String ZKSERVER = "172.25.46.201:2181,172.25.46.221:2181,172.25.46.241:2181";


        CacheClusterClient cacheClusterClient = new OriginalCacheClusterClient(
                new ZkProvider(APPID, ZKSERVER, 5000, 5000),
                2000, 5, new JedisPoolConfig()
        );

        String sjx = cacheClusterClient.get("sjx");
        String set = cacheClusterClient.set("sjx", "sjx");
        Long sjx1 = cacheClusterClient.del("sjx");

    }
}
