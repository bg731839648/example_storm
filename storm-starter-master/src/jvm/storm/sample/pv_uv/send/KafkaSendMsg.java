package storm.sample.pv_uv.send;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import storm.common.constant.KafkaConstant;

import java.util.Properties;

/**
 * Kafka Send
 *
 * @author sunjiaxin
 * @date 2019-06-12 14:21
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class KafkaSendMsg {

    /**
     * 初始数据
     */
    private static final String[] SENTENCE_LIST = new String[]{
            "www.jd.com p 2019-06-11 8:45:12",
            "www.jd.com o 2019-06-11 9:45:12",
            "www.jd.com n 2019-06-11 10:45:12",
            "www.jd.com m 2019-06-11 11:45:12",
            "www.jd.com l 2019-06-12 12:45:12",
            "www.jd.com k 2019-06-12 13:45:12",
            "www.jd.com j 2019-06-12 14:45:12",
            "www.jd.com i 2019-06-12 15:45:12",
            "www.jd.com h 2019-06-13 16:45:12",
            "www.jd.com g 2019-06-13 17:45:12",
            "www.jd.com f 2019-06-13 18:45:12",
            "www.jd.com e 2019-06-13 19:45:12",
            "www.jd.com d 2019-06-14 20:45:12",
            "www.jd.com c 2019-06-14 21:45:12",
            "www.jd.com b 2019-06-14 22:45:12",
            "www.jd.com a 2019-06-14 23:45:12"};

    /**
     * 主函数
     */
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaConstant.TEST_ADDRESS);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        String data;
        try {
            for (int j = 0; j < 20; j++) {
                for (int i = 0; i < SENTENCE_LIST.length; i++) {

                    data = SENTENCE_LIST[i];
                    System.err.println("待发送数据消息 MSG: " + data);

                    // 不计结果发送数据
                    producer.send(new ProducerRecord("sjx_20190613", "key", data));

                    // 同步反馈结果
                    //Object o = producer.send(new ProducerRecord("sjx_20190613", "key", data)).get();
                    //System.err.println(JSON.toJSONString(o));

                    // 异步反馈结果
                    //producer.send(new ProducerRecord("sjx_20190613", "key", data), new demoProducerCallBack());
                }
                Thread.sleep(1000);
                System.out.println("===>===> 第" + (j + 1) + "批数据 ===>===>");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static class demoProducerCallBack implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
            }
        }
    }
}
