/**
 * Created by lmz on 2017/7/21.
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

    public class KafkaProducer extends Thread{
        private Set<String> logSet = new HashSet<>();
        private String topic;

        public KafkaProducer(String topic){
            super();
            this.topic = topic;
        }


        @Override
        public void run() {
            Producer producer = createProducer();
            int i=0;
            try {
                FileReader reader = new FileReader("c://log_temp.txt");
                BufferedReader br = new BufferedReader(reader);
                String str;
                while((str = br.readLine()) != null) {
                    logSet.add(str);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            while(true){
                for (String str:logSet) {
                    producer.send(new KeyedMessage<Integer, String>(topic, str));
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        private Producer createProducer() {
            Properties properties = new Properties();
            properties.put("zookeeper.connect", "dev-node5.jkabc.com:2181,dev-node6.jkabc.com:2181,dev-node7.jkabc.com:2181/kafka08");//声明zk
            properties.put("serializer.class", StringEncoder.class.getName());
            properties.put("metadata.broker.list", "dev-node1:6667");// 声明kafka broker
            return new Producer<Integer, String>(new ProducerConfig(properties));
        }


        public static void main(String[] args) {
            new KafkaProducer("testlv").start();// 使用kafka集群中创建好的主题 test
        }

    }

