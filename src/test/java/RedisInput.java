import backtype.storm.tuple.Values;
import com.jk.storm_stat.util.redisUtil.RedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created by lmz on 2017/7/19.
 */
public class RedisInput {
    public static void main(String[] args) {
        JedisPool jedisPool = RedisPool.getPool();
        Jedis rs = jedisPool.getResource();

        FileReader reader = null;
        try {
            reader = new FileReader("c://cityNameCode.txt");
            BufferedReader br  = new BufferedReader(reader);
            StringBuffer sb= new StringBuffer("");
            String str = null;

            while((str = br.readLine()) != null) {
                sb.append(str+"/n");
                String[] values = str.split(",");
                rs.hset("province_city",values[0],values[1]);
            }
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
