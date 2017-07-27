import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * Created by lmz on 2017/7/26.
 */
public class TransForm {
    public static void main(String[] args) throws UnsupportedEncodingException {
        String str = "你这个小傻逼";
        String chinese = URLEncoder.encode(str, "utf-8");
        System.out.println(chinese);
    }
}
