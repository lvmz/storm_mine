import com.jk.storm_stat.util.TransJson;
import net.sf.json.JSONObject;

/**
 * Created by lmz on 2017/7/20.
 */
public class JsonError {
    public static void main(String[] args) {
        String str = "{\"uuid\":\"99da25677493468fa2299a0cdc5c7669\",\"kafkatime\":\"2017-06-09 15:20:53\",\"loghubtime\":\"2017-06-09 15:20:51\",\"loglevel\":\"INFO\",\"createtime\":\"2017-06-09 15:20:51\",\"userid\":\"1535717\",\"username\":\"18660661119\",\"sessionid\":\"bf7bdb305663656c647223955dcb2fef\",\"cid\":\"山东省潍坊市文昌中学（高中）\",\"ctype\":\"1\",\"userip\":null,\"useend\":\"11\",\"eventtype\":\"jk_click\",\"functionname\":\"选择主功能菜单\",\"cururl\":\"登录极课$极课教师\",\"position\":\"山东省潍坊市\",\"equipmentpara\":{\"word\":\"Office07\",\"addIn\":\"1.7.1.0\",\"scanner\":\"Unknown,WIA-HP LJ M436,WIA-HP LJ M436,,43562,usbscan,1008,Hewlett-Packard\",\"brand\":\"Unknown\",\"deviceName\":\"WIA-HP LJ M436\",\"serial\":\"\"},\"content\":\"{\\\"modulename\\\":\\\"主界面操作\\\",\\\"systemversion\\\":\\\"Microsoft Windows 7 家庭普通版  6.1.7601\\\",\\\"versionnum\\\":\\\"1.69.2.0\\\",\\\"networktype\\\":\\\"Internet\\\",\\\"frompage\\\":\\\"登录极课\\\",\\\"mobile\\\":\\\"18660661119\\\",\\\"flag\\\":\\\"0\\\"}\\r\\n\"}\n";
        JSONObject log = JSONObject.fromObject(str);
        JSONObject json = new JSONObject();
        TransJson.flattenJson(log,json);
        json.containsKey("lmz");
        Object position = json.getString("lmz");
//        System.out.println(position);
    }
}
