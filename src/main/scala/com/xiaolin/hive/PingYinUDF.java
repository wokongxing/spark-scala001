package xiaolin.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import xiaolin.java.PinYin4jUtils;

public class PingYinUDF extends UDF {
    public String evaluate(String input) {
            if (input.isEmpty()){
                return "";
            }else {
                return PinYin4jUtils.getHeadByString(input);
            }
    }

}
