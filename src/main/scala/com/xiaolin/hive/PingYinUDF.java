package com.xiaolin.hive;

import com.xiaolin.utils.PinYin4jUtils;
import org.apache.hadoop.hive.ql.exec.UDF;


public class PingYinUDF extends UDF {
    public String evaluate(String input) {
            if (input.isEmpty()){
                return "";
            }else {
                return PinYin4jUtils.getFirstPinYin(input);
            }
    }

}
