package com.xiaolin.utils;



import main.scala.com.xiaolin.spark01.entity.IpInfo;
import scala.collection.immutable.List;


public class IpUtil {

    /**
     * 二分查找 ip的信息
     *
     * @param ipInfoList
     * @param ip
     * @return
     */
    public static IpInfo searchIp(List<IpInfo> ipInfoList, String ip) {
        //获取到ip的十进制的值
        Long ipLong = IpConvertToInt(ip);
        //初始化左侧索引
        int leftIndex = 0;
        //初始化右侧索引
        int rightIndex = ipInfoList.size() - 1;
        while (leftIndex <= rightIndex) {
            //计算中间索引
            int mid = (leftIndex + rightIndex) >>> 1;
            //获取到中间的对象
            IpInfo ipInfo = ipInfoList.apply(mid);
            if (ipLong == ipInfo.start()) {
                return ipInfo;
            } else if (ipLong < ipInfo.start()) {
                rightIndex = mid - 1;//计算右侧索引
            } else {
                if (ipLong < ipInfo.end()) {
                    return ipInfo;
                }
                leftIndex = mid + 1;
            }
        }
        return null;
    }

    /**
     * 把ip转换成十进制
     *
     * @param ip
     * @return
     */
    public static Long IpConvertToInt(String ip) {
        String[] ipStr = ip.split("\\.");
        Long result = 0L;
        int j;
        int i;
        for (i = ipStr.length - 1, j = 0; i >= 0; i--, j++) {
            Long temp = Long.parseLong(ipStr[i]);
            temp = temp << (8 * j);
            result = result | temp;
        }
        return result;
    }
}


