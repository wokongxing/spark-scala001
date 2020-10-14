package com.xiaolin.algorithm;

/**
 * @program: spark-scala001
 * @description:
 *      leetcode121
 *      给定一个数组，它的第 i 个元素是一支给定股票第 i 天的价格。
 *      如果你最多只允许完成一笔交易（即买入和卖出一支股票一次），设计一个算法来计算你所能获取的最大利润。
 * @author: linzy
 * @create: 2020-09-28 15:33
 **/
public class BestBuyAndSell {

    public static void main(String[] args) {
        int a[] = new int[]{3,4,1,6,7,8,2,4,1};

        System.out.println(maxProfit(a));
    }

    public static int maxProfit(int[] prices) {
        int len = prices.length;
        if (len < 2) {
            return 0;
        }

        int res = 0;

        // 表示在当前位置之前的最小值，获取数组中 最大值之前的最小值,两者相减则是最大盈利点
        int minVal = prices[0];
        // 注意：这里从 1 开始
        for (int i = 1; i < len; i++) {
            res = Math.max(res, prices[i] - minVal);
            minVal = Math.min(minVal, prices[i]);
        }
        return res;
    }

}
