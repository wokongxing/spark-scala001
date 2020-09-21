package com.xiaolin.algorithm;

/**
 * @program: spark-scala001
 * @description: leetcode:326. 3的幂
 * 输入: 27
 * 输出: true
 * 输入: 45
 * 输出: false
 * @author: linzy
 * @create: 2020-09-11 14:29
 **/
public class isPowerOfThree {

    public static void main(String[] args) {
        isPowerOfThree(45);
    }
    //解题:
    public static boolean isPowerOfThree(int n) {
        if(n==1)    return true;
        if(n<=0||n%3!=0)    return false;
        return isPowerOfThree(n/3);
    }

}
