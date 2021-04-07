package com.xiaolin.algorithm.array;

import java.util.HashMap;

/**
 * @program:  leetcode 136
 * @description: 给定一个非空整数数组，除了某个元素只出现一次以外，其余每个元素均出现两次。找出那个只出现了一次的元素。
 * 1.任何数和0异或，结果仍然时其原来的数       5^0:----5
 * 2.任何数和其自身做异或，结果是0            1^1:----0
 * 3.异或运算满足交换律和结合律
 *          5^0^1^1^5^3^2^6^2^3:----6
 * @author: linzy
 * @create: 2020-09-22 17:28
 **/
public class SingleNumber {

    public static void main(String[] args) {

        int result = singleNumber2(new int[]{1,2,3,4,5,6,7,6,5,4,3,2,1});
        System.out.println(result);
        System.out.println("1^1:----"+ (1^1));
        System.out.println("5^0:----"+ (5^0));
        System.out.println("5^0^1^1^5^3^2^6^2^3:----"+ (5^0^1^1^5^3^2^6^2^3));
    }

    public static int singleNumber(int[] nums){
        int result = nums[0];
        for(int i = 1;i < nums.length; i++){
            // 1 ^ 1 = 0; 1 ^ 0 = 1;
            result = result ^ nums[i];
        }
        return result;

    }

    public static int singleNumber2(int[] nums) {

        HashMap<Integer, Integer> map = new HashMap<>();
        HashMap<Integer, Integer> map2 = new HashMap<>();
        //添加
        for (int i=0;i<nums.length;i++){
            if (map.containsKey(nums[i])){
                map.put(Integer.valueOf(nums[i]),map.get(nums[i])+1);
            }else{
                map.put(Integer.valueOf(nums[i]),1);
            }
        }

        //取值
        map.forEach((key,value)->{
            map2.put(value,key);
        });
        return map2.get(1).intValue();
    }

}
