package com.xiaolin.algorithm.array;

import java.util.HashMap;
import java.util.Map;

/**
 * @program: spark-scala001
 * @description: 求两数之和
 *  定一个整数数组 nums 和一个目标值 target，请你在该数组中找出和为目标值的那 两个 整数，并返回他们的数组下标。
 * 　你可以假设每种输入只会对应一个答案。但是，你不能重复利用这个数组中同样的元素。
 * 　　示例：给定 nums = [2, 7, 11, 15], target = 9，因为 nums[0] + nums[1] = 2 + 7 = 9，所以返回 [0, 1]
 * @author: linzy
 * @create: 2021-03-03 14:37
 **/
public class TwoSum {

    public static void main(String[] args) {
        int[] nums={1,3,34,25,18,11,15};
        twoSum(nums,19);
    }

    public static int[] twoSum(int[] nums, int target) {

        Map<Integer, Integer> hashMap = new HashMap();

         for(int i = 0; i < nums.length; i++){
             //计算差值，然后去哈希表中查找，如果存在则返回结果，不存在则把该元素放入哈希表中
             int num = target - nums[i];
            if(hashMap.containsKey(num)){
                System.out.println(i+"-----"+ hashMap.get(num));
                    return new int[] {i, hashMap.get(num)};
                }
                   hashMap.put(nums[i], i);
          }
             throw new IllegalArgumentException("no two sum solution");
        }
}
