package com.xiaolin.algorithm.array;


import java.util.*;

/**
 * @program: spark-scala001
 * @description: leetcode上第350号问题：Intersection of Two Arrays II
 *  给定两个数组，编写一个函数来计算它们的交集。
 *  示例 1:
 *      输入: nums1 = [1,2,2,1], nums2 = [2,2]
 *      输出: [2,2]
 *  示例 2:
 *      输入: nums1 = [4,9,5], nums2 = [9,4,9,8,4]
 *      输出: [4,9]
 * 说明：
 * 输出结果中每个元素出现的次数，应与元素在两个数组中出现的次数一致。
 * 我们可以不考虑输出结果的顺序。
 * @author: linzy
 * @create: 2021-03-29 15:33
 **/
public class IntersectionOfTwoArrays {
    public static void main(String[] args) {
        int[] num1 = {1,2,2,1};
        int[] num2 = {2,2};

        IntersectionOfTwoArrays(num1,num2);

    }

    public static void IntersectionOfTwoArrays(int[] nums1, int[] nums2) {

        Map<Integer, Integer> map = new HashMap();
        int[] result = new int[nums2.length];

        for(int i = 0; i < nums1.length; i++){
            Integer count = map.get(nums1[i]);
            if (count == null){
                map.put(nums1[i],1);
            }else {
                map.put(nums1[i],++count);
            }

        }

        // 2.
        ArrayList<Integer> list = new ArrayList<>();
        for(int num : nums2){
            if(map.containsKey(num)){
                list.add(num);
                map.put(num, map.get(num) - 1);
                if(map.get(num) == 0){
                    map.remove(num);
                }
            }
        }
        // 3. 输出
//        int[] res = new int[list.size()];
        for(int i = 0; i < list.size(); i++){
            System.out.println(list.get(i));
        }


    }
}
