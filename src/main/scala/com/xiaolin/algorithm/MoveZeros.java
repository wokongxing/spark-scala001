package com.xiaolin.algorithm;


/**
 * @program: spark-scala001
 * @description: leetcode上第283号问题：Move Zeros
 *          给定一个数组nums，写一个函数，将数组中所有的0挪到数组的末尾，⽽维持其他所有非0元素的相对位置。
 *          举例: nums = [0, 1, 0, 3, 12]，函数运⾏后结果为[1, 3, 12, 0, 0]
 * @author: linzy
 * @create: 2021-02-03 10:21
 **/
public class MoveZeros {
    public static void main(String[] args) {
        int nums[] = {1,2,0,0,3,0,4,0,5};


        moveZeros(nums);

    }

    /**
     * 解题思路:  设定一个临时变量k=0，遍历数组nums，将非零元素移动到nums[k]位置，同时k++，而后将【k,….nums.size()】中的元素置零
     * @param nums
     */
    public static  void moveZeros(int[] nums){

        int k=0;

        for (int i=0;i< nums.length;i++ ){

            if (nums[i]!=0){
                nums[k]=nums[i];
                k++;
            }
        }


        for (int i=k;i<nums.length;i++){
            nums[i]=0;
        }



    }
}
