package com.xiaolin.algorithm.array;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @program: spark-scala001 LeetCode 139 单词拆分
 * @description: 给定一个非空字符串 s 和一个包含非空单词的列表 wordDict，判定 s 是否可以被空格拆分为一个或多个在字典中出现的单词。
 *  说明：
 *      拆分时可以重复使用字典中的单词。
 *      你可以假设字典中没有重复的单词。
 *  示例 1：
 *      输入: s = "leetcode", wordDict = ["leet", "code"]
 *      输出: true
 *      解释: 返回 true 因为 "leetcode" 可以被拆分成 "leet code"。
 *
 * 示例 2：
 *      输入: s = "applepenapple", wordDict = ["apple", "pen"]
 *      输出: true
 *      解释: 返回 true 因为 "applepenapple" 可以被拆分成 "apple pen apple"。
 *              注意你可以重复使用字典中的单词。
 * 示例 3：
 *      输入: s = "catsandog", wordDict = ["cats", "dog", "sand", "and", "cat"]
 *      输出: false

 * @author: linzy
 * @create: 2021-03-29 16:47
 **/
public class WordBreak {

    public static void main(String[] args) {
        String a ="catsandog";
       List<String> list = new ArrayList<String>();
        list.add("cats");
        list.add("dog");
        list.add("san");
        list.add("and");
        list.add("cat");

        System.out.println(wordBreak(a,list));
    }

    public static  boolean wordBreak(String s, List<String> wordDict) {
        Set<String> wordDictSet = new HashSet(wordDict);
        boolean[] dp = new boolean[s.length() + 1];
        dp[0] = true;
        for (int i = 1; i <= s.length(); i++) {
            for (int j = 0; j < i; j++) {
                //判断 j 之前的 字符串 是否可以被拆分
                if (dp[j] && wordDictSet.contains(s.substring(j, i))) {
                    dp[i] = true;
                    break;
                }
            }
        }
        //返回
        return dp[s.length()];
    }

}
