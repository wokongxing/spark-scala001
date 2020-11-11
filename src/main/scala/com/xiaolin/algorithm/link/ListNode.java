package com.xiaolin.algorithm.link;

/**
 * @program: spark-scala001
 * @description:
 * @author: linzy
 * @create: 2020-10-26 17:45
 **/
public  class ListNode{
      int val;
      ListNode next;
    public ListNode(int x){
        val=x;
    }
    public ListNode(int val, ListNode next) { this.val = val; this.next = next; }
}
