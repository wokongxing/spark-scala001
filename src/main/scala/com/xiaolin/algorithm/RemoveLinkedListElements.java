package com.xiaolin.algorithm;

/**
 * @program: spark-scala001
 * @description: LeetCode上第203号问题：Remove Linked List Elements
 *          删除链表中等于给定值 val 的所有节点。
 *      示例:
 *          输入: 1->2->6->3->4->5->6, val = 6
 *          输出: 1->2->3->4->5
 * @author: linzy
 * @create: 2020-09-11 11:12
 **/
public class RemoveLinkedListElements {
    public static void main(String[] args) {
        ListNode listNode = new ListNode(1);
        listNode.next = new ListNode(2);
        listNode.next.next = new ListNode(6);
        listNode.next.next.next = new ListNode(3);
        listNode.next.next.next.next = new ListNode(4);
        listNode.next.next.next.next.next = new ListNode(5);
        listNode.next.next.next.next.next.next = new ListNode(6);

        ListNode listNode2 =  removeElements(listNode,6);
    }
    //解题:
    public static ListNode removeElements(ListNode head, int val) {
        if (head==null){
            return null;
        }
       //如 head.val == val 则跳过当前head对象 赋值下一个对象 即head.next
        head.next = removeElements(head.next, val);
        return  head.val == val?head.next:head;

    }

    public static class ListNode{
        int val;
        ListNode next;

        public ListNode(int x){
            val=x;
        }
    }

}
