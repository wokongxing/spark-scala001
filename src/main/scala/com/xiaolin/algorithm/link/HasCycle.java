package com.xiaolin.algorithm.link;

/**
 * @program:  LeetCode 141. Linked List Cycle LeetCode 142--判断成环起始点
 * @description: 判断链表是否成环,使用双指针 --快慢指针 判断是否成环
 * @author: linzy
 * @create: 2020-10-26 17:40
 **/
public class HasCycle {
    public static void main(String[] args) {
        ListNode listNode = new ListNode(1);
        listNode.next = new ListNode(2);
        listNode.next.next = new ListNode(3);
        listNode.next.next.next = new ListNode(4);
        listNode.next.next.next.next = new ListNode(5);
        listNode.next.next.next.next.next = new ListNode(6);
        listNode.next.next.next.next.next.next = new ListNode(7);
        listNode.next.next.next.next.next.next = new ListNode(8);
        //成环
        listNode.next.next.next.next.next.next =  listNode.next.next ;

        System.out.println(hasLinkCycle(listNode));
    }

    private static boolean hasLinkCycle(ListNode listNode){
        ListNode fast = listNode;
        ListNode slow = listNode;
        ListNode temp = listNode;

        while (fast!=null && fast.next!=null){
            fast = fast.next.next;
            slow = slow.next;
            if (fast == slow){
                // 只有当快慢指针第一次相遇时,慢指针到成环点的位置,与链表头部到成环点的位置距离 一致
                while (temp != slow){
                    temp = temp.next;
                    slow = slow.next;
                    if (temp ==slow){
                        //成环点
                        System.out.println(temp.val);
                    }
                }
                return true;
            }

        }
        return false;
    }

}
