package com.xiaolin.algorithm.link;

/**
 * @program: spark-scala001
 * @description: 给定一个单链表 L：L0→L1→…→Ln-1→Ln，
 *               将其重新排列后变为：L0→Ln→L1→Ln-1→L2→Ln-2→…
 *          示例:
 *               给定链表 1->2->3->4, 重新排列为 1->4->2->3.
 *               给定链表 1->2->3->4->5, 重新排列为 1->5->2->4->3.
 *          解析:
 *          第一步是 找链表的中点，这里我们需要用到快慢指针这一技巧，需要注意的是，我们要根据题目的要求来调节快慢指针的起始位置，这个拿几个例子跑跑大概就能知道。
 *
 *          第二步是 反转链表，一般来说用普通循环实现的话，需要三个指针交替完成。
 *
 *          第三步是 合并链表，这一步相对前两步来说，思考难度会小一点，需要注意的一点是，出了循环，我们仍然要判断。
 * @author: linzy
 * @create: 2020-10-13 16:24
 **/
public class LeetCode143 {
    public static void main(String[] args) {
        ListNode listNode = new ListNode(1);
        listNode.next = new ListNode(2);
        listNode.next.next = new ListNode(3);
        listNode.next.next.next = new ListNode(4);
        listNode.next.next.next.next = new ListNode(5);
        listNode.next.next.next.next.next = new ListNode(6);
        listNode.next.next.next.next.next.next = new ListNode(7);

        reorderList(listNode);
    }

    public static void reorderList(ListNode head) {
        if (head == null || head.next == null) {
            return;
        }

        // 步骤 1: 通过快慢指针找到链表中点
        // 通过调节快慢指针的起始位置，可以保证前半部分的长度大于等于后半部分
        ListNode slow = head, fast = head.next;
        while (fast != null && fast.next != null) {
            slow = slow.next;
            fast = fast.next.next;
        }

        // 步骤 2: 反转后半部分的链表
        // 在反转之前需要的一个操作是将前后半部分断开
        ListNode second = slow.next;
        slow.next = null;
        second = reverseList(second);

        // 步骤 3: 合并前半部分链表以及反转后的后半部分链表
        mergeList(head, second);
    }

    /**
     * 获取后半段链表数据 并且反转
     * 示例 : 123 =>321
     * @param head
     * @return
     */
    private static ListNode reverseList(ListNode head) {

        ListNode prev = null, tmp = null, pointer = head;
        while (pointer != null) {
            tmp = pointer.next;
            pointer.next = prev;
            prev = pointer;
            pointer = tmp;
        }

        return prev;
    }

    /**
     *  合并两个链表数据
     * @param first
     * @param second
     */
    private static void mergeList(ListNode first, ListNode second) {
        ListNode dummy = new ListNode(0);
        ListNode pointer = dummy;

        while (first != null && second != null) {
            pointer.next = first;
            first = first.next;
            pointer.next.next = second;
            second = second.next;
            pointer = pointer.next.next;
        }

        // 因为我们之前找中点的时候保证了前半部分的长度不小于后半部分的长度
        // 因此交叉后，多出来的部分只可能是前半部分，判断前半部分即可
        if (first != null) {
            pointer.next = first;
        }
    }

    public static class ListNode{
        int val;
        ListNode next;
        public ListNode() {}
        public ListNode(int x){
            val=x;
        }
        public ListNode(int val, ListNode next) { this.val = val; this.next = next; }
    }
}
