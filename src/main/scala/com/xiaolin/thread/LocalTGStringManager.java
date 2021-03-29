package com.xiaolin.thread;

import org.apache.commons.lang3.Validate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LocalTGStringManager {
    private static Map<Integer, String> taskGroupStringMap =
            new ConcurrentHashMap<Integer, String>();

    public static void registerTaskGroupString(
            int taskGroupId, String String) {
        taskGroupStringMap.put(taskGroupId, String);
    }

    public static void updateTaskGroupString(final int taskGroupId,
                                                    final String String) {
        Validate.isTrue(taskGroupStringMap.containsKey(
                taskGroupId), String.format("taskGroupStringMap中没有注册taskGroupId[%d]的String，" +
                "无法更新该taskGroup的信息", taskGroupId));
        taskGroupStringMap.put(taskGroupId, String);
    }

    public static void clear() {
        taskGroupStringMap.clear();
    }

    public static Map<Integer, String> getTaskGroupStringMap() {
        return taskGroupStringMap;
    }
}