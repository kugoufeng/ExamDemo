package com.migu.schedule;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/*
 *类名和方法不能修改
 */
public class Schedule
{
    /**
     * 存储服务节点和服务节点上任务列表的map
     */
    private static volatile Map<Integer, List<Integer>> serverTasksMap;

    /**
     * 存储任务和任务消耗资源的map
     */
    private static volatile Map<Integer, Integer> taskConsumptionMap;

    /**
     * 挂起的任务列表
     */
    private static volatile List<Integer> taskHangupList;

    public int init()
    {
        serverTasksMap = new HashMap<>();
        taskConsumptionMap = new HashMap<>();
        taskHangupList = new ArrayList<>();
        return ReturnCodeKeys.E001;
    }

    public int registerNode(int nodeId)
    {
        if (nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }
        synchronized (this)
        {
            List<Integer> integers = serverTasksMap.get(nodeId);
            if (integers == null)
            {
                serverTasksMap.put(nodeId, new ArrayList<>());
                return ReturnCodeKeys.E003;
            }
            return ReturnCodeKeys.E005;
        }
    }

    public int unregisterNode(int nodeId)
    {
        if (nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }
        synchronized (this)
        {
            List<Integer> tasks = serverTasksMap.get(nodeId);
            if (tasks != null)
            {
                serverTasksMap.remove(nodeId);
                if (tasks.size() > 0)
                {
                    taskHangupList.addAll(tasks);
                }
                return ReturnCodeKeys.E006;
            }
            return ReturnCodeKeys.E007;
        }
    }

    public int addTask(int taskId, int consumption)
    {
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;
        }
        synchronized (this)
        {
            List<Integer> taskIds = getAllTaskIds();
            if (taskIds.size() == 0 || !taskIds.contains(taskId))
            {
                taskHangupList.add(taskId);
                taskConsumptionMap.put(taskId, consumption);
                return ReturnCodeKeys.E008;
            }
            return ReturnCodeKeys.E010;
        }
    }

    public int deleteTask(int taskId)
    {
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;
        }
        synchronized (this)
        {
            Integer taskID = new Integer(taskId);
            if (taskHangupList.size() > 0 && taskHangupList.remove(taskID))
            {
                return ReturnCodeKeys.E011;
            }
            if (serverTasksMap.size() > 0)
            {
                for (Entry<Integer, List<Integer>> entry : serverTasksMap.entrySet())
                {
                    if (entry.getValue().size() > 0 && entry.getValue().remove(taskID))
                    {
                        return ReturnCodeKeys.E011;
                    }
                }
            }
            return ReturnCodeKeys.E012;
        }
    }

    public int scheduleTask(int threshold)
    {
        if (threshold <= 0)
        {
            return ReturnCodeKeys.E002;
        }
        if (serverTasksMap.size() == 0)
        {
            return ReturnCodeKeys.E014;
        }
        synchronized (this)
        {
            if (taskHangupList.size() > 0)
            {
                List<Integer> collect =
                    taskHangupList.stream().sorted((s1, s2) -> s1 - s2).collect(Collectors.toList());
                taskHangupList.clear();
                taskHangupList.addAll(collect);
                int freeNodeId = findFreeNodeId();
                serverTasksMap.get(freeNodeId).add(taskHangupList.get(0));
                taskHangupList.remove(0);
                if (taskHangupList.size() > 0)
                {
                    scheduleTask(threshold);
                }
                return ReturnCodeKeys.E013;
            }
        }

        return ReturnCodeKeys.E014;
    }

    public int queryTaskStatus(List<TaskInfo> tasks)
    {
        if (tasks == null)
        {
            return ReturnCodeKeys.E016;
        }
        List<TaskInfo> taskInfoList = new ArrayList<>();
        if (taskHangupList.size() > 0)
        {
            taskHangupList.forEach(s -> taskInfoList.add(new TaskInfo(s, -1)));
        }
        if (serverTasksMap.size() > 0)
        {
            for (Entry<Integer, List<Integer>> entry : serverTasksMap.entrySet())
            {
                Integer serverNode = entry.getKey();
                List<Integer> taskIds = entry.getValue();
                if (taskIds.size() > 0)
                {
                    taskIds.forEach(s -> taskInfoList.add(new TaskInfo(s, serverNode)));
                }
            }
        }
        tasks.clear();
        if (taskInfoList.size() > 0)
        {
            List<TaskInfo> collect = taskInfoList.stream()
                .sorted((s1, s2) -> (s1.getTaskId() - s2.getTaskId()))
                .collect(Collectors.toList());
            collect.forEach( s -> tasks.add(s));
        }

        return ReturnCodeKeys.E015;
    }

    /**
     * 统计服务节点上的资源占用数
     *
     * @return java.util.Map<java.lang.Integer   ,   java.lang.Integer>
     * @author fengjiangtao
     */
    private Map<Integer, Integer> countServerConsumption()
    {
        Map<Integer, Integer> count = new HashMap<>();
        if (serverTasksMap.size() > 0)
        {
            serverTasksMap.forEach((k, v) -> count.put(k, v.stream().mapToInt(s -> taskConsumptionMap.get(s)).sum()));
        }
        return count;
    }

    private int findFreeNodeId()
    {
        Map<Integer, Integer> map = countServerConsumption();
        if (map.size() == 0)
        {
            return -1;
        }
        List<Integer> nodeIds = new ArrayList<>();
        ArrayList<Integer> values = new ArrayList<>(map.values());
        List<Integer> list = values.stream().sorted((s1, s2) -> s1 - s2).collect(Collectors.toList());
        for (Entry<Integer,Integer> entry : map.entrySet())
        {
            if (entry.getValue().intValue() == list.get(0).intValue())
            {
                nodeIds.add(entry.getKey());
            }
        }
        List<Integer> collect = nodeIds.stream().sorted((s1, s2) -> s2 - s1).collect(Collectors.toList());
        return collect.get(0);
    }

    private List<Integer> getAllTaskIds()
    {
        List<Integer> tasks = new ArrayList<>();
        if (taskHangupList.size() > 0)
        {
            tasks.addAll(taskHangupList);
        }
        if (serverTasksMap.size() > 0)
        {
            for (Entry<Integer, List<Integer>> entry : serverTasksMap.entrySet())
            {
                if (entry.getValue().size() > 0)
                {
                    tasks.addAll(entry.getValue());
                }
            }
        }
        return tasks;
    }

}
