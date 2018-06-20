package com.migu.schedule;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;
import java.util.ArrayList;
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
        // TODO 方法未实现
        return ReturnCodeKeys.E000;
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
            tasks = taskInfoList.stream()
                .sorted((s1, s2) -> (s1.getTaskId() - s2.getTaskId()))
                .collect(Collectors.toList());
        }

        return ReturnCodeKeys.E015;
    }

    private List<Integer> getAllTaskIds()
    {
        List<Integer> tasks = new ArrayList<>();
        if (taskHangupList.size() > 0)
        {
            tasks.addAll(tasks);
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
