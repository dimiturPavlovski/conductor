package com.netflix.conductor.sdk.workflow.def.tasks;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;

/* Workflow exclusive task executed by a worker */
public class ExclusiveTask extends Task<ExclusiveTask> {

    private TaskDef taskDef;

    public ExclusiveTask(String taskDefName, String taskReferenceName) {
        super(taskReferenceName, TaskType.EXCLUSIVE);
        super.name(taskDefName);
    }

    ExclusiveTask(WorkflowTask workflowTask) {
        super(workflowTask);
        this.taskDef = workflowTask.getTaskDefinition();
    }

    public TaskDef getTaskDef() {
        return taskDef;
    }

    public ExclusiveTask setTaskDef(TaskDef taskDef) {
        this.taskDef = taskDef;
        return this;
    }

    @Override
    protected void updateWorkflowTask(WorkflowTask workflowTask) {
        workflowTask.setTaskDefinition(taskDef);
    }
}
