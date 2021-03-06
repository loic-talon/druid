/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common.actions;

import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskStorage;

import java.io.IOException;

public class LocalTaskActionClient implements TaskActionClient
{
  private final Task task;
  private final TaskStorage storage;
  private final TaskActionToolbox toolbox;

  private static final EmittingLogger log = new EmittingLogger(LocalTaskActionClient.class);

  public LocalTaskActionClient(Task task, TaskStorage storage, TaskActionToolbox toolbox)
  {
    this.task = task;
    this.storage = storage;
    this.toolbox = toolbox;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
  {
    log.info("Performing action for task[%s]: %s", task.getId(), taskAction);

    if (taskAction.isAudited()) {
      // Add audit log
      try {
        storage.addAuditLog(task, taskAction);
      }
      catch (Exception e) {
        final String actionClass = taskAction.getClass().getName();
        log.makeAlert(e, "Failed to record action in audit log")
           .addData("task", task.getId())
           .addData("actionClass", actionClass)
           .emit();
        throw new ISE(e, "Failed to record action [%s] in audit log", actionClass);
      }
    }

    return taskAction.perform(task, toolbox);
  }
}
