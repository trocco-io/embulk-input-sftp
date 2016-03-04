package org.embulk.input.sftp;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SftpFileInputPlugin
        implements FileInputPlugin
{
    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // list files recursively
        task.setFiles(SftpFileInput.listFilesByPrefix(task));
        // number of processors is same with number of files
        return resume(task.dump(), task.getFiles().size(), control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        control.run(taskSource, taskCount);

        ConfigDiff configDiff = Exec.newConfigDiff();

        List<String> files = new ArrayList<String>(task.getFiles());
        if (files.isEmpty()) {
            if (task.getLastPath().isPresent()) {
                configDiff.set("last_path", task.getLastPath().get());
            }
        }
        else {
            Collections.sort(files);
            configDiff.set("last_path", files.get(files.size() - 1));
        }

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new SftpFileInput(task, taskIndex);
    }
}
