package org.embulk.input.sftp;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;

import java.util.List;

public class SftpFileInputPlugin
        implements FileInputPlugin
{
    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final PluginTask task = configMapper.map(config, PluginTask.class);
        SftpFileInput.validateHost(task);

        // list files recursively
        task.setFiles(SftpFileInput.listFilesByPrefix(task));
        // number of processors is same with number of files
        return resume(task.toTaskSource(), task.getFiles().getTaskCount(), control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
        String lastPath = null;
        if (task.getIncremental()) {
            lastPath = SftpFileInput.getRelativePath(task, task.getFiles().getLastPath(task.getLastPath()));
        }
        control.run(taskSource, taskCount);

        ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();
        if (task.getIncremental() && lastPath != null) {
            configDiff.set("last_path", lastPath);
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
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
        return new SftpFileInput(task, taskIndex);
    }

    static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
}
