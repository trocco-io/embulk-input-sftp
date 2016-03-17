package org.embulk.input.sftp;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.InputStreamFileInput;
import org.slf4j.Logger;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class SftpFileInput
        extends InputStreamFileInput
        implements TransactionalFileInput
{
    private static final Logger log = Exec.getLogger(SftpFileInput.class);
    private static boolean isMatchLastKey = false;

    public SftpFileInput(PluginTask task, int taskIndex)
    {
        super(task.getBufferAllocator(), new SingleFileProvider(task, taskIndex, initializeStandardFileSystemManager()));
    }

    public void abort()
    {
    }

    public TaskReport commit()
    {
        return Exec.newTaskReport();
    }

    @Override
    public void close()
    {
    }

    private static StandardFileSystemManager initializeStandardFileSystemManager()
    {
        if (!log.isDebugEnabled()) {
            // TODO: change logging format: org.apache.commons.logging.Log
            System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
        }

        StandardFileSystemManager manager = new StandardFileSystemManager();
        try {
            manager.init();
        }
        catch (FileSystemException ex) {
            Throwables.propagate(ex);
        }

        return manager;
    }

    private static String initializeUserInfo(PluginTask task)
    {
        String userInfo = task.getUser();
        if (task.getPassword().isPresent()) {
            userInfo += ":" + task.getPassword().get();
        }
        return userInfo;
    }

    public static FileSystemOptions initializeFsOptions(PluginTask task)
    {
        FileSystemOptions fsOptions = new FileSystemOptions();

        try {
            SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
            builder.setUserDirIsRoot(fsOptions, task.getUserDirIsRoot());
            builder.setTimeout(fsOptions, task.getSftpConnectionTimeout());
            builder.setStrictHostKeyChecking(fsOptions, "no");

            if (task.getSecretKeyFile().isPresent()) {
                IdentityInfo identityInfo = new IdentityInfo(
                        new File((task.getSecretKeyFile().transform(localFileToPathString()).get())),
                        task.getSecretKeyPassphrase().getBytes()
                );
                builder.setIdentityInfo(fsOptions, identityInfo);
                log.info("set identity: {}", task.getSecretKeyFile().get());
            }

            if (task.getProxy().isPresent()) {
                ProxyTask proxy = task.getProxy().get();

                ProxyTask.ProxyType.setProxyType(builder, fsOptions, proxy.getType());

                if (proxy.getHost().isPresent()) {
                    builder.setProxyHost(fsOptions, proxy.getHost().get());
                    builder.setProxyPort(fsOptions, proxy.getPort());
                }

                if (proxy.getUser().isPresent()) {
                    builder.setProxyUser(fsOptions, proxy.getUser().get());
                }

                if (proxy.getPassword().isPresent()) {
                    builder.setProxyPassword(fsOptions, proxy.getPassword().get());
                }

                if (proxy.getCommand().isPresent()) {
                    builder.setProxyCommand(fsOptions, proxy.getCommand().get());
                }
            }
        }
        catch (FileSystemException ex) {
            Throwables.propagate(ex);
        }

        return fsOptions;
    }

    public static String getSftpFileUri(PluginTask task, String path)
    {
        try {
            return new URI("sftp", initializeUserInfo(task), task.getHost(), task.getPort(), path, null, null).toString();
        }
        catch (URISyntaxException ex) {
            throw new ConfigException(ex);
        }
    }

    public static List<String> listFilesByPrefix(PluginTask task)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        int maxConnectionRetry = task.getMaxConnectionRetry();
        String lastKey = null;

        StandardFileSystemManager manager = null;
        int count = 0;
        while (true) {
            try {
                manager = initializeStandardFileSystemManager();
                FileSystemOptions fsOptions = initializeFsOptions(task);

                if (task.getLastPath().isPresent() && !task.getLastPath().get().isEmpty()) {
                    lastKey = manager.resolveFile(getSftpFileUri(task, task.getLastPath().get()), fsOptions).toString();
                }

                FileObject files = manager.resolveFile(getSftpFileUri(task, task.getPathPrefix()), fsOptions);
                String basename = FilenameUtils.getBaseName(task.getPathPrefix());
                if (files.isFolder()) {
                    for (FileObject f : files.getChildren()) {
                        if (f.isFile()) {
                            addFileToList(builder, f.toString(), "", lastKey);
                        }
                    }
                }
                else {
                    FileObject parent = files.getParent();
                    for (FileObject f : parent.getChildren()) {
                        if (f.isFile()) {
                            addFileToList(builder, f.toString(), basename, lastKey);
                        }
                    }
                }
                return builder.build();
            }
            catch (FileSystemException ex) {
                if (++count == maxConnectionRetry) {
                    Throwables.propagate(ex);
                }
                log.warn("failed to connect sftp server: " + ex.getMessage(), ex);

                try {
                    long sleepTime = ((long) Math.pow(2, count) * 1000);
                    log.warn("sleep in next connection retry: {} milliseconds", sleepTime);
                    Thread.sleep(sleepTime); // milliseconds
                }
                catch (InterruptedException ex2) {
                    // Ignore this exception because this exception is just about `sleep`.
                    log.warn(ex2.getMessage(), ex2);
                }
                log.warn("retrying to connect sftp server: " + count + " times");
            }
            finally {
                if (manager != null) {
                    manager.close();
                }
            }
        }
    }

    private static void addFileToList(ImmutableList.Builder<String> builder, String fileName, String basename, String lastKey)
    {
        if (!basename.isEmpty()) {
            String remoteBasename = FilenameUtils.getBaseName(fileName);
            if (remoteBasename.startsWith(basename)) {
                if (lastKey != null && !isMatchLastKey) {
                    if (!fileName.equals(lastKey)) {
                        return;
                    }
                    else {
                        isMatchLastKey = true;
                    }
                }
                builder.add(fileName);
                log.info("add file to the request list: {}", fileName);
            }
        }
        else {
            if (lastKey != null && !isMatchLastKey) {
                if (!fileName.equals(lastKey)) {
                    return;
                }
                else {
                    isMatchLastKey = true;
                }
            }
            builder.add(fileName);
            log.info("add file to the request list: {}", fileName);
        }
    }

    private static Function<LocalFile, String> localFileToPathString()
    {
        return new Function<LocalFile, String>()
        {
            public String apply(LocalFile file)
            {
                return file.getPath().toString();
            }
        };
    }
}
