package org.embulk.input.sftp;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.embulk.spi.Exec;
import org.embulk.spi.util.InputStreamFileInput;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;

public class SingleFileProvider
        implements InputStreamFileInput.Provider
{
    private final StandardFileSystemManager manager;
    private final FileSystemOptions fsOptions;
    private final String key;
    private final int maxConnectionRetry;
    private boolean opened = false;
    private final Logger log = Exec.getLogger(SingleFileProvider.class);

    public SingleFileProvider(PluginTask task, int taskIndex, StandardFileSystemManager manager, FileSystemOptions fsOptions)
    {
        this.manager = manager;
        this.fsOptions = fsOptions;
        this.key = task.getFiles().get(taskIndex);
        this.maxConnectionRetry = task.getMaxConnectionRetry();
    }

    @Override
    public InputStream openNext() throws IOException
    {
        if (opened) {
            return null;
        }
        opened = true;

        int count = 0;
        while (true) {
            try {
                FileObject file = manager.resolveFile(key, fsOptions);
                log.info("Starting to download file {}", key);

                return file.getContent().getInputStream();
            }
            catch (FileSystemException ex) {
                if (++count == maxConnectionRetry || ex.getMessage().indexOf("Permission denied") > 0) {
                    throw ex;
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
        }
    }

    @Override
    public void close()
    {
    }
}
