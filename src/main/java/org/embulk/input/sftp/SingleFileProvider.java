package org.embulk.input.sftp;

import com.google.common.base.Throwables;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.embulk.spi.Exec;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.Iterator;

public class SingleFileProvider
        implements InputStreamFileInput.Provider
{
    private final StandardFileSystemManager manager;
    private final FileSystemOptions fsOptions;
    private final Iterator<String> iterator;
    private final int maxConnectionRetry;
    private boolean opened = false;
    private final Logger log = Exec.getLogger(SingleFileProvider.class);

    public SingleFileProvider(PluginTask task, int taskIndex, StandardFileSystemManager manager, FileSystemOptions fsOptions)
    {
        this.manager = manager;
        this.fsOptions = fsOptions;
        this.iterator = task.getFiles().get(taskIndex).iterator();
        this.maxConnectionRetry = task.getMaxConnectionRetry();
    }

    @Override
    public InputStream openNext() throws IOException
    {
        if (opened || !iterator.hasNext()) {
            return null;
        }
        opened = true;
        final String key = iterator.next();

        try {
            return retryExecutor()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWait(500)
                    .withMaxRetryWait(30 * 1000)
                    .runInterruptible(new Retryable<InputStream>() {
                        @Override
                        public InputStream call() throws FileSystemException
                        {
                            log.info("Starting to download file {}", key);
                            FileObject file = manager.resolveFile(key, fsOptions);
                            return file.getContent().getInputStream();
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                throws RetryGiveupException
                        {
                            if (exception.getMessage().indexOf("Permission denied") > 0) {
                                log.error("Could not download file due to Permission Denied");
                                throw new RetryGiveupException(exception);
                            }
                            String message = String.format("SFTP GET request failed. Retrying %d/%d after %d seconds. Message: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                            if (retryCount % 3 == 0) {
                                log.warn(message, exception);
                            }
                            else {
                                log.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException)
                                throws RetryGiveupException
                        {
                        }
                    });
        }
        catch (RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
        catch (InterruptedException ex) {
            throw new InterruptedIOException();
        }
    }

    @Override
    public void close()
    {
    }
}
