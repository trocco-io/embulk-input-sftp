/*
 * Copyright 2016 The Embulk project
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

package org.embulk.input.sftp;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;
import org.embulk.util.config.units.LocalFile;

import java.util.Optional;

public interface PluginTask
        extends Task, FileList.Task
{
    @Config("host")
    String getHost();

    @Config("port")
    @ConfigDefault("22")
    int getPort();

    @Config("user")
    String getUser();

    @Config("password")
    @ConfigDefault("null")
    Optional<String> getPassword();

    @Config("secret_key_file")
    @ConfigDefault("null")
    Optional<LocalFile> getSecretKeyFile();
    void setSecretKeyFile(Optional<LocalFile> secretKeyFile);

    @Config("secret_key_passphrase")
    @ConfigDefault("\"\"")
    String getSecretKeyPassphrase();

    @Config("user_directory_is_root")
    @ConfigDefault("true")
    Boolean getUserDirIsRoot();

    @Config("timeout")
    @ConfigDefault("600") // 10 minutes
    int getSftpConnectionTimeout();

    @Config("max_connection_retry")
    @ConfigDefault("5") // 5 times retry to connect sftp server if failed.
    int getMaxConnectionRetry();

    @Config("path_prefix")
    String getPathPrefix();

    @Config("incremental")
    @ConfigDefault("true")
    boolean getIncremental();

    @Config("last_path")
    @ConfigDefault("null")
    Optional<String> getLastPath();

    @Config("proxy")
    @ConfigDefault("null")
    Optional<ProxyTask> getProxy();

    @Config("stop_when_file_not_found")
    @ConfigDefault("false")
    boolean getStopWhenFileNotFound();

    FileList getFiles();
    void setFiles(FileList files);
}
