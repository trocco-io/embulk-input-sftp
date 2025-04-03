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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator;
import org.apache.sshd.server.keyprovider.AbstractGeneratorHostKeyProvider;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.scp.ScpCommandFactory;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.output.file.LocalFileOutputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapperFactory;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class TestSftpFileInputPlugin
{
    private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES;

    static {
        final Properties properties = new Properties();
        properties.setProperty("default_guess_plugins", "gzip,bzip2,json,csv");
        EMBULK_SYSTEM_PROPERTIES = EmbulkSystemProperties.of(properties);
    }

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
            .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
            .registerPlugin(FileInputPlugin.class, "sftp", SftpFileInputPlugin.class)
            .registerPlugin(FileOutputPlugin.class, "file", LocalFileOutputPlugin.class)
            .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
            .build();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Rule
    public TemporaryFolder sshDirFolder = new TemporaryFolder();

    private static Logger log = LoggerFactory.getLogger(TestSftpFileInputPlugin.class);

    private ConfigSource config;
    private SftpFileInputPlugin plugin;
    private SshServer sshServer;

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = SftpFileInputPlugin.CONFIG_MAPPER_FACTORY;

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 20022;
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String REMOTE_DIRECTORY = "/home/username/unittest/";
    private static final String SECRET_KEY_FILE = Resources.getResource("id_rsa").getPath();
    private static final String SECRET_KEY_PASSPHRASE = "SECRET_KEY_PASSPHRASE";
    private static final String PROXY_HOST = "127.0.0.1";
    private static final int PROXY_PORT = 8080;

    @Before
    public void createResources() throws Exception
    {
        // set system property for ssh_dir
        System.setProperty("vfs.sftp.sshdir", sshDirFolder.getRoot().getPath());
        config = config();
        plugin = new SftpFileInputPlugin();
        if (!log.isDebugEnabled()) {
            // TODO: change logging format: org.apache.commons.logging.Log
            System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
        }

        sshServer = createSshServer(HOST, PORT, USERNAME, PASSWORD);
    }

    @After
    public void cleanup() throws InterruptedException
    {
        try {
            sshServer.stop(true);
        }
        catch (Exception ex) {
            log.debug(ex.getMessage(), ex);
        }
    }

    @Test
    public void checkDefaultValues()
    {
        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("host", HOST)
                .set("user", USERNAME)
                .set("password", PASSWORD)
                .set("path_prefix", "")
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        final PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        assertEquals(22, task.getPort());
        assertEquals(true, task.getIncremental());
        assertEquals(true, task.getUserDirIsRoot());
        assertEquals(600, task.getSftpConnectionTimeout());
        assertEquals(5, task.getMaxConnectionRetry());
    }

    @Test
    public void checkDefaultValuesHostIsNull() throws IOException
    {
        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "sftp")
                .set("host", null)
                .set("user", USERNAME)
                .set("password", PASSWORD)
                .set("path_prefix", "")
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        final Path out = embulk.createTempFile("csv");
        try {
            final TestingEmbulk.RunResult result = embulk.runInput(config, out);
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
            assertTrue(ex.getCause().getCause() instanceof JsonMappingException);
            assertTrue(ex.getCause().getCause().getMessage().startsWith("Field 'host' is required but not set."));
            return;
        }
        fail("Expected Exception was not thrown.");
    }

    @Test
    public void checkDefaultValuesUserIsNull() throws IOException
    {
        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "sftp")
                .set("host", HOST)
                .set("user", null)
                .set("password", PASSWORD)
                .set("path_prefix", "")
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        final Path out = embulk.createTempFile("csv");
        try {
            final TestingEmbulk.RunResult result = embulk.runInput(config, out);
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
            assertTrue(ex.getCause().getCause() instanceof JsonMappingException);
            assertTrue(ex.getCause().getCause().getMessage().startsWith("Field 'user' is required but not set."));
            return;
        }
        fail("Expected Exception was not thrown.");
    }

    @Test
    public void checkDefaultValuesHostIsInvalid() throws IOException
    {
        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "sftp")
                .set("host", HOST + " ")
                .set("user", USERNAME)
                .set("password", PASSWORD)
                .set("path_prefix", "")
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        final Path out = embulk.createTempFile("csv");
        try {
            final TestingEmbulk.RunResult result = embulk.runInput(config, out);
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
            assertTrue(ex.getCause().getMessage().startsWith("'host' can't contain spaces"));
            return;
        }
        fail("Expected Exception was not thrown.");
    }

    @Test
    public void testResume()
    {
        PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        task.setFiles(createFileList(Arrays.asList("/in/aa/a"), task));
        ConfigDiff configDiff = plugin.resume(task.toTaskSource(), 0, new FileInputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                return emptyTaskReports(taskCount);
            }
        });
        assertEquals("/in/aa/a", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testResumeIncrementalFalse()
    {
        ConfigSource newConfig = config.deepCopy().set("incremental", false);
        PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(newConfig, PluginTask.class);
        task.setFiles(createFileList(Arrays.asList("in/aa/a"), task));
        ConfigDiff configDiff = plugin.resume(task.toTaskSource(), 0, new FileInputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                return emptyTaskReports(taskCount);
            }
        });
        assertEquals("{}", configDiff.toString());
    }

    @Test
    public void testCleanup()
    {
        PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        plugin.cleanup(task.toTaskSource(), 0, Lists.<TaskReport>newArrayList()); // no errors happens
    }

    @Test
    public void testListFiles() throws Exception
    {
        uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01.csv", true);
        uploadFile(Resources.getResource("sample_02.csv").getPath(), REMOTE_DIRECTORY + "sample_02.csv", true);

        PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        List<String> fileList = Arrays.asList(
            SftpFileInput.getSftpFileUri(task, REMOTE_DIRECTORY + "sample_01.csv"),
            SftpFileInput.getSftpFileUri(task, REMOTE_DIRECTORY + "sample_02.csv")
        );
        FileList expected = createFileList(fileList, task);

        ConfigDiff configDiff = plugin.transaction(config, new FileInputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                assertEquals(2, taskCount);
                return emptyTaskReports(taskCount);
            }
        });

        Method listFilesByPrefix = SftpFileInput.class.getDeclaredMethod("listFilesByPrefix", PluginTask.class);
        listFilesByPrefix.setAccessible(true);
        FileList actual = (FileList) listFilesByPrefix.invoke(plugin, task);

        assertEquals(expected.get(0), actual.get(0));
        assertEquals(expected.get(1), actual.get(1));
        assertEquals(SftpFileInput.getRelativePath(task, Optional.of(expected.get(1).get(0))), configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testListFilesAuthFail() throws Exception
    {
        uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01.csv", true);
        uploadFile(Resources.getResource("sample_02.csv").getPath(), REMOTE_DIRECTORY + "sample_02.csv", true);

        PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper()
                .map(config.deepCopy().set("user", "wrong_user"), PluginTask.class);

        plugin.transaction(config, new FileInputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                assertEquals(2, taskCount);
                return emptyTaskReports(taskCount);
            }
        });

        Method listFilesByPrefix = SftpFileInput.class.getDeclaredMethod("listFilesByPrefix", PluginTask.class);
        listFilesByPrefix.setAccessible(true);
        try {
            listFilesByPrefix.invoke(plugin, task);
            fail();
        }
        catch (Exception ex) {
            Throwable cause = ex.getCause();
            assertEquals(cause.getClass(), ConfigException.class);
            assertEquals(cause.getCause().getCause().getCause().getMessage(), "Auth fail");
        }
    }

    @Test
    public void testListFilesWithPathPrefixPointToFile() throws Exception
    {
        uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01.csv", true);
        ConfigSource configSource = config.deepCopy();
        configSource.set("path_prefix", REMOTE_DIRECTORY + "not_exist.csv");
        PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(configSource, PluginTask.class);
        FileList actual = (FileList) SftpFileInput.listFilesByPrefix(task);
        assertEquals(0, actual.getTaskCount());
    }

    @Test
    public void testListFilesWithPathPrefix() throws Exception
    {
        uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01.csv", true);
        ConfigSource configSource = config.deepCopy();
        configSource.set("path_prefix", REMOTE_DIRECTORY + "sample_01");
        PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        FileList actual = (FileList) SftpFileInput.listFilesByPrefix(task);
        assertEquals(1, actual.getTaskCount());
        assertEquals(actual.get(0).get(0), "sftp://username:password@127.0.0.1:20022/home/username/unittest/sample_01.csv");
    }

    @Test
    public void testSftpInputByOpen() throws Exception
    {
        uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01.csv", true);
        uploadFile(Resources.getResource("sample_02.csv").getPath(), REMOTE_DIRECTORY + "sample_02.csv", true);

        PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        final Path out = embulk.createTempFile("csv");
        final TestingEmbulk.RunResult result = embulk.runInput(config, out);

        Method listFilesByPrefix = SftpFileInput.class.getDeclaredMethod("listFilesByPrefix", PluginTask.class);
        listFilesByPrefix.setAccessible(true);
        task.setFiles((FileList) listFilesByPrefix.invoke(plugin, task));

        assertArrayEquals(Files.readAllBytes(Paths.get(Resources.getResource("sample_out.csv").getPath())), Files.readAllBytes(out));
    }

//    @Test
//    public void testSftpInputByOpenWithProxy() throws Exception
//    {
//        HttpProxyServer proxyServer = null;
//        try {
//            proxyServer = createProxyServer(PROXY_PORT);
//
//            uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01.csv", true);
//            uploadFile(Resources.getResource("sample_02.csv").getPath(), REMOTE_DIRECTORY + "sample_02.csv", true);
//
//            ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
//                    .set("host", HOST)
//                    .set("port", PORT)
//                    .set("user", USERNAME)
//                    .set("password", PASSWORD)
//                    .set("path_prefix", REMOTE_DIRECTORY)
//                    .set("last_path", "")
//                    .set("proxy", proxyConfig())
//                    .set("parser", parserConfig(schemaConfig()));
//
//            PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
//            runner.transaction(config, new Control());
//
//            Method listFilesByPrefix = SftpFileInput.class.getDeclaredMethod("listFilesByPrefix", PluginTask.class);
//            listFilesByPrefix.setAccessible(true);
//            task.setFiles((FileList) listFilesByPrefix.invoke(plugin, task));
//
//            assertRecords(config, output);
//            log.info("config:", config);
//            log.info("output:", output);
//        }
//        finally {
//            if (proxyServer != null) {
//                proxyServer.stop();
//            }
//        }
//    }

    @Test
    public void testSftpInputByOpenTimeout() throws Exception
    {
        uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01.csv", true);
        uploadFile(Resources.getResource("sample_02.csv").getPath(), REMOTE_DIRECTORY + "sample_02.csv", true);

        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "sftp")
                .set("host", HOST)
                .set("port", PORT)
                .set("user", "invalid-username")
                .set("password", PASSWORD)
                .set("path_prefix", REMOTE_DIRECTORY)
                .set("max_connection_retry", 2)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        final Path out = embulk.createTempFile("csv");
        try {
            final TestingEmbulk.RunResult result = embulk.runInput(config, out);
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof RuntimeException);
            assertTrue(ex.getCause().getCause() instanceof FileSystemException);
            assertTrue(ex.getCause().getCause().getMessage().startsWith("Could not connect to SFTP server"));
            return;
        }
        fail("Expected Exception was not thrown.");
    }

    @Test
    public void testSftpInputByOpenFailWithRetry() throws Exception
    {
        uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01.csv", false);
        uploadFile(Resources.getResource("sample_02.csv").getPath(), REMOTE_DIRECTORY + "sample_02.csv", false);

        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "sftp")
                .set("host", HOST)
                .set("port", PORT)
                .set("user", USERNAME)
                .set("password", PASSWORD)
                .set("path_prefix", REMOTE_DIRECTORY)
                .set("max_connection_retry", 2)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        final Path out = embulk.createTempFile("csv");
        try {
            final TestingEmbulk.RunResult result = embulk.runInput(config, out);
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof RuntimeException);
            assertTrue(ex.getCause().getCause() instanceof FileSystemException);
            assertTrue(ex.getCause().getCause().getMessage().startsWith("Unknown message with code \"java.nio.file.AccessDeniedException"));
            return;
        }
        fail("Expected Exception was not thrown.");
    }

    @Test
    public void testProxyType()
    {
        // test valueOf()
        assertEquals("http", ProxyTask.ProxyType.valueOf("HTTP").toString());
        assertEquals("socks", ProxyTask.ProxyType.valueOf("SOCKS").toString());
        assertEquals("stream", ProxyTask.ProxyType.valueOf("STREAM").toString());
        try {
            ProxyTask.ProxyType.valueOf("non-existing-type");
        }
        catch (Exception ex) {
            assertEquals(IllegalArgumentException.class, ex.getClass());
        }

        // test fromString
        assertEquals(ProxyTask.ProxyType.HTTP, ProxyTask.ProxyType.fromString("http"));
        assertEquals(ProxyTask.ProxyType.SOCKS, ProxyTask.ProxyType.fromString("socks"));
        assertEquals(ProxyTask.ProxyType.STREAM, ProxyTask.ProxyType.fromString("stream"));
        try {
            ProxyTask.ProxyType.fromString("non-existing-type");
        }
        catch (Exception ex) {
            assertEquals(ConfigException.class, ex.getClass());
        }
    }

    @Test
    public void testSetProxyType() throws Exception
    {
        PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        FileSystemOptions fsOptions = SftpFileInput.initializeFsOptions(task);
        SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();

        ProxyTask.ProxyType.setProxyType(builder, fsOptions, ProxyTask.ProxyType.HTTP);
        assertEquals(SftpFileSystemConfigBuilder.PROXY_HTTP, builder.getProxyType(fsOptions));

        ProxyTask.ProxyType.setProxyType(builder, fsOptions, ProxyTask.ProxyType.SOCKS);
        assertEquals(SftpFileSystemConfigBuilder.PROXY_SOCKS5, builder.getProxyType(fsOptions));

        ProxyTask.ProxyType.setProxyType(builder, fsOptions, ProxyTask.ProxyType.STREAM);
        assertEquals(SftpFileSystemConfigBuilder.PROXY_STREAM, builder.getProxyType(fsOptions));
    }

    /**
     * Test get relative path with special character password
     */
    @Test
    public void testGetRelativePathWithPassword()
    {
        ConfigSource conf = config.deepCopy();
        String expected = "/path/to/sample !@#.csv";

        conf.set("password", "ABCDE");
        PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(conf, PluginTask.class);
        String uri = SftpFileInput.getSftpFileUri(task, "/path/to/sample !@#.csv");
        assertEquals(expected, SftpFileInput.getRelativePath(task, Optional.of(uri)));

        conf.set("password", "ABCD#$¥!%'\"@?<>\\&/_^~|-=+-,{}[]()");
        task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(conf, PluginTask.class);
        uri = SftpFileInput.getSftpFileUri(task, "/path/to/sample !@#.csv");
        assertEquals(expected, SftpFileInput.getRelativePath(task, Optional.of(uri)));
    }

    @Test
    public void testGetRelativePath()
    {
        String expected = "/path/to/sample !@#.csv";
        String path = "/path/to/sample !@#.csv";
        CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        assertEquals(expected, SftpFileInput.getRelativePath(null, Optional.of(path)));
    }

    @Test(expected = ConfigException.class)
    public void testGetRelativePathWithHttpScheme()
    {
        String path = "http://host/path/to/sample !@#.csv";
        CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);
        SftpFileInput.getRelativePath(null, Optional.of(path));
    }

    /**
     * When user explicitly set path_prefix to a single file. we should add that file only
     * @throws Exception
     */
    @Test
    public void testListByPrefixWithSpecificPathPrefix() throws Exception
    {
        ConfigSource conf = config.deepCopy();
        conf.set("path_prefix", REMOTE_DIRECTORY + "sample_01.csv");
        PluginTask pluginTask = CONFIG_MAPPER_FACTORY.createConfigMapper().map(conf, PluginTask.class);
        uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01.csv", true);
        uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01 .csv", true);
        uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01ABC.csv", true);
        uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01DEF!@#.csv", true);
        FileList fileList = SftpFileInput.listFilesByPrefix(pluginTask);
        assertEquals(1, fileList.getTaskCount());
        assertEquals("sftp://username:password@127.0.0.1:20022/home/username/unittest/sample_01.csv", fileList.get
                (0).get(0));
    }

    @Test
    public void testListFilesStopWhenFileNotFound() throws Exception
    {
        ConfigSource conf = config.deepCopy();
        conf.set("path_prefix", REMOTE_DIRECTORY + "sample_01.csv");
        conf.set("path_match_pattern", "\\.xml$");
        conf.set("stop_when_file_not_found", true);

        uploadFile(Resources.getResource("sample_01.csv").getPath(), REMOTE_DIRECTORY + "sample_01.csv", true);
        PluginTask task = SftpFileInputPlugin.CONFIG_MAPPER_FACTORY
                .createConfigMapper()
                .map(conf, PluginTask.class);

        try {
            SftpFileInput.listFilesByPrefix(task);
        }
        catch (RuntimeException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof ConfigException) {
                String expected = "No file is found. \"stop_when_file_not_found\" option is \"true\".";
                assertEquals(expected, cause.getMessage());
            } else {
                throw ex;
            }
        }
    }

    private SshServer createSshServer(String host, int port, final String sshUsername, final String sshPassword)
    {
        // setup a mock sftp server
        SshServer sshServer = SshServer.setUpDefaultServer();
        VirtualFileSystemFactory fsFactory = new VirtualFileSystemFactory();
        fsFactory.setUserHomeDir(sshUsername, testFolder.getRoot().toPath());
        sshServer.setFileSystemFactory(fsFactory);
        sshServer.setHost(host);
        sshServer.setPort(port);
        sshServer.setSubsystemFactories(Collections.<NamedFactory<Command>>singletonList(new SftpSubsystemFactory()));
        sshServer.setCommandFactory(new ScpCommandFactory());
        File file = new File(SECRET_KEY_FILE);
        AbstractGeneratorHostKeyProvider hostKeyProvider = new SimpleGeneratorHostKeyProvider(file);
        hostKeyProvider.setAlgorithm("RSA");
        sshServer.setKeyPairProvider(hostKeyProvider);
        sshServer.setPasswordAuthenticator(new PasswordAuthenticator()
        {
            @Override
            public boolean authenticate(final String username, final String password, final ServerSession session)
            {
                return sshUsername.contentEquals(username) && sshPassword.contentEquals(password);
            }
        });
        sshServer.setPublickeyAuthenticator(new PublickeyAuthenticator()
        {
            @Override
            public boolean authenticate(String username, PublicKey key, ServerSession session)
            {
                return true;
            }
        });

        try {
            sshServer.start();
        }
        catch (IOException ex) {
            log.debug(ex.getMessage(), ex);
        }
        return sshServer;
    }

    private HttpProxyServer createProxyServer(int port)
    {
        return DefaultHttpProxyServer.bootstrap()
                .withPort(port)
                .start();
    }

    private void uploadFile(String localPath, String remotePath, boolean isReadable) throws Exception
    {
        PluginTask task = CONFIG_MAPPER_FACTORY.createConfigMapper().map(config, PluginTask.class);

        FileSystemOptions fsOptions = SftpFileInput.initializeFsOptions(task);
        String uri = SftpFileInput.getSftpFileUri(task, remotePath);

        int count = 0;
        while (true) {
            try {
                StandardFileSystemManager manager = new StandardFileSystemManager();
                manager.setClassLoader(TestSftpFileInputPlugin.class.getClassLoader());
                manager.init();

                FileObject localFile = manager.resolveFile(localPath);
                FileObject remoteFile = manager.resolveFile(uri, fsOptions);
                remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
                if (!isReadable) {
                    remoteFile.setReadable(false, false);
                }

                if (log.isDebugEnabled()) {
                    FileObject files = manager.resolveFile(SftpFileInput.getSftpFileUri(task, REMOTE_DIRECTORY));
                    for (FileObject f : files.getChildren()) {
                        if (f.isFile()) {
                            log.debug("remote file list:" + f.toString());
                        }
                    }
                }
                return;
            }
            catch (FileSystemException ex) {
                if (++count == task.getMaxConnectionRetry()) {
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
        }
    }

    private FileList createFileList(List<String> fileList, PluginTask task)
    {
        FileList.Builder builder = new FileList.Builder(task);
        for (String file : fileList) {
            builder.add(file, 0);
        }
        return builder.build();
    }

    static List<TaskReport> emptyTaskReports(int taskCount)
    {
        ImmutableList.Builder<TaskReport> reports = new ImmutableList.Builder<>();
        for (int i = 0; i < taskCount; i++) {
            reports.add(CONFIG_MAPPER_FACTORY.newTaskReport());
        }
        return reports.build();
    }

    private ConfigSource config()
    {
        return CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "sftp")
                .set("host", HOST)
                .set("port", PORT)
                .set("user", USERNAME)
                .set("password", PASSWORD)
                .set("path_prefix", REMOTE_DIRECTORY)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));
    }

    private ImmutableMap<String, Object> proxyConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "http");
        builder.put("host", PROXY_HOST);
        builder.put("port", PROXY_PORT);
        return builder.build();
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        builder.add(ImmutableMap.of("name", "json_column", "type", "json"));
        return builder.build();
    }
}
