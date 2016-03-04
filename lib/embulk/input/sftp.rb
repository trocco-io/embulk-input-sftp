Embulk::JavaPlugin.register_input(
  "sftp", "org.embulk.input.sftp.SftpFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
