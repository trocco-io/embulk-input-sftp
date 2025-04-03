# SFTP file input plugin for Embulk
[![Build Status](https://travis-ci.org/embulk/embulk-input-sftp.svg?branch=master)](https://travis-ci.org/embulk/embulk-input-sftp)

Reads files stored on remote server using SFTP

embulk-input-sftp v0.3.0+ requires Embulk v0.9.12+

## Overview

* **Plugin type**: file input
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

- **host**: (string, required)
- **port**: (string, default: `22`)
- **user**: (string, required)
- **password**: (string, default: `null`)
- **secret_key_file**: (string, default: `null`). **OpenSSH** format is required.
- **secret_key_passphrase**: (string, default: `""`)
- **user_directory_is_root**: (boolean, default: `true`)
- **timeout**: sftp connection timeout seconds (integer, default: `600`)
- **path_prefix**: Prefix of output paths (string, required)
- **incremental**: enables incremental loading(boolean, optional. default: `true`). If incremental loading is enabled, config diff for the next execution will include `last_path` parameter so that next execution skips files before the path. Otherwise, `last_path` will not be included.
- **path_match_pattern**: regexp to match file paths. If a file path doesn't match with this pattern, the file will be skipped (regexp string, optional)
- **total_file_count_limit**: maximum number of files to read (integer, optional)
- **min_task_size (experimental)**: minimum size of a task. If this is larger than 0, one task includes multiple input files. This is useful if too many number of tasks impacts performance of output or executor plugins badly. (integer, optional)
- **stop_when_file_not_found**: if true, check existence of files (boolean, default false)

### Proxy configuration

- **proxy**:
    - **type**: (string(http | socks | stream), required, default: `null`)
        - **http**: use HTTP Proxy
        - **socks**: use SOCKS Proxy
        - **stream**: Connects to the SFTP server through a remote host reached by SSH
    - **host**: (string, required)
    - **port**: (int, default: `22`)
    - **user**: (string, optional)
    - **password**: (string, optional, default: `null`)
    - **command**: (string, optional)

### Example

```yaml
in:
  type: sftp
  host: 127.0.0.1
  port: 22
  user: embulk
  secret_key_file: /Users/embulk/.ssh/id_rsa
  secret_key_passphrase: secret_pass
  user_directory_is_root: false
  timeout: 600
  path_prefix: /data/sftp
```

To filter files using regexp:

```yaml
in:
  type: sftp
  path_prefix: logs/csv-
  ...
  path_match_pattern: \.csv$   # a file will be skipped if its path doesn't match with this pattern

  ## some examples of regexp:
  #path_match_pattern: /archive/         # match files in .../archive/... directory
  #path_match_pattern: /data1/|/data2/   # match files in .../data1/... or .../data2/... directory
  #path_match_pattern: .csv$|.csv.gz$    # match files whose suffix is .csv or .csv.gz
```

With proxy
```yaml
in:
  type: sftp
  host: 127.0.0.1
  port: 22
  user: embulk
  secret_key_file: /Users/embulk/.ssh/id_rsa
  secret_key_passphrase: secret_pass
  user_directory_is_root: false
  timeout: 600
  path_prefix: /data/sftp
  proxy:
    type: http
    host: proxy_host
    port: 8080
    user: proxy_user
    password: proxy_secret_pass
    command:
```

## Proxy settings

### Example
```yaml
in:
  type: sftp
  host: 127.0.0.1
  port: 22
  user: embulk
  secret_key_file: /Users/embulk/.ssh/id_rsa
  secret_key_passphrase: secret_pass
  user_directory_is_root: false
  timeout: 600
  path_prefix: /data/sftp
```

### Secret Keyfile configuration

Please set path of secret_key_file as follows.
```yaml
in:
  type: sftp
  ...
  secret_key_file: /path/to/id_rsa
  ...
```

You can also embed contents of secret_key_file at config.yml.
```yaml
in:
  type: sftp
  ...
  secret_key_file:
    content: |
      -----BEGIN RSA PRIVATE KEY-----
      ABCDEFG...
      HIJKLMN...
      OPQRSTU...
      -----END RSA PRIVATE KEY-----
  ...
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
$ ./gradlew bintrayUpload # release embulk-input-sftp to Bintray maven repo
```

## Test

```
$ ./gradlew test  # -t to watch change of files and rebuild continuously
```
