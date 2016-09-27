## 0.2.2 - 2016-09-26

* [maintenance] Fix bug Use second as timetout setting instead of milli second [#18](https://github.com/embulk/embulk-input-sftp/pull/18)

## 0.2.1 - 2016-09-12
* [maintenance] Fix last_path generation failure when password contains special chars [#15](https://github.com/sakama/embulk-input-sftp/pull/15)

## 0.2.0 - 2016-08-19

* [new feature] Support incremental option [#11](https://github.com/sakama/embulk-input-sftp/pull/11)
* [maintenance] Mask password in the log [#12](https://github.com/sakama/embulk-input-sftp/pull/12)
* [maintenance] Fix file listing order and handling of last_path [#14](https://github.com/sakama/embulk-input-sftp/pull/14)

## 0.1.2 - 2015-03-23

* [maintenance] Use RetryExecutor when retrying that is provide by embulk-core [#9](https://github.com/sakama/embulk-input-sftp/pull/9)

## 0.1.1 - 2015-03-18

* [new feature] Support last_path_ option [#2](https://github.com/sakama/embulk-input-sftp/pull/2)[#4](https://github.com/sakama/embulk-input-sftp/pull/4)[#7](https://github.com/sakama/embulk-input-sftp/pull/7)
* [new feature] Support path_match_pattern option [#6](https://github.com/sakama/embulk-input-sftp/pull/6)
* [maintenance] Add unit test [#3](https://github.com/sakama/embulk-input-sftp/pull/3)
* [maintenance] Skip retry of file downloading when permission denied error happens [#1](https://github.com/sakama/embulk-input-sftp/pull/1)
