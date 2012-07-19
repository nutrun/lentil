# lentil - beanstalkd command line client

_This is the lentil command line client's README. For the the golang library see [github.com/nutrun/lentil/blob/master/README.md](https://github.com/nutrun/lentil/blob/master/README.md)_

## INSTALL

```bash
go get github.com/nutrun/lentil/lentil
```

Or, in $GOPATH/src:

```bash
git clone git://github.com/nutrun/lentil.git && cd lentil/lentil && go install
```

## USAGE

```bash
# List available tubes
$ lentil -list-tubes
default
# Put a job in default tube
$ lentil -put="hello world"
id:1
# Put a job in a non default tube
$ lentil -put="hello wurld" -tube=foo
id:2
# More tubes to list
$ lentil -list-tubes
default
foo
# Peek (print id and body) a job by id
$ lentil -peek=2
2:hello wurld
# Delete a job by its id
$ lentil -delete=2
# Emtpy tubes get deleted by beanstalkd
$ lentil -list-tubes
default
# Put a bunch of jobs
$ for i in {1..5}; do lentil -put="msg $i"; done
id:3
id:4
id:5
id:6
id:7
# Drain all jobs from a tube
$ lentil -drain=default
1:hello world
3:msg 1
4:msg 2
5:msg 3
6:msg 4
7:msg 5
# Get stats for a tube. There's a lot, filter out what's not pause related here
$ lentil -stats-tube=default | grep pause
cmd-pause-tube:0
pause-time-left:0
pause:0
# Pause a tube for 30 seconds
$ lentil -pause-tube=default -delay=30
Paused default for 30 seconds
# Get pause stats for default tube again
$ lentil -stats-tube=default | grep pause
cmd-pause-tube:1
pause-time-left:26
pause:30
# Get queue stats, there's a lot, only looking at first three here
$ lentil -stats | head -n3
cmd-list-tubes:3
binlog-current-index:0
cmd-stats-tube:1
```


## LINKS
* beanstalkd: http://kr.github.com/beanstalkd/
* beanstalkd source: https://github.com/kr/beanstalkd/
* beanstalkd protocol: https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt

## LICENCE
See [LICENSE](https://github.com/nutrun/lentil/blob/master/LICENSE),
it's the same as [beanstalkd's license](https://github.com/kr/beanstalkd/blob/master/LICENSE)
