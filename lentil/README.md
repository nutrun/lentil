# lentil - beanstalkd command line client

_This is the lentil command line client. For the the golang library see [github.com/nutrun/lentil](https://github.com/nutrun/lentil)_

## INSTALL

Needs [golang](http://golang.org/)

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
# Any of pri, ttr and delay can be specified when putting a job
$ lentil -put="hello fine tuned world" -pri=7 -ttr=30 -delay=10
id:2
# Put a job in a tube
$ lentil -put="hello wurld" -tube=foo
id:3
# More tubes to list now
$ lentil -list-tubes
default
foo
# Peek a job by id
$ lentil -peek=3
3:hello wurld
# Delete a job by id
$ lentil -delete=3
# Beanstalkd deletes empty queues
$ lentil -list-tubes
default
# Put a bunch of jobs
$ for i in {1..5}; do lentil -put="msg $i"; done
id:4
id:5
id:6
id:7
id:8
# Drain all the jobs from a tube
$ lentil -drain=default
1:hello world
4:msg 1
5:msg 2
6:msg 3
7:msg 4
8:msg 5
2:hello fine tuned world
# Get tube stats. There's a lot, only looking at pause related ones here
$ lentil -stats-tube=default | grep pause
pause-time-left:0
pause:0
cmd-pause-tube:0
# Pause a tube for 30 seconds
$ lentil -pause-tube=default -delay=30
# Look at tube pause stats again
$ lentil -stats-tube=default | grep pause
cmd-pause-tube:1
pause-time-left:22
pause:30
# Get queue stats. There's a lot, only listing first three here
$ lentil -stats | head -n3
cmd-ignore:0
total-connections:18
cmd-list-tube-used:0
# Chain pipe commands shell style
$ lentil -list-tubes | xargs -n1 lentil -stats-tube
```

## LINKS
* beanstalkd: http://kr.github.com/beanstalkd/
* beanstalkd source: https://github.com/kr/beanstalkd/
* beanstalkd protocol: https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt

## LICENCE
See [LICENSE](https://github.com/nutrun/lentil/blob/master/LICENSE),
it's the same as [beanstalkd's license](https://github.com/kr/beanstalkd/blob/master/LICENSE)
