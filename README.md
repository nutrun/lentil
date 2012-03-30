# lentil - golang beanstalkd client library

## INSTALL

```bash
go get github.com/nutrun/lentil
```

Or, in $GOPATH/src:

```bash
git clone git://github.com/nutrun/lentil.git && cd lentil && go install
```

Use godoc to view documentation. For example:

```bash
godoc -http=:6060
```

And open http://0.0.0.0:6060/pkg/lentil/ in a browser.

## USAGE

### Example producer:

```go
package main

import(
	"lentil"
	"log"
)

func main() {
	conn, e := lentil.Dial("0.0.0.0:11300")
	if e != nil {
		log.Fatal(e)
	}
	jobId, e := conn.Put(0, 0, 60, []byte("hello"))
	if e != nil {
		log.Fatal(e)
	}
	log.Printf("JOB ID: %d\n", jobId)
}
```

### Example consumer:

```go
package main

import(
	"lentil"
	"log"
)

func main() {
	conn, e := lentil.Dial("0.0.0.0:11300")
	if e != nil {
		log.Fatal(e)
	}
	job, e := conn.Reserve()
	if e != nil {
		log.Fatal(e)
	}
	log.Printf("JOB ID: %d, JOB BODY: %s", job.Id, job.Body)
	e = conn.Delete(job.Id)
	if e != nil {
		log.Fatal(e)
	}
}
```

## LINKS

* beanstalkd: http://kr.github.com/beanstalkd/
* beanstalkd source: https://github.com/kr/beanstalkd/
* beanstalkd protocol: https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt
* kr's beanstalk.go: https://github.com/kr/beanstalk.go

