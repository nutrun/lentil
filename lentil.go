package lentil

import (
	"bufio"
	"errors"
	"fmt"
	"net"
)

type Beanstalkd struct {
	conn   net.Conn
	reader *bufio.Reader
}

type Job struct {
	Id   uint64
	Body []byte
}

func Dial(addr string) (*Beanstalkd, error) {
	this := new(Beanstalkd)
	var e error
	this.conn, e = net.Dial("tcp", addr)
	if e != nil {
		return nil, e
	}
	this.reader = bufio.NewReader(this.conn)
	return this, nil
}

func (this *Beanstalkd) Watch(tube string) (int, error) {
	fmt.Fprintf(this.conn, "watch %s\r\n", tube)
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return 0, e
	}
	var watched int
	_, e = fmt.Sscanf(reply, "WATCHING %d\r\n", &watched)
	if e != nil {
		return 0, errors.New(reply)
	}
	return watched, nil
}

func (this *Beanstalkd) Ignore(tube string) (int, error) {
	fmt.Fprintf(this.conn, "ignore %s\r\n", tube)
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return 0, e
	}
	var watched int
	_, e = fmt.Sscanf(reply, "WATCHING %d\r\n", &watched)
	if e != nil {
		return 0, errors.New(reply)
	}
	return watched, nil
}

func (this *Beanstalkd) Use(tube string) error {
	fmt.Fprintf(this.conn, "use %s\r\n", tube)
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return e
	}
	var usedTube string
	_, e = fmt.Sscanf(reply, "USING %s\r\n", &usedTube)
	if e != nil || tube != usedTube {
		return errors.New(reply)
	}
	return nil
}

func (this *Beanstalkd) Put(priority, delay, ttr int, bytes []byte) (int, error) {
	fmt.Fprintf(this.conn, "put %d %d %d %d\r\n%s\r\n", priority, delay, ttr, len(bytes), bytes)
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return -1, e
	}
	var id int
	_, e = fmt.Sscanf(reply, "INSERTED %d\r\n", &id)
	if e != nil {
		return -1, errors.New(reply)
	}
	return id, nil
}

func (this *Beanstalkd) Reserve() (*Job, error) {
	fmt.Fprint(this.conn, "reserve\r\n")
	return this.handleReserveReply()
}

func (this *Beanstalkd) ReserveWithTimeout(seconds int) (*Job, error) {
	fmt.Fprintf(this.conn, "reserve-with-timeout %d\r\n", seconds)
	return this.handleReserveReply()
}

func (this *Beanstalkd) handleReserveReply() (*Job, error) {
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return nil, e
	}
	var id uint64
	var bodylen int
	_, e = fmt.Sscanf(reply, "RESERVED %d %d\r\n", &id, &bodylen)
	if e != nil {
		return nil, errors.New(reply)
	}
	body, e := this.reader.ReadSlice('\n')
	if e != nil {
		return nil, e
	}
	body = body[0 : len(body)-2] // throw away \r\n suffix
	if len(body) != bodylen {
		return nil, errors.New(fmt.Sprintf("Job body length missmatch %d/%d", len(body), bodylen))
	}
	return &Job{Id: id, Body: body}, nil
}

func (this *Beanstalkd) Delete(id uint64) error {
	fmt.Fprintf(this.conn, "delete %d\r\n", id)
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return e
	}
	_, e = fmt.Sscanf(reply, "DELETED\r\n")
	if e != nil {
		return e
	}
	return nil
}
