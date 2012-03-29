package lentil

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
)

type Beanstalkd struct {
	conn   net.Conn
	reader *bufio.Reader
}

type Job struct {
	Id   uint64
	Body []byte
}

// Dial opens a connection to beanstalkd. The format of addr is 'host:port', e.g '0.0.0.0:11300'.
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

// Watch adds the named tube to a consumer's watch list for the current connection.
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

// Ignore removes the named tube from a consumer's watch list for the current connection
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

// Use is for producers. 
// Subsequent Put commands will put jobs into the tube specified by this command.
// If no use command has been issued, jobs will be put into the tube named "default".
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

// Put inserts a job into the queue.
func (this *Beanstalkd) Put(priority, delay, ttr int, data []byte) (int, error) {
	fmt.Fprintf(this.conn, "put %d %d %d %d\r\n%s\r\n", priority, delay, ttr, len(data), data)
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

// Reserve is for processes that want to consume jobs from the queue.
func (this *Beanstalkd) Reserve() (*Job, error) {
	fmt.Fprint(this.conn, "reserve\r\n")
	return this.handleReserveReply()
}

// ReserveWithTimeout is for processes that want to consume jobs from the queue.
// A timeout value of 0 will cause the server to immediately return either a response or TIMED_OUT.
// A positive timeout value will limit the amount of time the client will block on the reserve request.
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

// Delete  removes a job from the server entirely.
// It is normally used by the client when the job has successfully run to completion.
func (this *Beanstalkd) Delete(id uint64) error {
	fmt.Fprintf(this.conn, "delete %d\r\n", id)
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return e
	}
	_, e = fmt.Sscanf(reply, "DELETED\r\n")
	if e != nil {
		return errors.New(reply)
	}
	return nil
}

// Release puts a reserved job back into the ready queue.
func (this *Beanstalkd) Release(id uint64, pri, delay int) error {
	fmt.Fprintf(this.conn, "release %d %d %d\r\n", id, pri, delay)
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return e
	}
	_, e = fmt.Sscanf(reply, "RELEASED\r\n")
	if e != nil {
		return errors.New(reply)
	}
	return nil
}

// Bury puts a job into the "buried" state.
func (this *Beanstalkd) Bury(id uint64, pri int) error {
	fmt.Fprintf(this.conn, "bury %d %d\r\n", id, pri)
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return e
	}
	_, e = fmt.Sscanf(reply, "BURIED\r\n")
	if e != nil {
		return errors.New(reply)
	}
	return nil
}

// Touch allows a worker to request more time to work on a job.
func (this *Beanstalkd) Touch(id uint64) error {
	fmt.Fprintf(this.conn, "touch %d\r\n", id)
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return e
	}
	_, e = fmt.Sscanf(reply, "TOUCHED\r\n")
	if e != nil {
		return errors.New(reply)
	}
	return nil
}

// Peek lets the client inspect a job in the system.
func (this *Beanstalkd) Peek(id uint64) (*Job, error) {
	fmt.Fprintf(this.conn, "peek %d\r\n", id)
	return this.handlePeekReply()
}

// PeekReady lets the client inspect the first "ready" job.
func (this *Beanstalkd) PeekReady() (*Job, error) {
	fmt.Fprintf(this.conn, "peek-ready\r\n")
	return this.handlePeekReply()
}

// PeekDelayed lets the client inspect the first "delayed" job.
func (this *Beanstalkd) PeekDelayed() (*Job, error) {
	fmt.Fprintf(this.conn, "peek-delayed\r\n")
	return this.handlePeekReply()
}

// PeekDelayed lets the client inspect the first "buried" job.
func (this *Beanstalkd) PeekBuried() (*Job, error) {
	fmt.Fprintf(this.conn, "peek-buried\r\n")
	return this.handlePeekReply()
}

func (this *Beanstalkd) handlePeekReply() (*Job, error) {
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return nil, e
	}
	var id uint64
	var bodylen int
	_, e = fmt.Sscanf(reply, "FOUND %d %d\r\n", &id, &bodylen)
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

// Kick moves a job to the "ready" queue.
func (this *Beanstalkd) Kick(bound int) (int, error) {
	fmt.Fprintf(this.conn, "kick %d\r\n", bound)
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return -1, e
	}
	var count int
	_, e = fmt.Sscanf(reply, "KICKED %d\r\n", &count)
	if e != nil {
		return -1, errors.New(reply)
	}
	return count, nil
}

// StatsJob returns statistical information about a job.
func (this *Beanstalkd) StatsJob(id uint64) (map[string]string, error) {
	fmt.Fprintf(this.conn, "stats-job %d\r\n", id)
	return this.handleMapResponse()
}

// StatsTube returns statistical information about a tube.
func (this *Beanstalkd) StatsTube(tube string) (map[string]string, error) {
	fmt.Fprintf(this.conn, "stats-tube %s\r\n", tube)
	return this.handleMapResponse()
}

// Stats returns statistical information about the queue.
func (this *Beanstalkd) Stats() (map[string]string, error) {
	fmt.Fprintf(this.conn, "stats\r\n")
	return this.handleMapResponse()
}

func (this *Beanstalkd) handleMapResponse() (map[string]string, error) {
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return nil, e
	}
	var datalen int
	_, e = fmt.Sscanf(reply, "OK %d\r\n", &datalen)
	if e != nil {
		return nil, errors.New(reply)
	}
	data := make([]byte, datalen+2) // Add 2 for the trailing \r\n
	_, e = this.reader.Read(data)
	if e != nil {
		return nil, e
	}
	dict := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Index(line, ":") != -1 {
			keyvalue := strings.Split(line, ":")
			dict[keyvalue[0]] = strings.TrimSpace(keyvalue[1])
		}
	}
	return dict, nil
}

// ListTubes returns a list of all the existing tubes.
func (this *Beanstalkd) ListTubes() ([]string, error) {
	fmt.Fprintf(this.conn, "list-tubes\r\n")
	return this.handleListResponse()
}

func (this *Beanstalkd) handleListResponse() ([]string, error) {
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return nil, e
	}
	var datalen int
	_, e = fmt.Sscanf(reply, "OK %d\r\n", &datalen)
	if e != nil {
		return nil, errors.New(reply)
	}
	data := make([]byte, datalen+2) // Add 2 for the trailing \r\n
	_, e = this.reader.Read(data)
	if e != nil {
		return nil, e
	}
	lines := strings.Split(string(data), "\n")
	tubes := make([]string, 0)
	for _, line := range lines[1:len(lines)-2] {
		tube := strings.TrimSpace(line)
		tube = strings.TrimLeft(tube, "- ")
		tubes = append(tubes, tube)
	}
	return tubes, nil
}

// ListTubeUsed returns the tube currently used by a producer.
func (this *Beanstalkd) ListTubeUsed() (string, error) {
	fmt.Fprintf(this.conn, "list-tube-used\r\n")
	var tube string
	_, e := fmt.Fscanf(this.conn, "USING %s\r\n", &tube)
	if e != nil {
		return "", e
	}
	return tube, nil
}

// ListTubesWatched returns the list of tubes watched by a consumer.
func (this *Beanstalkd) ListTubesWatched() ([]string, error) {
	fmt.Fprint(this.conn, "list-tubes-watched\r\n")
	return this.handleListResponse()
}

// Quit closes the connection to the queue.
func (this *Beanstalkd) Quit() {
	fmt.Fprint(this.conn, "list-tubes-watched\r\n")
}

// PauseTube delays any new job being reserved for a given time.
func (this *Beanstalkd) PauseTube(tube string, delay int) error {
	fmt.Fprintf(this.conn, "pause-tube %s %d\r\n", tube, delay)
	reply, e := this.reader.ReadString('\n')
	if e != nil {
		return e
	}
	_, e = fmt.Sscanf(reply, "PAUSED\r\n")
	if e != nil {
		return errors.New(reply)
	}
	return nil
}