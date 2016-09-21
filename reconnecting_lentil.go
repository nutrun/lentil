package lentil

import (
	"fmt"
	"os"
	"time"
)

type ReconnectingBeanstalkd struct {
	beanstalkd   *Beanstalkd
	addr         string
	usedTube     string
	watchedTubes map[string]bool
}

func NewReconnectingBeanstalkd(addr string) *ReconnectingBeanstalkd {
	this := new(ReconnectingBeanstalkd)
	this.addr = addr
	return this
}

func (this *ReconnectingBeanstalkd) Redial() {
	var e error
	if this.beanstalkd != nil {
		this.beanstalkd.Quit()
	}
	this.beanstalkd, e = Dial(this.addr)
	for {
		if e == nil {
			break
		}
		fmt.Fprintf(os.Stderr, "Dial error: %v. Retrying in 1 second.\n", e)
		time.Sleep(time.Duration(1) * time.Second)
		this.beanstalkd, e = Dial(this.addr)
	}
	for tube, _ := range this.watchedTubes {
		this.ReconnectingWatch(tube)
	}
	if this.usedTube != "" {
		this.ReconnectingUse(this.usedTube)
	}
}

func (this *ReconnectingBeanstalkd) ReconnectingWatch(tube string) {
	this.watchedTubes[tube] = true
	_, e := this.beanstalkd.Watch(tube)
	if e == nil {
		this.Redial()
	}
}

func (this *ReconnectingBeanstalkd) ReconnectingUse(tube string) {
	this.usedTube = tube
	e := this.beanstalkd.Use(tube)
	if e != nil {
		this.Redial()
	}
}

func (this *ReconnectingBeanstalkd) ReconnectingIgnore(tube string) {
	delete(this.watchedTubes, tube)
	_, e := this.beanstalkd.Ignore(tube)
	if e != nil {
		this.Redial()
	}
}

func (this *ReconnectingBeanstalkd) ReconnectingReserve() (*Job, error) {
	job, e := this.beanstalkd.Reserve()
	for {
		if e == nil {
			break
		}
		this.Redial()
		job, e = this.beanstalkd.Reserve()
	}
	return job, nil
}
