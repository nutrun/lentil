package lentil

import (
	"fmt"
	"testing"
)

// Test assumes beanstalkd running on 0.0.0.0:11300
func TestBeanstalk(t *testing.T) {
	beanstalkd, e := Dial("0.0.0.0:11301")
	if beanstalkd != nil || e == nil {
		t.Error("Shouldn't have connected")
	}
	beanstalkd, e = Dial("0.0.0.0:11300")
	if beanstalkd == nil || e != nil {
		t.Fatal("Should have connected without errors. Is beanstalkd running on 0.0.0.0:11300?")
	}
	e = beanstalkd.Use("rock")
	if e != nil {
		t.Error(e)
	}
	id, e := beanstalkd.Put(0, 0, 60, []byte("y u no is job?"))
	if e != nil || id == -1 {
		t.Error(e)
	}
	watching, e := beanstalkd.Watch("rock")
	if e != nil {
		t.Error(e)
	}
	if watching != 2 {
		t.Error("Y U NO WATCHIN 2 TUBS?")
	}
	job, e := beanstalkd.Reserve()
	if e != nil {
		t.Error(e)
	}
	if string(job.Body) != "y u no is job?" {
		t.Error(fmt.Sprintf("[%s] isn't [%s]", job.Body, "y u no is job?"))
	}
	e = beanstalkd.Delete(job.Id)
	if e != nil {
		t.Error(e)
	}
	_, e = beanstalkd.ReserveWithTimeout(0)
	if e == nil {
		t.Error("Y U NO TIME OUT?")
	}
	watching, e = beanstalkd.Ignore("dontexist")
	if e != nil {
		t.Error(e)
	}
	if watching != 2 {
		t.Error("Y U NO WATCH 2 TUBS?")
	}
	watching, e = beanstalkd.Ignore("rock")
	if e != nil {
		t.Error(e)
	}
	if watching != 1 {
		t.Error("Y U NO WATCH 1 TUB?")
	}
	_, e = beanstalkd.Ignore("default")
	if e == nil {
		t.Error("Y U NO ERROR?")
	}
}
