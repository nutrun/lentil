package lentil

import (
	"fmt"
	"log"
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
	// cleanup beanstalkd
	for {
		tubes, e := beanstalkd.ListTubes()
		if e != nil {
			t.Fatal(e)
		}
		for _, tube := range tubes {
			beanstalkd.Watch(tube)
		}
		job, e := beanstalkd.ReserveWithTimeout(0)
		if e != nil {
			for _, tube := range tubes {
				if tube != "default" {
					_, e := beanstalkd.Ignore(tube)
					if e != nil {
						t.Fatal(e)
					}
				}
			}
			break
		}
		beanstalkd.Delete(job.Id)
	}
	e = beanstalkd.Use("rock")
	if e != nil {
		t.Error(e)
	}
	id, e := beanstalkd.Put(0, 0, 60, []byte("y u no is job?"))
	if e != nil || id == 0 {
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
	beanstalkd.Use("default")
	beanstalkd.Put(0, 0, 60, []byte("job 2"))
	job, _ = beanstalkd.Reserve()
	e = beanstalkd.Release(job.Id, 0, 0)
	if e != nil {
		t.Error(e)
	}
	job, _ = beanstalkd.Reserve()
	e = beanstalkd.Touch(job.Id)
	if e != nil {
		t.Error(e)
	}
	e = beanstalkd.Bury(job.Id, 0)
	if e != nil {
		t.Error(e)
	}
	job, e = beanstalkd.PeekBuried()
	if e != nil {
		t.Error(e)
	}
	if string(job.Body) != "job 2" {
		t.Error("Peeked wrong job")
	}
	count, e := beanstalkd.Kick(1)
	if e != nil {
		t.Error(e)
	}
	if count != 1 {
		t.Error("Y U NO KIK?")
	}
	job, e = beanstalkd.Peek(job.Id)
	if e != nil {
		t.Error(e)
	}
	if string(job.Body) != "job 2" {
		t.Error("Peeked wrong job")
	}
	stats, e := beanstalkd.StatsJob(job.Id)
	if e != nil {
		t.Error(e)
	}
	// stats: age, delay, time-left, reserves, kicks, file, ttr, timeouts, releases, buries, id, tube, state, pri
	if len(stats) != 14 {
		t.Error("bad job stats")
	}
	stats, e = beanstalkd.StatsTube("default")
	if e != nil {
		t.Error(e)
	}
	// stats: name,	current-jobs-ready, current-jobs-buried, current-watching, current-jobs-delayed, total-jobs, pause, current-jobs-urgent, current-waiting, cmd-pause-tube, pause-time-left, current-jobs-reserved, current-using, cmd-delete
	if len(stats) != 14 {
		t.Error("bad tube stats")
	}
	stats, e = beanstalkd.Stats()
	if e != nil {
		t.Error()
	}
	// stats: cmd-list-tubes, current-tubes, current-waiting, cmd-peek, cmd-peek-ready, cmd-reserve-with-timeout, cmd-use, cmd-bury, binlog-records-written, cmd-kick, cmd-stats, cmd-pause-tube, version, total-jobs, hostname, cmd-peek-buried, cmd-reserve, cmd-delete, cmd-release, cmd-ignore, current-workers, total-connections, rusage-stime, current-jobs-reserved, cmd-put, cmd-watch, current-connections, current-producers, id, current-jobs-ready, current-jobs-buried, uptime, binlog-oldest-index, binlog-max-size, cmd-stats-tube, cmd-list-tubes-watched, max-job-size, binlog-records-migrated, rusage-utime, current-jobs-urgent, cmd-touch, cmd-stats-job, cmd-list-tube-used, pid, current-jobs-delayed, cmd-peek-delayed, job-timeouts, binlog-current-index
	if len(stats) != 48 {
		t.Error("bad stats")
	}
	tubes, e := beanstalkd.ListTubes()
	if e != nil {
		t.Error(e)
	}
	if len(tubes) != 1 {
		t.Error(len(tubes))
	}
	if tubes[0] != "default" {
		t.Error("Y U NO HAVE RITE TUB?")
	}
	tube, e := beanstalkd.ListTubeUsed()
	if e != nil {
		t.Error(e)
	}
	if tube != "default" {
		t.Error("Watching wrong tube")
	}
	tubes, e = beanstalkd.ListTubesWatched()
	if e != nil {
		t.Error(e)
	}
	if len(tubes) != 1 {
		t.Error(len(tubes))
	}
	if tubes[0] != "default" {
		t.Error("Y U NO HAVE RITE TUB?")
	}
	e = beanstalkd.PauseTube("default", 1)
	if e != nil {
		t.Error(e)
	}
	beanstalkd.Quit()
}

func ExampleBeanstalkd() {
	queue, e := Dial("0.0.0.0:11300")
	if e != nil {
		log.Fatal(e)
	}
	priority := 0 // Job priority, smaller runs first
	delay := 0    // Wait in seconds before making available to reserve
	ttr := 60     // Time to run in seconds since reserved by consummer before re-released in queue
	id, e := queue.Put(priority, delay, ttr, []byte("job body"))
	if e != nil {
		log.Fatal(e)
	}
	log.Printf("JOB ID: %d\n", id)
	job, e := queue.Reserve()
	if e != nil {
		log.Fatal(e)
	}
	log.Printf("JOB: %d %s\n", job.Id, job.Body)
	e = queue.Delete(job.Id)
	if e != nil {
		log.Fatal(e)
	}
}
