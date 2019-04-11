package repo

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"sync"
	"time"
)

type CassandraFn func() (*gocql.Session, error)

var insertStmt = "INSERT INTO account (id, name) VALUES (?,?);"

func (c CassandraFn) Write(count int) error {

	s, err := c()
	if err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime)
		if elapsed > 10*time.Millisecond {
			logrus.Warnf("processed in: %s", elapsed)
		} else {
			logrus.Debugf("processed in: %s", elapsed)
		}
	}()

	insertedCh := make(chan int)

	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			//logrus.Infof("Inserting: %d", i)
			defer func() {
				insertedCh <- i
			}()
			if err = s.Query(insertStmt, gocql.UUIDFromTime(time.Now()), fmt.Sprintf("name_%", time.Now().Nanosecond())).Exec(); err != nil {
				logrus.Errorf("Failed to insert: %v", err)
			}
		}(i)
	}

	go myPrintFn(insertedCh)

	wg.Wait()

	return nil
}

func myPrintFn(insertedCh chan int) {

	for {
		select {
		case i := <-insertedCh:
			logrus.Infof("in my print Inserting: %d", i)
		}
	}
}
