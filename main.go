package main

import (
	"context"
	"github.com/Sirupsen/logrus"
	"github.com/lutomas/PR00B121-microservice/cassandra"
	"github.com/lutomas/PR00B121-microservice/repo"
	"os"
)

func main() {

	ctx, cancelFn := context.WithCancel(context.Background())
	logrus.Info("Initialising DB...")
	d, err := cassandra.NewCassandra(ctx)
	if err != nil {
		logrus.Errorf("Failed to initialise DB: %v", err)
		os.Exit(1)
	}
	logrus.Info("DB Initialised")
	defer cancelFn()

	sessionFn := repo.CassandraFn(d.Session)

	sessionFn.Write(30000)

	//stop := make(chan os.Signal, 1)
	//signal.Notify(stop, os.Interrupt)
	//
	//// Wait for interrupt signal
	//<-stop
	//log.Info("Stopping internal servers and releasing resources...")

}
