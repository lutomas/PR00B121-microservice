package cassandra

import (
	"context"
	log "github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"sync"
	"time"
)

type Driver struct {
	id       int // Just for identification
	mux      sync.Mutex
	session  *gocql.Session
	hosts    []string
	keyspace string
	port     int
	timeout  time.Duration
}

func (driver *Driver) Session() (*gocql.Session, error) {
	// Check if Session is valid
	if driver.session == nil || driver.session.Closed() {
		// Lock config
		driver.mux.Lock()
		// Unlock config
		defer driver.mux.Unlock()

		// Check if Session is valid inside config lock
		if driver.session == nil || driver.session.Closed() {
			//log.Debug("initialising new Cassandra session...")
			var err error
			if driver.session, err = driver.makeCluster().CreateSession(); err == nil {
				log.WithFields(log.Fields{"r": "Cassandra", "id": driver.id}).Debug("Session re-created")
			}
		}
	}
	return driver.session, nil
}

func (driver *Driver) makeCluster() *gocql.ClusterConfig {
	cluster := gocql.NewCluster(driver.hosts...)
	cluster.Keyspace = driver.keyspace
	cluster.ProtoVersion = 4
	cluster.Timeout = driver.timeout
	cluster.Port = driver.port // default port

	// FIXME: ReconnectInterval should be configurable through config file.
	cluster.ReconnectInterval = time.Duration(1 * time.Second)
	return cluster
}

func NewCassandra(ctx context.Context) (*Driver, error) {
	driver := new(Driver)
	driver.id = time.Now().Nanosecond()
	driver.hosts = []string{"tlu.local"}
	driver.keyspace = "pr00b121_microservice"
	driver.port = 9042
	driver.timeout = 3 * time.Second

	lwf := log.WithFields(log.Fields{"r": "Cassandra", "id": driver.id})

	var err error
	driver.session, err = driver.makeCluster().CreateSession()
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		if driver.session != nil && driver.session.Closed() == false {
			lwf.Warn("Closing session")
			driver.session.Close()
		}
	}()

	return driver, nil
}
