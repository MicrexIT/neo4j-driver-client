package neo4j

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
)
type Record map[string]interface{}

type Client interface {
	 Write(string, func(Record) error) error
	 Read(string, func(Record) error) error
}

type client struct {
	BoltUrl string
	Username string
	Password string
}

func NewClient(boltUrl string, username string, password string) client {
	return client{
		 boltUrl,
		 username,
		 password,
	}
}

func (c client) Write(query string, job func(record Record) error) error {
	return c.execute(neo4j.AccessModeWrite, query, job)
}

func (c client) Read(query string, job func(record Record) error) error {
	return c.execute(neo4j.AccessModeRead, query, job)
}

func (c client) execute(accessMode neo4j.AccessMode, query string, job func(record Record) error) error {
	driver, err := neo4j.NewDriver("bolt://" + c.BoltUrl, neo4j.BasicAuth(c.Username, c.Password, ""))
	if err != nil {
		return err // handle error
	}
	defer driver.Close()

	session, err := driver.Session(accessMode)
	if err != nil {
		return err
	}
	defer session.Close()
	result, err := session.Run(query, map[string]interface{}{})
	if err != nil {
		return err // handle error
	}
	for result.Next() {
		err = result.Err()
		if err != nil {
			return err
		}
		record := Record{}
		for _, key := range result.Record().Keys() {
			record[key], _ = result.Record().Get(key)
			fmt.Println(record[key])
		}

		if err := job(record); err != nil {
			fmt.Println("Error", err)
			return err
		}
	}
	return nil
}