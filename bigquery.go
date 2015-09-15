package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"

	uuid "github.com/satori/go.uuid"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
)

func connectToBigquery(config *logstalkerConfig) *bigquery.TabledataService {
	jwtJSON, err := ioutil.ReadFile(config.JwtFilename)
	if err != nil {
		log.Fatal("jwt.json not found.")
	}

	jwtConfig, err := google.JWTConfigFromJSON(jwtJSON, bigquery.BigqueryScope)
	if err != nil {
		log.Fatal(err)
	}

	client := jwtConfig.Client(oauth2.NoContext)
	bigqueryService, _ := bigquery.New(client)

	manageTableCreation(config, bigqueryService)
	return bigquery.NewTabledataService(bigqueryService)
}

func manageTableCreation(config *logstalkerConfig, bigqueryService *bigquery.Service) {
	createUpcomingDaysTables(config, bigqueryService)

	ticker := time.NewTicker(time.Hour * 24)
	go func() {
		for range ticker.C {
			createUpcomingDaysTables(config, bigqueryService)
		}
	}()
}

func createUpcomingDaysTables(config *logstalkerConfig, bigqueryService *bigquery.Service) {
	// Create a table for today and the next 4 days. This gives us 5 opportunities
	// to create a table before logs are streamed to it, in case of errors.
	today := time.Now()

	for i := 0; i < 5; i++ {
		futureDate := today.Add(time.Hour * 24 * time.Duration(i))
		tableName := tableNameFromTime(futureDate)
		createTable(config, tableName, bigqueryService)
	}
}

func tableNameFromTime(t time.Time) string {
	return fmt.Sprintf("logs_%s", t.Format("20060102"))
}

func createTable(config *logstalkerConfig, tableName string, bigQueryService *bigquery.Service) {
	fields := []*bigquery.TableFieldSchema{
		&bigquery.TableFieldSchema{Name: "service", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "log_type", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "host", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "timestamp", Type: "TIMESTAMP"},
		&bigquery.TableFieldSchema{Name: "ip", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "domain", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "method", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "path", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "query", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "action", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "message", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "status", Type: "INTEGER"},
		&bigquery.TableFieldSchema{Name: "referrer", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "user_agent", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "user_id", Type: "STRING"},
		&bigquery.TableFieldSchema{Name: "response_time", Type: "FLOAT"},
	}

	ref := bigquery.TableReference{
		DatasetId: config.DatasetID,
		ProjectId: config.ProjectID,
		TableId:   tableName,
	}

	table := bigquery.Table{
		TableReference: &ref,
		Schema:         &bigquery.TableSchema{Fields: fields},
	}

	tableService := bigquery.NewTablesService(bigQueryService)
	insertCall := tableService.Insert(config.ProjectID, config.DatasetID, &table)
	insertCall.Do()
}

func stream(config *logstalkerConfig, tabledataService *bigquery.TabledataService, jsonData map[string]bigquery.JsonValue) {
	insertID := uuid.NewV4().String()
	jsonData["service"] = config.ServiceName

	tableName, ok := jsonData["tableName"].(string)
	if ok {
		delete(jsonData, "tableName")
	} else {
		tableName = tableNameFromTime(time.Now())
	}

	insertRequest := bigquery.TableDataInsertAllRequest{
		Rows: []*bigquery.TableDataInsertAllRequestRows{
			&bigquery.TableDataInsertAllRequestRows{
				InsertId: insertID,
				Json:     jsonData,
			},
		},
	}

	insertCall := tabledataService.InsertAll(config.ProjectID, config.DatasetID, tableName, &insertRequest)
	insertCall.Do()
}
