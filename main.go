package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/ActiveState/tail"
)

var logFilenameFlag = flag.String("log_filename", "", "full path to the log file")
var jwtFilenameFlag = flag.String("jwt_filename", "", "full path to the jwt.json file")
var serviceNameFlag = flag.String("service", "", "name of the service generating the logs (nginx, rails, nginx-backend, user-service, etc.)")
var parserTypeFlag = flag.String("parser", "nginx-access", "log file parser (nginx-access, nginx-error, rails)")
var projectIDFlag = flag.String("project_id", "", "Google project ID")
var datasetIDFlag = flag.String("dataset_id", "", "BigQuery dataset ID")

type logstalkerConfig struct {
	LogFilename string
	JwtFilename string
	ServiceName string
	Parser      string
	Host        string
	ProjectID   string
	DatasetID   string
}

func main() {
	flag.Parse()
	config := loadConfig()

	// Determine the parsing function for this log file
	parserFn, err := parsingFunctionForType(config.Parser)
	if err != nil {
		log.Fatal(err)
	}

	// Connect to BigQuery & create tables
	tabledataService := connectToBigquery(&config)

	// Start tailing the log file & parsing the entries
	seek := tail.SeekInfo{Offset: 0, Whence: 2}
	t, _ := tail.TailFile(config.LogFilename, tail.Config{
		Location: &seek,
		Follow:   true,
		Logger:   tail.DiscardingLogger,
	})

	for line := range t.Lines {
		parsed, err := parserFn(config.Host, strings.Replace(line.Text, "\\", "", -1))
		if err == nil {
			go stream(&config, tabledataService, parsed)
		}
	}
}

func loadConfig() logstalkerConfig {
	if *logFilenameFlag == "" {
		log.Fatal("log_filename is required")
	}

	if *jwtFilenameFlag == "" {
		log.Fatal("jwt_filename is required")
	}

	if *projectIDFlag == "" {
		log.Fatal("project_id is required")
	}

	if *datasetIDFlag == "" {
		log.Fatal("dataset_id is required")
	}

	hostname, _ := os.Hostname()
	config := logstalkerConfig{
		LogFilename: *logFilenameFlag,
		JwtFilename: *jwtFilenameFlag,
		ServiceName: *serviceNameFlag,
		Parser:      *parserTypeFlag,
		Host:        hostname,
		ProjectID:   *projectIDFlag,
		DatasetID:   *datasetIDFlag,
	}

	return config
}
