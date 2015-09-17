package main

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"
)

const bigQueryTimeFormat = "2006-01-02 15:04:05"

func parsingFunctionForType(parserType string) (func(string, string) (map[string]bigquery.JsonValue, error), error) {
	parsers := map[string]func(string, string) (map[string]bigquery.JsonValue, error){
		"nginx-access": parseNginxAccessLine,
		"nginx-error":  parseNginxErrorLine,
		"rails":        parseRailsLine,
	}

	if fn, ok := parsers[parserType]; ok {
		return fn, nil
	}

	return nil, errors.New("Parser type not supported.")
}

func parseNginxAccessLine(host, logLine string) (map[string]bigquery.JsonValue, error) {
	lineData := make(map[string]bigquery.JsonValue)
	if err := json.Unmarshal([]byte(logLine), &lineData); err != nil {
		return lineData, err
	}

	lineData["host"] = host
	lineData["log_type"] = "nginx-access"

	parseRawRequest(lineData)
	parseNginxAccessTimestamp(lineData)

	for k, v := range lineData {
		if v == "-" || v == "" {
			delete(lineData, k)
		}
	}

	return lineData, nil
}

func parseNginxErrorLine(host, logLine string) (map[string]bigquery.JsonValue, error) {
	lineData := make(map[string]bigquery.JsonValue)

	errorPieces := strings.Split(logLine, ", ")
	parseNginxErrorHeader(errorPieces[0], lineData)

	params := make(map[string]string)
	for i := 1; i <= len(errorPieces[1:]); i++ {
		paramPieces := strings.Split(errorPieces[i], ": ")
		if len(paramPieces) == 2 {
			params[paramPieces[0]] = strings.Replace(paramPieces[1], "\"", "", -1)
		}
	}

	if IP, ok := params["client"]; ok {
		lineData["ip"] = IP
	}

	if domain, ok := params["host"]; ok {
		lineData["domain"] = domain
	}

	if request, ok := params["request"]; ok {
		lineData["request"] = request
		parseRawRequest(lineData)
	}

	if referrer, ok := params["referrer"]; ok {
		lineData["referrer"] = referrer
	}

	lineData["host"] = host
	lineData["log_type"] = "nginx-error"

	return lineData, nil
}

func parseRailsLine(host, logLine string) (map[string]bigquery.JsonValue, error) {
	lineData := make(map[string]bigquery.JsonValue)
	if err := json.Unmarshal([]byte(logLine), &lineData); err != nil {
		return lineData, err
	}

	lineData["host"] = host
	lineData["log_type"] = "rails"
	parseTableNameForRails(lineData)

	return lineData, nil
}

func parseNginxErrorHeader(header string, lineData map[string]bigquery.JsonValue) {
	if strings.Contains(header, " [error] ") {
		headerPieces := strings.Split(header, " [error] ")

		if len(headerPieces) == 2 {
			parseNginxErrorTimestamp(headerPieces[0], lineData)

			// Error message
			msgPieces := strings.SplitN(headerPieces[1], " ", 3)
			if len(msgPieces) == 3 {
				lineData["message"] = strings.Replace(msgPieces[2], "\"", "", -1)
			}
		}
	} else {
		lineData["message"] = strings.Replace(header, "\"", "", -1)

		now := time.Now()
		lineData["timestamp"] = now.Format(bigQueryTimeFormat)
		lineData["tableName"] = tableNameFromTime(now)
	}
}

func parseNginxAccessTimestamp(lineData map[string]bigquery.JsonValue) {
	t, err := time.Parse("02/Jan/2006:15:04:05 +0000", lineData["timestamp"].(string))
	if err == nil {
		lineData["timestamp"] = t.Format(bigQueryTimeFormat)
		lineData["tableName"] = tableNameFromTime(t)
	}
}

func parseNginxErrorTimestamp(raw string, lineData map[string]bigquery.JsonValue) {
	t, err := time.Parse("2006/01/02 15:04:05", raw)
	if err == nil {
		lineData["timestamp"] = t.Format(bigQueryTimeFormat)
		lineData["tableName"] = tableNameFromTime(t)
	}
}

func parseTableNameForRails(lineData map[string]bigquery.JsonValue) {
	t, err := time.Parse(bigQueryTimeFormat, lineData["timestamp"].(string))
	if err == nil {
		lineData["tableName"] = tableNameFromTime(t)
	}
}

func parseRawRequest(lineData map[string]bigquery.JsonValue) {
	requestPieces := strings.Split(lineData["request"].(string), " ")

	if len(requestPieces) == 3 {
		lineData["method"] = requestPieces[0]

		if strings.Contains(requestPieces[1], "?") {
			pathPieces := strings.SplitAfter(requestPieces[1], "?")
			lineData["path"] = strings.Replace(pathPieces[0], "?", "", -1)
			lineData["query"] = pathPieces[1]
		} else {
			lineData["path"] = requestPieces[1]
		}
	}

	delete(lineData, "request")
}
