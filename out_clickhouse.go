package main

import (
	"C"
	"database/sql"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/ugorji/go/codec"
	klog "k8s.io/klog"
)

var (
	client *sql.DB

	database  string
	table     string

	insertSQL = "INSERT INTO %s.%s(date, cluster, namespace, app, pod_name, container_name, host, log, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

const (
	DefaultWriteTimeout string = "20"
	DefaultReadTimeout  string = "10"
)

type Log struct {
	Cluster       string
	Namespace     string
	App           string
	Pod       string
	Container string
	Host          string
	Log           string
	Ts            time.Time
}

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "clickhouse", "Clickhouse Output Plugin.!")
}

//export FLBPluginInit
// ctx (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(ctx unsafe.Pointer) int {
	var host string
	if v := output.FLBPluginConfigKey(ctx, "Host"); v != "" {
		host = v
	} else {
		klog.Error("you must set host of clickhouse!")
		return output.FLB_ERROR
	}

	var user string
	if v := output.FLBPluginConfigKey(ctx, "User"); v != "" {
		user = v
	} else {
		klog.Error("you must set user of clickhouse!")
		return output.FLB_ERROR
	}

	var password string
	if v := output.FLBPluginConfigKey(ctx, "Password"); v != "" {
		password = v
	} else {
		klog.Error("you must set password of clickhouse!")
		return output.FLB_ERROR
	}

	if v := output.FLBPluginConfigKey(ctx, "Database"); v != "" {
		database = v
	} else {
		klog.Error("you must set database of clickhouse!")
		return output.FLB_ERROR
	}

	if v := output.FLBPluginConfigKey(ctx, "Table"); v != "" {
		table = v
	} else {
		klog.Error("you must set table of clickhouse!")
		return output.FLB_ERROR
	}

	var writeTimeout string
	if v := output.FLBPluginConfigKey(ctx, "Write_Timeout"); v != "" {
		writeTimeout = v
	} else {
		klog.Infof("you set the default write_timeout: %s", DefaultWriteTimeout)
		writeTimeout = DefaultWriteTimeout
	}

	var readTimeout string
	if v := output.FLBPluginConfigKey(ctx, "Read_Timeout"); v != "" {
		readTimeout = v
	} else {
		klog.Infof("you set the default read_timeout: %s", DefaultReadTimeout)
		readTimeout = DefaultReadTimeout
	}

	dsn := "tcp://" + host + "?username=" + user + "&password=" + password + "&database=" + database + "&write_timeout=" + writeTimeout + "&read_timeout=" + readTimeout

	client, err := sql.Open("clickhouse", dsn)
	if err != nil {
		klog.Error("connecting to clickhouse: %v", err)
		return output.FLB_ERROR
	}

	if err := client.Ping(); err != nil {
		klog.Errorf("Failed to ping clickhouse: %v", err)
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

//export FLBPluginFlush
// FLBPluginFlush is called from fluent-bit when data need to be sent. is called from fluent-bit when data need to be sent.
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	var h codec.Handle = new(codec.MsgpackHandle)

	var b []byte
	var m interface{}
	var err error

	b = C.GoBytes(data, length)
	dec := codec.NewDecoderBytes(b, h)

	var logs []*Log
	for {
		// decode the msgpack data
		err = dec.Decode(&m)
		if err != nil {
			break
		}

		// Get a slice and their two entries: timestamp and map
		slice := reflect.ValueOf(m)
		timestampData := slice.Index(0)
		data := slice.Index(1)

		timestamp, ok := timestampData.Interface().(uint64)
		if !ok {
			klog.Errorf("Unable to convert timestamp: %+v", timestampData)
			return output.FLB_ERROR
		}

		// Convert slice data to a real map and iterate
		mapData := data.Interface().(map[interface{}]interface{})
		flattenData, err := Flatten(mapData, "", UnderscoreStyle)
		if err != nil {
			break
		}

		log := &Log{}
		for k, v := range flattenData {
			value := ""
			switch t := v.(type) {
			case string:
				value = t
			case []byte:
				value = string(t)
			default:
				value = fmt.Sprintf("%v", v)
			}

			switch k {
			case "cluster":
				log.Cluster = value
			case "namespace_name":
				log.Namespace = value
			case "app":
				log.App = value
			case "k8s-app":
				log.App = value
			case "pod_name":
				log.Pod = value
			case "container_name":
				log.Container = value
			case "host":
				log.Host = value
			case "log":
				log.Log = value
			}

		}

		if log.App == "" {
			break
		}

		log.Ts = time.Unix(int64(timestamp), 0)

		logs = append(logs, log)
	}

	sql := fmt.Sprintf(insertSQL, database, table)

	start := time.Now()
	// post them to db all at once
	tx, err := client.Begin()
	if err != nil {
		klog.Errorf("begin transaction failure: %s", err.Error())
		return output.FLB_ERROR
	}

	// build statements
	smt, err := tx.Prepare(sql)
	if err != nil {
		klog.Errorf("prepare statement failure: %s", err.Error())
		return output.FLB_ERROR
	}
	for _, l := range logs {
		// ensure tags are inserted in the same order each time
		// possibly/probably impacts indexing?
		_, err = smt.Exec(l.Ts, l.Cluster, l.Namespace, l.App, l.Pod, l.Container, l.Host,
			l.Log, l.Ts)

		if err != nil {
			klog.Errorf("statement exec failure: %s", err.Error())
			return output.FLB_ERROR
		}
	}

	// commit and record metrics
	if err = tx.Commit(); err != nil {
		klog.Errorf("commit failed failure: %s", err.Error())
		return output.FLB_ERROR
	}

	end := time.Now()
	klog.Infof("Exported %d log to clickhouse in %s", len(logs), end.Sub(start))

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}
