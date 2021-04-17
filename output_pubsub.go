package main

import (
	"C"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
	"unsafe"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/satori/go.uuid"
)
import "os"

var (
	plugins    = make([]Keeper, 0, 3)
	hostname   string
	wrapper    = OutputWrapper(&Output{})
	attributes map[string]string

	timeout        = pubsub.DefaultPublishSettings.Timeout
	delayThreshold = pubsub.DefaultPublishSettings.DelayThreshold
	countThreshold = pubsub.DefaultPublishSettings.CountThreshold
	byteThreshold  = pubsub.DefaultPublishSettings.ByteThreshold
	debug          = false
	jsonEncode     = false
	uuId           = false
)

type Output struct{}

type OutputWrapper interface {
	Register(ctx unsafe.Pointer, name string, desc string) int
	GetConfigKey(ctx unsafe.Pointer, key string) string
	NewDecoder(data unsafe.Pointer, length int) *output.FLBDecoder
	GetRecord(dec *output.FLBDecoder) (ret int, ts interface{}, rec map[interface{}]interface{})
}

func (o *Output) Register(ctx unsafe.Pointer, name string, desc string) int {
	return output.FLBPluginRegister(ctx, name, desc)
}

func (o *Output) GetConfigKey(ctx unsafe.Pointer, key string) string {
	return output.FLBPluginConfigKey(ctx, key)
}

func (o *Output) NewDecoder(data unsafe.Pointer, length int) *output.FLBDecoder {
	return output.NewDecoder(data, length)
}

func (o *Output) GetRecord(dec *output.FLBDecoder) (ret int, ts interface{}, rec map[interface{}]interface{}) {
	return output.GetRecord(dec)
}

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return wrapper.Register(ctx, "pubsub", "output pubsub")
}

//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	var err error
	project := wrapper.GetConfigKey(ctx, "Project")
	topic := wrapper.GetConfigKey(ctx, "Topic")
	jwtPath := wrapper.GetConfigKey(ctx, "JwtPath")
	dg := wrapper.GetConfigKey(ctx, "Debug")
	to := wrapper.GetConfigKey(ctx, "Timeout")
	bt := wrapper.GetConfigKey(ctx, "ByteThreshold")
	ct := wrapper.GetConfigKey(ctx, "CountThreshold")
	dt := wrapper.GetConfigKey(ctx, "DelayThreshold")
	je := wrapper.GetConfigKey(ctx, "JSONEncode")
	attr := wrapper.GetConfigKey(ctx, "Attributes")
	uuid := wrapper.GetConfigKey(ctx, "Uuid")

	fmt.Printf("[pubsub-go] plugin parameter project = '%s'\n", project)
	fmt.Printf("[pubsub-go] plugin parameter topic = '%s'\n", topic)
	fmt.Printf("[pubsub-go] plugin parameter jwtPath = '%s'\n", jwtPath)
	fmt.Printf("[pubsub-go] plugin parameter debug = '%s'\n", dg)
	fmt.Printf("[pubsub-go] plugin parameter timeout = '%s'\n", to)
	fmt.Printf("[pubsub-go] plugin parameter byte threshold = '%s'\n", bt)
	fmt.Printf("[pubsub-go] plugin parameter count threshold = '%s'\n", ct)
	fmt.Printf("[pubsub-go] plugin parameter delay threshold = '%s'\n", dt)
	fmt.Printf("[pubsub-go] plugin parameter jsonEncode = '%s'\n", je)
	fmt.Printf("[pubsub-go] plugin parameter attributes = '%s'\n", attr)
	fmt.Printf("[pubsub-go] plugin parameter uuid = '%s'\n", uuid)

	hostname, err = os.Hostname()
	if err != nil {
		fmt.Printf("[err][init] %+v\n", err)
		return output.FLB_ERROR
	}

	fmt.Printf("[pubsub-go] plugin hostname = '%s'\n", hostname)

	if dg != "" {
		debug, err = strconv.ParseBool(dg)
		if err != nil {
			fmt.Printf("[err][init] %+v\n", err)
			return output.FLB_ERROR
		}
	}
	if to != "" {
		v, err := strconv.Atoi(to)
		if err != nil {
			fmt.Printf("[err][init] %+v\n", err)
			return output.FLB_ERROR
		}
		timeout = time.Duration(v) * time.Millisecond
	}
	if bt != "" {
		v, err := strconv.Atoi(bt)
		if err != nil {
			fmt.Printf("[err][init] %+v\n", err)
			return output.FLB_ERROR
		}
		byteThreshold = v
	}
	if ct != "" {
		v, err := strconv.Atoi(ct)
		if err != nil {
			fmt.Printf("[err][init] %+v\n", err)
			return output.FLB_ERROR
		}
		countThreshold = v
	}
	if dt != "" {
		v, err := strconv.Atoi(dt)
		if err != nil {
			fmt.Printf("[err][init] %+v\n", err)
			return output.FLB_ERROR
		}
		delayThreshold = time.Duration(v) * time.Millisecond
	}
	if je != "" {
		jsonEncode, err = strconv.ParseBool(je)
		if err != nil {
			fmt.Printf("[err][init] %+v\n", err)
			return output.FLB_ERROR
		}
	}
	if attr != "" {
		attributes = make(map[string]string)
		err := json.Unmarshal([]byte(attr), &attributes)
		if err != nil {
			fmt.Printf("[err][init] %+v\n", err)
			return output.FLB_ERROR
		}
	}
	if uuid != "" {
		uuId, err = strconv.ParseBool(uuid)
		if err != nil {
			fmt.Printf("[err][init] %+v\n", err)
			return output.FLB_ERROR
		}
	}
	publishSetting := pubsub.PublishSettings{
		ByteThreshold:  byteThreshold,
		CountThreshold: countThreshold,
		DelayThreshold: delayThreshold,
		Timeout:        timeout,
	}

	keeper, err := NewKeeper(project, topic, jwtPath, &publishSetting)
	if err != nil {
		fmt.Printf("[err][init] %+v\n", err)
		return output.FLB_ERROR
	}
	plugins = append(plugins, keeper)
	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	var mutex = &sync.Mutex{}
	ctx := context.Background()
	tagname := ""
	if tag == nil {
		tagname = C.GoString(tag)
	}
	// Create Fluent Bit decoder
	dec := wrapper.NewDecoder(data, int(length))
	var results []*pubsub.PublishResult
	// Iterate Records
	for {
		// Extract Record
		ret, ts, record := wrapper.GetRecord(dec)
		if ret != 0 { // don't rest
			break
		}
		timestamp := ts.(output.FLBTime)
		record["timestamp"] = []byte(timestamp.String())

		if tagname != "" {
			record["tag"] = tagname
		}
		if uuId  {
			if (len(attributes) == 0) {
				attributes = make(map[string]string)
			}
			attributes["uuid"] = uuid.Must(uuid.NewV4(), nil).String()
		}
		mutex.Lock()
		results = make([]*pubsub.PublishResult, 0, len(record))
		mutex.Unlock()

		if !jsonEncode {
			for k, v := range record {
				if debug {
					fmt.Printf("[%s] %s %s %v \n", tagname, timestamp.String(), k, v)
				}
				for _, plugin := range plugins {
					results = append(results, plugin.Send(ctx, v.([]byte), attributes))
				}
			}
		} else {
			rec := make(map[string]interface{}, len(record))
			for k, v := range record {
				key := k.(string)
				value := string(v.([]byte))
				mutex.Lock()
				rec[key] = value
				mutex.Unlock()
			}
			recordJSON, err := json.Marshal(rec)
			if err != nil {
				fmt.Printf("[err][publish][retry] %+v \n", err)
				return output.FLB_ERROR
			}
			if debug {
				fmt.Printf("[%s] %s %s \n", tagname, timestamp.String(), string(recordJSON))
			}
			for _, plugin := range plugins {
				results = append(results, plugin.Send(ctx, recordJSON, attributes))
			}
		}
	}

	for _, result := range results {
		if _, err := result.Get(ctx); err != nil {
			// if timeout is raised.
			if err == context.DeadlineExceeded || err == context.Canceled {
				fmt.Printf("[err][publish][retry] %+v \n", err)
				return output.FLB_RETRY
			}
			// else error is next
			fmt.Printf("[err][publish][don't retry] %+v \n", err)
		}
	}
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	for _, plugin := range plugins {
		plugin.Stop()
	}
	return output.FLB_OK
}

func main() {}
