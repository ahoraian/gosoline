package test

import (
	"fmt"
	"github.com/ory/dockertest"
	"log"
	"sync"
)

var err error
var wait sync.WaitGroup
var dockerPool *dockertest.Pool
var dockerResources []*dockertest.Resource
var cfgFilename = "config.test.yml"

func init() {
	dockerPool, err = dockertest.NewPool("")
	dockerResources = make([]*dockertest.Resource, 0)

	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
}

func logErr(err error, msg string) {
	Shutdown()
	log.Println(msg)
	log.Fatal(err)
}

func Boot(configFilenames ...string) {
	if len(configFilenames) == 0 {
		configFilenames = append(configFilenames, cfgFilename)
	}

	for _, filename := range configFilenames {
		log.Println(fmt.Sprintf("booting configuration %s", filename))
		bootFromFile(filename)
	}

	wait.Wait()

	log.Println("test environment up and running")
	fmt.Println()
}

func bootFromFile(filename string) {
	config := readConfig(filename)

	for name, mockConfig := range config.Mocks {
		bootComponent(name, mockConfig)
	}
}

func bootComponent(name string, mockConfig configInput) {
	component := mockConfig["component"]

	switch component {
	case "cloudwatch":
		runCloudwatch(name, mockConfig)
	case "dynamodb":
		runDynamoDb(name, mockConfig)
	case "elasticsearch":
		runElasticsearch(name, mockConfig)
	case "kinesis":
		runKinesis(name, mockConfig)
	case "mysql":
		runMysql(name, mockConfig)
	case "redis":
		runRedis(name, mockConfig)
	case "sns":
		runSns(name, mockConfig)
	case "sqs":
		runSqs(name, mockConfig)
	case "wiremock":
		runWiremock(name, mockConfig)
	default:
		err := fmt.Errorf("unknown component '%s'", component)
		logErr(err, err.Error())
	}
}

func Shutdown() {
	for _, res := range dockerResources {
		if err := dockerPool.Purge(res); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}

	dockerResources = make([]*dockertest.Resource, 0)
}