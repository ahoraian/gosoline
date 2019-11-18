package blob

import (
	"fmt"
	"github.com/applike/gosoline/pkg/cfg"
	"github.com/applike/gosoline/pkg/mdl"
	"github.com/applike/gosoline/pkg/mon"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"sync"
)

const (
	PrivateACL    = "private"
	PublicReadACL = "public-read"
)

type Object struct {
	Key    *string
	Body   Stream
	ACL    *string
	Exists bool
	Error  error

	bucket *string
	prefix *string
	wg     *sync.WaitGroup
}

type Batch []*Object

type Settings struct {
	Project     string
	Family      string
	Environment string
	Application string
	Bucket      string
	Prefix      string
}

//go:generate mockery -name Store
type Store interface {
	Read(batch Batch)
	ReadOne(obj *Object) error
	Write(batch Batch)
	WriteOne(obj *Object) error
}

type s3Store struct {
	logger mon.Logger
	runner *BatchRunner
	client s3iface.S3API

	bucket *string
	prefix *string
}

func NewStore(config cfg.Config, logger mon.Logger, settings Settings) *s3Store {
	runner := ProvideBatchRunner()
	client := ProvideS3Client(config)
	appId := &cfg.AppId{
		Project:     settings.Project,
		Environment: settings.Environment,
		Family:      settings.Family,
		Application: settings.Application,
	}
	appId.PadFromConfig(config)

	if settings.Bucket == "" {
		settings.Bucket = fmt.Sprintf("%s-%s-%s", appId.Project, appId.Environment, appId.Family)
	}

	settings.Prefix = fmt.Sprintf("%s/%s", appId.Application, settings.Prefix)

	store := NewStoreWithInterfaces(logger, runner, client, settings)

	autoCreate := config.GetBool("aws_s3_autoCreate")
	if autoCreate {
		store.CreateBucket()
	}

	return store
}

func NewStoreWithInterfaces(logger mon.Logger, runner *BatchRunner, client s3iface.S3API, settings Settings) *s3Store {
	return &s3Store{
		logger: logger,
		runner: runner,
		client: client,
		bucket: mdl.String(settings.Bucket),
		prefix: mdl.String(settings.Prefix),
	}
}

func (s *s3Store) CreateBucket() {
	_, err := s.client.CreateBucket(&s3.CreateBucketInput{
		Bucket: s.bucket,
	})

	if isBucketAlreadyExistsError(err) {
		s.logger.Infof("s3 bucket %s did already exist", *s.bucket)
	} else if err != nil {
		s.logger.Errorf(err, "could not create s3 bucket %s", *s.bucket)
	} else {
		s.logger.Infof("created s3 bucket %s", *s.bucket)
	}
}

func (s *s3Store) ReadOne(obj *Object) error {
	s.Read(Batch{obj})

	return obj.Error
}

func (s *s3Store) Read(batch Batch) {
	wg := &sync.WaitGroup{}
	wg.Add(len(batch))

	for i := 0; i < len(batch); i++ {
		batch[i].bucket = s.bucket
		batch[i].prefix = s.prefix
		batch[i].wg = wg
	}

	for i := 0; i < len(batch); i++ {
		s.runner.read <- batch[i]
	}

	wg.Wait()
}

func (s *s3Store) WriteOne(obj *Object) error {
	s.Write(Batch{obj})

	return obj.Error
}

func (s *s3Store) Write(batch Batch) {
	wg := &sync.WaitGroup{}
	wg.Add(len(batch))

	for i := 0; i < len(batch); i++ {
		batch[i].bucket = s.bucket
		batch[i].prefix = s.prefix
		batch[i].wg = wg
	}

	for i := 0; i < len(batch); i++ {
		s.runner.write <- batch[i]
	}

	wg.Wait()
}

func isBucketAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}

	if aerr, ok := err.(awserr.Error); ok {
		return aerr.Code() == s3.ErrCodeBucketAlreadyExists
	}

	return false
}
