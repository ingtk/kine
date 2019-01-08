package stimulus

import (
	"fmt"
	"math/big"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/olekukonko/tablewriter"
)

type Stimulus struct {
	svc      *kinesis.Kinesis
	session  *session.Session
	endpoint string
	region   string
}

type StimulusOption interface {
	Apply(s *Stimulus) error
}

type OptionFn func(*Stimulus) error

func (o OptionFn) Apply(s *Stimulus) error {
	return o(s)
}

func WithRegion(region string) StimulusOption {
	return OptionFn(func(s *Stimulus) error {
		s.region = region
		return nil
	})
}

func WithEndpoint(endpoint string) StimulusOption {
	return OptionFn(func(s *Stimulus) error {
		s.endpoint = endpoint
		return nil
	})
}

func New(opts ...StimulusOption) (*Stimulus, error) {
	s := &Stimulus{}
	for _, o := range opts {
		err := o.Apply(s)
		if err != nil {
			return nil, err
		}
	}

	if s.session == nil {
		conf := &aws.Config{}
		if s.endpoint != "" {
			conf = conf.WithEndpoint(s.endpoint)
		}
		if s.region != "" {
			conf = conf.WithRegion(s.region)
		}
		s.session = session.New(conf)
	}

	s.svc = kinesis.New(s.session)

	return s, nil
}

const (
	defaultWaitSecond time.Duration = 5 * time.Second

	maxPartitionKey = "340282366920938463463374607431768211456"
)

func (s *Stimulus) AWSKinesis() *kinesis.Kinesis {
	return s.svc
}

func (s *Stimulus) HalveShard(streamName string) error {

	var stream *kinesis.DescribeStreamOutput

	// Wait until active (if returnIfNotActive is false)
	for {
		var err error
		stream, err = s.svc.DescribeStream(&kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			return err
		}
		if *stream.StreamDescription.StreamStatus == kinesis.StreamStatusActive {
			break
		}
		time.Sleep(5 * time.Second)
	}

	shards := filterOpenShards(stream.StreamDescription.Shards, true)

	if len(shards) == 1 {
		return nil
	}

	for i := 0; i < len(shards); i += 2 {
		params := &kinesis.MergeShardsInput{
			AdjacentShardToMerge: shards[i+1].ShardId,    // Required
			ShardToMerge:         shards[i].ShardId,      // Required
			StreamName:           aws.String(streamName), // Required
		}
		_, err := s.svc.MergeShards(params)
		if err != nil {
			return err
		}

		// Wait until active
		for {
			stream, err := s.svc.DescribeStream(&kinesis.DescribeStreamInput{
				StreamName: aws.String(streamName),
			})
			if err != nil {
				return err
			}
			if *stream.StreamDescription.StreamStatus == kinesis.StreamStatusActive {
				break
			}
			time.Sleep(5 * time.Second)
		}

		err = s.View(streamName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Stimulus) DoubleShard(streamName string) error {

	var stream *kinesis.DescribeStreamOutput

	// Wait until active (if returnIfNotActive is false)
	for {
		var err error
		stream, err = s.svc.DescribeStream(&kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			return err
		}
		if *stream.StreamDescription.StreamStatus == kinesis.StreamStatusActive {
			break
		}
		time.Sleep(5 * time.Second)
	}

	shards := filterOpenShards(stream.StreamDescription.Shards, false)

	for _, shard := range shards {
		newStartingHashKey := calcNewStartingHashKey(
			*shard.HashKeyRange.StartingHashKey,
			*shard.HashKeyRange.EndingHashKey,
		)

		params := &kinesis.SplitShardInput{
			NewStartingHashKey: aws.String(newStartingHashKey),
			ShardToSplit:       shard.ShardId,
			StreamName:         aws.String(streamName),
		}
		_, err := s.svc.SplitShard(params)
		if err != nil {
			return err
		}

		// Wait until active
		for {
			stream, err := s.svc.DescribeStream(&kinesis.DescribeStreamInput{
				StreamName: aws.String(streamName),
			})
			if err != nil {
				return err
			}
			if *stream.StreamDescription.StreamStatus == kinesis.StreamStatusActive {
				break
			}
			time.Sleep(5 * time.Second)
		}

		err = s.View(streamName)
		if err != nil {
			return err
		}
	}

	return nil
}

func calcNewStartingHashKey(startingHashKey, endingHashKey string) string {

	skey, _ := big.NewInt(0).SetString(startingHashKey, 10)
	ekey, _ := big.NewInt(0).SetString(endingHashKey, 10)

	newStartingHashKey := big.NewInt(0)
	newStartingHashKey.Add(skey, ekey).Div(newStartingHashKey, big.NewInt(2))

	return newStartingHashKey.String()
}

func filterOpenShards(shards []*kinesis.Shard, sorted bool) []*kinesis.Shard {
	filtered := make([]*kinesis.Shard, 0, len(shards))
	i := 0
	for _, shard := range shards {
		i++
		if shard.SequenceNumberRange.EndingSequenceNumber == nil {
			filtered = append(filtered, shard)
		}
	}

	if !sorted {
		return filtered
	}
	sort.Slice(filtered, func(i, j int) bool {
		endingHashKey1 := big.NewInt(0)
		endingHashKey2 := big.NewInt(0)
		endingHashKey1.SetString(*filtered[i].HashKeyRange.EndingHashKey, 10)
		endingHashKey2.SetString(*filtered[j].HashKeyRange.EndingHashKey, 10)
		return endingHashKey1.Cmp(endingHashKey2) < 0
	})

	return filtered
}

func (s *Stimulus) View(streamName string) error {

	var stream *kinesis.DescribeStreamOutput

	for {
		var err error
		stream, err = s.svc.DescribeStream(&kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err != nil {
			return err
		}
		if *stream.StreamDescription.StreamStatus == kinesis.StreamStatusActive {
			break
		}
	}

	maxHashKey, _ := big.NewInt(0).SetString(maxPartitionKey, 10)

	table := tablewriter.NewWriter(os.Stdout)

	data := make([][]string, 0)

	shards := filterOpenShards(stream.StreamDescription.Shards, false)

	for _, s := range shards {
		skey, _ := big.NewInt(0).SetString(*s.HashKeyRange.StartingHashKey, 10)
		ekey, _ := big.NewInt(0).SetString(*s.HashKeyRange.EndingHashKey, 10)

		diff := big.NewInt(0).Sub(ekey, skey)
		r := big.NewRat(1, 1).SetFrac(diff, maxHashKey)
		v, _ := r.Float32()
		data = append(data, []string{*s.ShardId, fmt.Sprintf("%.2f %%", (v * 100.0))})
	}

	table.AppendBulk(data)
	table.Render()

	return nil
}
