package kine

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

type Kine struct {
	svc      *kinesis.Kinesis
	session  *session.Session
	endpoint string
	region   string
}

type KineOption interface {
	Apply(k *Kine) error
}

type OptionFn func(*Kine) error

func (o OptionFn) Apply(k *Kine) error {
	return o(k)
}

func WithRegion(region string) KineOption {
	return OptionFn(func(k *Kine) error {
		k.region = region
		return nil
	})
}

func WithEndpoint(endpoint string) KineOption {
	return OptionFn(func(k *Kine) error {
		k.endpoint = endpoint
		return nil
	})
}

func New(opts ...KineOption) (*Kine, error) {
	k := &Kine{}
	for _, o := range opts {
		err := o.Apply(k)
		if err != nil {
			return nil, err
		}
	}

	if k.session == nil {
		conf := &aws.Config{}
		if k.endpoint != "" {
			conf = conf.WithEndpoint(k.endpoint)
		}
		if k.region != "" {
			conf = conf.WithRegion(k.region)
		}
		k.session = session.New(conf)
	}

	k.svc = kinesis.New(k.session)

	return k, nil
}

const (
	defaultWaitSecond time.Duration = 5 * time.Second

	maxPartitionKey = "340282366920938463463374607431768211456"
)

func (k *Kine) AWSKinesis() *kinesis.Kinesis {
	return k.svc
}

// 全シャード取得してから返す
func (k *Kine) DescribeStream(streamName string) (*kinesis.StreamDescription, error) {

	var stream *kinesis.DescribeStreamOutput

	var shards []*kinesis.Shard
	var startShardID *string

	for {
		var err error
		stream, err = k.svc.DescribeStream(&kinesis.DescribeStreamInput{
			ExclusiveStartShardId: startShardID,
			StreamName:            aws.String(streamName),
		})
		if err != nil {
			return nil, err
		}

		sd := stream.StreamDescription

		// activeになるまでは最初から再度読み込み続ける
		if *sd.StreamStatus != kinesis.StreamStatusActive {
			// 途中まで読み込んでても最初から読み込み直す
			startShardID = nil
			shards = make([]*kinesis.Shard, 0)
		} else {
			shards = append(shards, sd.Shards...)
			if *sd.HasMoreShards == true {
				startShardID = sd.Shards[len(sd.Shards)-1].ShardId
			} else {
				break
			}
		}

		time.Sleep(5 * time.Second)
	}

	stream.StreamDescription.Shards = shards

	return stream.StreamDescription, nil
}

func (k *Kine) HalveShard(streamName string) error {

	stream, err := k.DescribeStream(streamName)
	if err != nil {
		return err
	}

	shards := filterOpenShards(stream.Shards, true)

	if len(shards) == 1 {
		return nil
	}

	for i := 0; i < len(shards); i += 2 {
		params := &kinesis.MergeShardsInput{
			AdjacentShardToMerge: shards[i+1].ShardId,    // Required
			ShardToMerge:         shards[i].ShardId,      // Required
			StreamName:           aws.String(streamName), // Required
		}
		_, err := k.svc.MergeShards(params)
		if err != nil {
			return err
		}

		err = k.View(streamName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (k *Kine) DoubleShard(streamName string) error {

	stream, err := k.DescribeStream(streamName)
	if err != nil {
		return err
	}

	shards := filterOpenShards(stream.Shards, false)

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
		_, err := k.svc.SplitShard(params)
		if err != nil {
			return err
		}

		// Wait until active
		for {
			stream, err := k.svc.DescribeStreamSummary(&kinesis.DescribeStreamSummaryInput{
				StreamName: aws.String(streamName),
			})
			if err != nil {
				return err
			}
			if *stream.StreamDescriptionSummary.StreamStatus == kinesis.StreamStatusActive {
				break
			}
			time.Sleep(5 * time.Second)
		}

		err = k.View(streamName)
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

func (k *Kine) View(streamName string) error {

	maxHashKey, _ := big.NewInt(0).SetString(maxPartitionKey, 10)

	table := tablewriter.NewWriter(os.Stdout)

	data := make([][]string, 0)

	stream, err := k.DescribeStream(streamName)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	openShards := filterOpenShards(stream.Shards, false)

	for _, s := range openShards {
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
