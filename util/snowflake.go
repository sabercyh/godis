package util

import (
	"math/rand"
	"sync"
	"time"

	"github.com/godis/errs"
	"github.com/rs/zerolog"
)

const (
	initTimeMillis int64 = 1717397640000
	workerIDBits   int64 = 6
	sequenceBits   int64 = 12

	workerIDLeft  int64 = sequenceBits
	timestampLeft int64 = sequenceBits + workerIDBits

	maxWorkerID int64 = -1 ^ (-1 << workerIDBits)
	maxSequence int64 = -1 ^ (-1 << sequenceBits)
)

var (
	snowFlake *SnowFlake
	once      sync.Once
)

type SnowFlake struct {
	log            zerolog.Logger
	workerID       int64
	sequence       int64
	lastTimeMillis int64
}

func NewSnowFlake(log zerolog.Logger, workerID int64) (*SnowFlake, error) {
	if snowFlake == nil {
		if workerID > maxWorkerID || workerID < 0 {
			log.Error().Int64("worker_id", workerID).Msg("get snowflake unique id fail of worker_id over max")
			return nil, errs.UnknownError
		}

		once.Do(func() {
			snowFlake = &SnowFlake{
				log:            log.With().Logger(),
				workerID:       workerID,
				sequence:       0,
				lastTimeMillis: 0,
			}
		})
	}
	return snowFlake, nil
}

func (s *SnowFlake) NextID() (int64, error) {
	currentTimeMillis := GetMsTime()
	if currentTimeMillis < s.lastTimeMillis {
		s.log.Error().Int64("currentTimeMillis", currentTimeMillis).Int64("lastTimeMillis", s.lastTimeMillis).Int64("workerID", s.workerID).Msg("get snowflake unique id fail of last_timestamp is not right")
		return 0, errs.UnknownError
	}
	if currentTimeMillis == s.lastTimeMillis {
		s.sequence = (s.sequence + 1) & maxSequence
		//自增后的序列号超过了最大值时，该值为0，则需要使用新的时间戳
		if s.sequence == 0 {
			time.Sleep(time.Millisecond)
			currentTimeMillis = GetMsTime()
		}
	} else {
		s.sequence = rand.Int63n(50) // 不在同一毫秒内，则起始序列号从[0,50]中重新随机开始
	}

	s.lastTimeMillis = currentTimeMillis

	id := (currentTimeMillis-initTimeMillis)<<timestampLeft | s.workerID<<workerIDLeft | s.sequence
	return id, nil
}
