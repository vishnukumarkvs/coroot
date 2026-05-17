package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/coroot/coroot/cache/chunk"
	"github.com/coroot/coroot/db"
	"github.com/coroot/coroot/model"
	"github.com/coroot/coroot/timeseries"
	"github.com/redis/go-redis/v9"
	"k8s.io/klog"
)

const redisKeyNamespace = "coroot"

func newRedisClient(redisURL string) (*redis.Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis url: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}
	return rdb, nil
}

type redisStore struct {
	client *redis.Client
	ttl    time.Duration
}

func (s *redisStore) chunkKey(projectID db.ProjectId, queryHash string, from timeseries.Time) string {
	return fmt.Sprintf("%s:chunk:%s:%s:%d", redisKeyNamespace, projectID, queryHash, from)
}

func (s *redisStore) indexKey(projectID db.ProjectId, queryHash string) string {
	return fmt.Sprintf("%s:idx:%s:%s", redisKeyNamespace, projectID, queryHash)
}

func (s *redisStore) stateKey(projectID db.ProjectId) string {
	return fmt.Sprintf("%s:state:%s", redisKeyNamespace, projectID)
}

func (s *redisStore) stepKey(projectID db.ProjectId) string {
	return fmt.Sprintf("%s:step:%s", redisKeyNamespace, projectID)
}

type stateValue struct {
	Query     string          `json:"query"`
	LastTs    timeseries.Time `json:"last_ts"`
	LastError string          `json:"last_error"`
}

func (s *redisStore) writeChunk(projectID db.ProjectId, queryHash string, from timeseries.Time, pointsCount int, step timeseries.Duration, finalized bool, metrics []*model.MetricValues) error {
	var buf bytes.Buffer
	if err := chunk.Write(&buf, from, pointsCount, step, finalized, metrics); err != nil {
		return err
	}
	ctx := context.Background()
	key := s.chunkKey(projectID, queryHash, from)
	if err := s.client.Set(ctx, key, buf.Bytes(), s.ttl).Err(); err != nil {
		return fmt.Errorf("failed to store chunk in redis: %w", err)
	}
	idxKey := s.indexKey(projectID, queryHash)
	member := strconv.FormatInt(int64(from), 10)
	if err := s.client.ZAdd(ctx, idxKey, redis.Z{Score: float64(from), Member: member}).Err(); err != nil {
		return fmt.Errorf("failed to update chunk index: %w", err)
	}
	return nil
}

func (s *redisStore) readChunks(projectID db.ProjectId, queryHash string, from, to timeseries.Time, step timeseries.Duration, pointsCount int, fillFunc timeseries.FillFunc) (map[uint64]*model.MetricValues, error) {
	ctx := context.Background()
	idxKey := s.indexKey(projectID, queryHash)
	members, err := s.client.ZRangeWithScores(ctx, idxKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list chunks from redis: %w", err)
	}
	res := map[uint64]*model.MetricValues{}
	for _, m := range members {
		chunkFrom := timeseries.Time(m.Score)
		if chunkFrom > to || chunkFrom.Add(timeseries.Duration(chunk.Size)) < from {
			continue
		}
		key := s.chunkKey(projectID, queryHash, chunkFrom)
		data, err := s.client.Get(ctx, key).Bytes()
		if err != nil {
			klog.Errorln("failed to read chunk from redis:", err)
			continue
		}
		if err := chunk.ReadFrom(bytes.NewReader(data), from, pointsCount, step, res, fillFunc); err != nil {
			klog.Errorln("failed to parse chunk from redis:", err)
			continue
		}
	}
	return res, nil
}

func (s *redisStore) getMaxStep(projectID db.ProjectId) (timeseries.Duration, error) {
	ctx := context.Background()
	key := s.stepKey(projectID)
	v, err := s.client.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return timeseries.Duration(v), nil
}

func (s *redisStore) setStep(projectID db.ProjectId, step timeseries.Duration) error {
	ctx := context.Background()
	key := s.stepKey(projectID)
	return s.client.Set(ctx, key, int64(step), s.ttl).Err()
}

func (s *redisStore) deleteChunks(projectID db.ProjectId, queryHash string, fromList []timeseries.Time) error {
	ctx := context.Background()
	idxKey := s.indexKey(projectID, queryHash)
	for _, from := range fromList {
		key := s.chunkKey(projectID, queryHash, from)
		if err := s.client.Del(ctx, key).Err(); err != nil {
			return err
		}
		member := strconv.FormatInt(int64(from), 10)
		if err := s.client.ZRem(ctx, idxKey, member).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (s *redisStore) saveState(projectID db.ProjectId, qs *PrometheusQueryState) error {
	ctx := context.Background()
	key := s.stateKey(projectID)
	sv := stateValue{
		Query:     qs.Query,
		LastTs:    qs.LastTs,
		LastError: qs.LastError,
	}
	data, err := json.Marshal(sv)
	if err != nil {
		return err
	}
	return s.client.HSet(ctx, key, queryHash(qs.Query), string(data)).Err()
}

func (s *redisStore) loadStates(projectID db.ProjectId) (map[string]*PrometheusQueryState, error) {
	ctx := context.Background()
	key := s.stateKey(projectID)
	result, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	res := map[string]*PrometheusQueryState{}
	for _, v := range result {
		var sv stateValue
		if err := json.Unmarshal([]byte(v), &sv); err != nil {
			klog.Errorln("failed to unmarshal state:", err)
			continue
		}
		res[sv.Query] = &PrometheusQueryState{
			ProjectId: projectID,
			Query:     sv.Query,
			LastTs:    sv.LastTs,
			LastError: sv.LastError,
		}
	}
	return res, nil
}

func (s *redisStore) deleteState(projectID db.ProjectId, query string) error {
	ctx := context.Background()
	key := s.stateKey(projectID)
	return s.client.HDel(ctx, key, queryHash(query)).Err()
}

func (s *redisStore) getMinUpdateTime(projectID db.ProjectId) (timeseries.Time, error) {
	states, err := s.loadStates(projectID)
	if err != nil {
		return 0, err
	}
	var min timeseries.Time
	first := true
	for _, st := range states {
		if first || st.LastTs < min {
			min = st.LastTs
			first = false
		}
	}
	return min, nil
}

func (s *redisStore) getMinUpdateTimeWithoutRecordingRules(projectID db.ProjectId) (timeseries.Time, error) {
	states, err := s.loadStates(projectID)
	if err != nil {
		return 0, err
	}
	var min timeseries.Time
	first := true
	for _, st := range states {
		if len(st.Query) >= 3 && st.Query[:3] == "rr_" {
			continue
		}
		if first || st.LastTs < min {
			min = st.LastTs
			first = false
		}
	}
	return min, nil
}

func (s *redisStore) getStatus(projectID db.ProjectId) (*Status, error) {
	states, err := s.loadStates(projectID)
	if err != nil {
		return nil, err
	}
	st := &Status{}
	now := timeseries.Now()
	var maxLag, avgLag, count float64
	for _, qs := range states {
		if st.Error == "" && qs.LastError != "" {
			st.Error = qs.LastError
		}
		lag := float64(now - qs.LastTs)
		if lag > maxLag {
			maxLag = lag
		}
		avgLag += lag
		count++
	}
	if count > 0 {
		st.LagMax = timeseries.Duration(maxLag)
		st.LagAvg = timeseries.Duration(avgLag / count)
	} else {
		st.LagMax = BackFillInterval
		st.LagAvg = BackFillInterval
	}
	return st, nil
}
