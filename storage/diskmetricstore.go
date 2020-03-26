// Copyright 2014 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	dto "github.com/prometheus/client_model/go"
)

const (
	pushMetricName       = "push_time_seconds"
	pushMetricHelp       = "Last Unix time when changing this group in the Pushgateway succeeded."
	pushFailedMetricName = "push_failure_time_seconds"
	pushFailedMetricHelp = "Last Unix time when changing this group in the Pushgateway failed."
	writeQueueCapacity   = 1000
)

var errTimestamp = errors.New("pushed metrics must not have timestamps")

// DiskMetricStore is an implementation of MetricStore that persists metrics to disk.
//
// DiskMetricStore 是 MetricStore 的一种实现，它将指标持久化到磁盘。
type DiskMetricStore struct {
	lock            sync.RWMutex 				// Protects metricFamilies.
	writeQueue      chan WriteRequest			//
	drain           chan struct{}				//
	done            chan error					//
	metricGroups    GroupingKeyToMetricGroup	// map[groupKey] => MetricGroup, groupKey is a unique key build from labels.
	persistenceFile string						//
	predefinedHelp  map[string]string			//
	logger          log.Logger					//
}



type mfStat struct {
	pos    int  // Where in the result slice is the MetricFamily?
	copied bool // Has the MetricFamily already been copied?
}



// NewDiskMetricStore returns a DiskMetricStore ready to use. To cleanly shut it
// down and free resources, the Shutdown() method has to be called.
//
// NewDiskMetricStore 返回初始化后的 DiskMetricStore。
// 要彻底关闭它并释放资源，必须调用 Shutdown() 方法。



// If persistenceFile is the empty string, no persisting to disk will
// happen. Otherwise, a file of that name is used for persisting metrics to
// disk. If the file already exists, metrics are read from it as part of the
// start-up. Persisting is happening upon shutdown and after every write action,
// but the latter will only happen persistenceDuration after the previous
// persisting.
//
// 如果 persistenceFile 是空字符串，则不会发生持久化到磁盘。
// 否则，以 persistenceFile 为名称的文件用于将指标持久保存到磁盘。
// 如果该文件已存在，则会在启动时从其中读取 metrics 。
// 在关机和每次写入操作后都会发生持久化，但后者只会在距离上一次持续化 persistenceDuration 时间间隔之后发生。


// If a non-nil Gatherer is provided, the help strings of metrics gathered by it
// will be used as standard. Pushed metrics with deviating help strings will be
// adjusted to avoid inconsistent expositions.
//
// ？？？

func NewDiskMetricStore(
	persistenceFile string,
	persistenceInterval time.Duration,
	gatherPredefinedHelpFrom prometheus.Gatherer,
	logger log.Logger,
) *DiskMetricStore {

	// TODO: Do that outside of the constructor to allow the HTTP server to serve /-/healthy and /-/ready earlier.

	dms := &DiskMetricStore{
		writeQueue:      make(chan WriteRequest, writeQueueCapacity),
		drain:           make(chan struct{}),
		done:            make(chan error),
		metricGroups:    GroupingKeyToMetricGroup{},
		persistenceFile: persistenceFile,
		logger:          logger,
	}

	// 从 persistenceFile 中反序列化并保存到 metricGroups 成员变量中
	if err := dms.restore(); err != nil {
		level.Error(logger).Log("msg", "could not load persisted metrics", "err", err)
	}


	if helpStrings, err := extractPredefinedHelpStrings(gatherPredefinedHelpFrom); err == nil {
		dms.predefinedHelp = helpStrings
	} else {
		level.Error(logger).Log("msg", "could not gather metrics for predefined help strings", "err", err)
	}

	//
	go dms.loop(persistenceInterval)
	return dms
}


// SubmitWriteRequest implements the MetricStore interface.
func (dms *DiskMetricStore) SubmitWriteRequest(req WriteRequest) {
	dms.writeQueue <- req
}

// Shutdown implements the MetricStore interface.
func (dms *DiskMetricStore) Shutdown() error {
	//1. 关闭 drain 管道，来通知 loop goroutine 主动退出
	close(dms.drain)
	//2. 读取 done 管道，阻塞式等待 loop goroutine 完成退出
	return <-dms.done
}

// Healthy implements the MetricStore interface.
func (dms *DiskMetricStore) Healthy() error {
	// By taking the lock we check that there is no deadlock.
	dms.lock.Lock()
	defer dms.lock.Unlock()

	// A pushgateway that cannot be written to should not be considered as healthy.
	//
	// 检查写管道是否已满，如果已满意味着 metrics 上报速度太快，而 processWriteRequest() 处理不过来。
	if len(dms.writeQueue) == cap(dms.writeQueue) {
		return fmt.Errorf("write queue is full")
	}

	return nil
}

// Ready implements the MetricStore interface.
func (dms *DiskMetricStore) Ready() error {
	return dms.Healthy()
}

// GetMetricFamilies implements the MetricStore interface.
func (dms *DiskMetricStore) GetMetricFamilies() []*dto.MetricFamily {

	dms.lock.RLock()
	defer dms.lock.RUnlock()

	// 这个数组用于存储按照 metricName 聚合后的 dto.MetricFamily。
	result := []*dto.MetricFamily{}

	// 注意，
	//
	// dms.metricGroups 是 groupKey 到 MetricGroup 的映射，
	// MetricGroup 是一组具有相同 labels 的 MetricFamily 的集合。
	// MetricFamily 是一组具有相同名称、描述、类型的 metrics 集合。

	// 不同的 groupKey 下可能存在相同的 MetricFamily，现在需要遍历每个 MetricGroup 下的每个 MetricFamily，
	// 将同名的 MetricFamily 聚合保存到 result 的同一个元素上（位置为pos）。
	//
	// 为了方便聚合，用 mfStatByName 保存 name => pos 的映射。
	mfStatByName := map[string]mfStat{}


	// 遍历 dms.metricGroups 中的所有 group [ groupKey => MetricGroup ]
	for _, group := range dms.metricGroups {

		// 遍历 group 下的所有 MetricFamily
		for name, tmf := range group.Metrics {

			// 获取 MetricFamily，为 nil 则跳过
			metricFamily := tmf.GetMetricFamily()
			if metricFamily == nil {
				level.Warn(dms.logger).Log("msg", "storage corruption detected, consider wiping the persistence file")
				continue
			}

			// 判断 MetricFamily 是否已经存在于 mfStatByName 中
			stat, exists := mfStatByName[name]

			// 如果已经存在，则这个 MetricFamily 的聚合信息保存在 result[stat.pos] 上，
			if exists {

				// 取出 result[stat.pos] 上的 MetricFamily
				existingMF := result[stat.pos]

				// 判断 MetricFamily 是否已经被复制？ 如果未被复制(false)，就执行一次复制（不太懂。。。）
				if !stat.copied {

					// 重新保存新 mfStat 结构体
					mfStatByName[name] = mfStat{
						pos:    stat.pos,
						copied: true,
					}

					// 复制？
					existingMF = copyMetricFamily(existingMF)

					// 重新保存新 MetricFamily 结构体
					result[stat.pos] = existingMF
				}


				// help 字符串一致性检查
				if metricFamily.GetHelp() != existingMF.GetHelp() {
					level.Info(dms.logger).Log("msg", "metric families inconsistent help strings", "err", "Metric families have inconsistent help strings. The latter will have priority. This is bad. Fix your pushed metrics!", "new", mf, "old", existingMF)
				}


				// Type inconsistency cannot be fixed here.
				// We will detect it during gathering anyway, so no reason to log anything here.
				//
				// 所谓聚合操作：将当前 metricFamily.Metric 追加到 existingMF.Metric 中。
				existingMF.Metric = append(existingMF.Metric, metricFamily.Metric...)


			} else {

				// 如果当前 metrics 尚未被保存到 mfStatByName 中
				copied := false

				// help 字符串一致性检查，如果不一致，需要复制并重新设置 help 字符串
				if help, ok := dms.predefinedHelp[name]; ok && metricFamily.GetHelp() != help {
					level.Info(dms.logger).Log("msg", "metric families overlap", "err", "Metric family has the same name as a metric family used by the Pushgateway itself but it has a different help string. Changing it to the standard help string. This is bad. Fix your pushed metrics!", "metric_family", mf, "standard_help", help)
					metricFamily = copyMetricFamily(metricFamily)
					copied = true
					metricFamily.Help = proto.String(help)
				}

				// 添加新 mfStat 元素到 mfStatByName 中
				mfStatByName[name] = mfStat{
					pos:    len(result),
					copied: copied,
				}

				// 添加新元素到 result 中
				result = append(result, metricFamily)
			}
		}
	}
	return result
}

// GetMetricFamiliesMap implements the MetricStore interface.
//
// 将 dms.metricGroups 深拷贝后返回
func (dms *DiskMetricStore) GetMetricFamiliesMap() GroupingKeyToMetricGroup {
	dms.lock.RLock()
	defer dms.lock.RUnlock()

	// copy dms.metricGroups to groupsCopy then return.
	groupsCopy := make(GroupingKeyToMetricGroup, len(dms.metricGroups))
	for k, g := range dms.metricGroups {
		// copy g.Metrics
		metricsCopy := make(NameToTimestampedMetricFamilyMap, len(g.Metrics))
		for n, tmf := range g.Metrics {
			metricsCopy[n] = tmf
		}
		// copy g
		metricGroupCopy := MetricGroup{
			Labels: g.Labels,
			Metrics: metricsCopy,
		}
		// save copy of g to groupsCopy[k]
		groupsCopy[k] = metricGroupCopy
	}

	return groupsCopy
}

func (dms *DiskMetricStore) loop(persistenceInterval time.Duration) {

	lastPersist := time.Now()
	persistScheduled := false
	lastWrite := time.Time{}
	persistDone := make(chan time.Time)
	var persistTimer *time.Timer

	//检查是否需要持久化
	checkPersist := func() {

		//检查条件：
		//  1. 检查 persistenceFile 是否为空，若为空，则无法持久化
		//  2. 检查 persistScheduled 是否为 false，如果为 true，意味着当前正在有持久化等待执行（设置了定时任务），本次毋需持久化，否则会重复
		//  3. 检查 lastWrite 是否晚于 lastPersist，如果为 false，则上次持久化以后没有发生写操作，毋需再持久化

		if dms.persistenceFile != "" && !persistScheduled && lastWrite.After(lastPersist) {
			persistTimer = time.AfterFunc(

				//1. 计算定时间隔：persistenceInterval - lastWrite.Sub(lastPersist)，以确保两次持久化的时间间隔为 persistenceInterval
				persistenceInterval-lastWrite.Sub(lastPersist),

				//2. 执行持久化：将 metricGroups 序列化后写入到文件中
				func() {
					persistStarted := time.Now()
					if err := dms.persist(); err != nil {
						level.Error(dms.logger).Log("msg", "error persisting metrics", "err", err)
					} else {
						level.Info(dms.logger).Log("msg", "metrics persisted", "file", dms.persistenceFile)
					}
					persistDone <- persistStarted
				},
			)
			// 如果持久化任务的定时器 persistTimer 已经设置，那么就设置 persistScheduled 标识为 true，避免
			// 当前持久化任务尚未执行时又重复创建新的持久化任务；当 persistTimer 定时抵达并完成持久化之后，
			// 会重置 persistScheduled 为 false，以允许创建新的持久化任务。
			persistScheduled = true
		}
	}

	for {
		select {


		// 读取 WriteRequest 写请求
		case wr := <-dms.writeQueue:

			// 记录当前时间
			lastWrite = time.Now()

			if dms.checkWriteRequest(wr) {
				// 处理写请求
				dms.processWriteRequest(wr)
			} else {
				dms.setPushFailedTimestamp(wr)
			}


			//
			if wr.Done != nil {
				close(wr.Done)
			}

			checkPersist()


		//2. 当前的持久化任务结束后，会发送信令到 persistDone 管道，此时设置 persistScheduled 标识为 false，
		// 以允许创建新的持久化任务，这样是为了避免当前持久化任务尚未执行时又重复创建新的持久化任务，造成资源浪费。
		case lastPersist = <-persistDone:

			persistScheduled = false
			checkPersist() // In case something has been written in the meantime.

		//3. 监听停止信令
		case <-dms.drain:

			// 如果 persistTimer 不为 nil，意味着有定时持久化任务等待执行，需要取消它，以便安全退出
			if persistTimer != nil {
				persistTimer.Stop()
			}


			//1. 把 writeQueue 中剩余的消息逐个处理
			//2. 当 writeQueue 暂无消息可处理时，直接强制持久化，并返回
			for {
				select {
				case wr := <-dms.writeQueue:	// 消费剩余消息
					dms.processWriteRequest(wr)
				default:
					dms.done <- dms.persist() 	// 强制持久化，然后退出
					return
				}
			}
		}
	}
}

func (dms *DiskMetricStore) processWriteRequest(wr WriteRequest) {
	dms.lock.Lock()
	defer dms.lock.Unlock()


	// The grouping key is created by joining all label names and values together with
	// model.SeparatorByte as a separator.
	//
	// This grouping key is both reproducible and unique.
	//
	key := groupingKeyFor(wr.Labels)  // wr.labels => unique key


	// If MetricFamilies is nil, this is a request to delete metrics that share the
	// same grouping key.
	//
	// Otherwise, this is a request to update the MetricStore with the MetricFamilies.
	//
	if wr.MetricFamilies == nil {
		// Delete the whole metric group, and we are done here.
		delete(dms.metricGroups, key)
		return
	}

	// Otherwise, it's an update.


	// Get MetricGroup according to key.
	group, ok := dms.metricGroups[key]	// map[groupKey] => MetricGroup

	// If key not exist, means it's first pushed, create and save a new MetricGroup.
	if !ok {
		group = MetricGroup{
			Labels:  wr.Labels,
			Metrics: NameToTimestampedMetricFamilyMap{},
		}
		dms.metricGroups[key] = group

	} else if wr.Replace {

		// If key exist and wr.Replace is true, the wr.MetricFamilies will completely
		// replace the metrics in group.Metrics.
		//
		// So we delete all metric families in group.Metrics except pre-existing push timestamps.
		for name := range group.Metrics {
			if name != pushMetricName && name != pushFailedMetricName {
				delete(group.Metrics, name)
			}
		}
	}


	// Now,


	//
	wr.MetricFamilies[pushMetricName] = newPushTimestampGauge(wr.Labels, wr.Timestamp)

	// Only add a zero push-failed metric if none is there yet,
	// so that a previously added fail timestamp is retained.
	if _, ok := group.Metrics[pushFailedMetricName]; !ok {
		wr.MetricFamilies[pushFailedMetricName] = newPushFailedTimestampGauge(wr.Labels, time.Time{})
	}


	// replace group.Metrics[...]
	for name, mf := range wr.MetricFamilies {
		group.Metrics[name] = TimestampedMetricFamily{
			Timestamp:            wr.Timestamp,					// push timestamp
			GobbableMetricFamily: (*GobbableMetricFamily)(mf),	// *dto.MetricFamily
		}
	}

}

func (dms *DiskMetricStore) setPushFailedTimestamp(wr WriteRequest) {
	dms.lock.Lock()
	defer dms.lock.Unlock()

	key := groupingKeyFor(wr.Labels)

	group, ok := dms.metricGroups[key]
	if !ok {
		group = MetricGroup{
			Labels:  wr.Labels,
			Metrics: NameToTimestampedMetricFamilyMap{},
		}
		dms.metricGroups[key] = group
	}



	group.Metrics[pushFailedMetricName] = TimestampedMetricFamily{
		Timestamp:            wr.Timestamp,
		GobbableMetricFamily: (*GobbableMetricFamily)(newPushFailedTimestampGauge(wr.Labels, wr.Timestamp)),
	}


	// Only add a zero push metric if none is there yet, so that a
	// previously added push timestamp is retained.
	if _, ok := group.Metrics[pushMetricName]; !ok {
		group.Metrics[pushMetricName] = TimestampedMetricFamily{
			Timestamp:            wr.Timestamp,
			GobbableMetricFamily: (*GobbableMetricFamily)(newPushTimestampGauge(wr.Labels, time.Time{})),
		}
	}
}

// checkWriteRequest return if applying the provided WriteRequest will result in
// a consistent state of metrics.
//
// The dms is not modified by the check.
//
// However, the WriteRequest _will_ be sanitized: the MetricFamilies are ensured to
// contain the grouping Labels after the check. If false is returned,
// the causing error is written to the Done channel of the WriteRequest.
//
// Special case: If the WriteRequest has no Done channel set, the (expensive)
// consistency check is skipped. The WriteRequest is still sanitized,
// and the presence of timestamps still results in returning false.
//
//
//
//
func (dms *DiskMetricStore) checkWriteRequest(wr WriteRequest) bool {


	// Delete request cannot create inconsistencies, and nothing has to be sanitized.
	if wr.MetricFamilies == nil {
		return true
	}

	var err error
	defer func() {
		if err != nil && wr.Done != nil {
			wr.Done <- err
		}
	}()

	// Checks if any timestamps have been specified.
	if timestampsPresent(wr.MetricFamilies) {
		err = errTimestamp
		return false
	}

	// 确保 wr 中每个 MetricFamily 下的每个 metric 都保护所有的 wr.Labels 标签。
	for _, mf := range wr.MetricFamilies {
		sanitizeLabels(mf, wr.Labels)
	}

	// Without Done channel, don't do the expensive consistency check.
	if wr.Done == nil {
		return true
	}


	// Construct a test dms, acting on a copy of the metrics, to test the WriteRequest with.
	tdms := &DiskMetricStore{
		metricGroups:   dms.GetMetricFamiliesMap(),
		predefinedHelp: dms.predefinedHelp,
		logger:         log.NewNopLogger(),
	}

	tdms.processWriteRequest(wr)

	// Construct a test Gatherer to check if consistent gathering is possible.
	tg := prometheus.Gatherers{
		prometheus.DefaultGatherer,
		prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
			return tdms.GetMetricFamilies(), nil
		}),
	}
	if _, err = tg.Gather(); err != nil {
		return false
	}
	return true
}

func (dms *DiskMetricStore) persist() error {
	// Check (again) if persistence is configured because some code paths
	// will call this method even if it is not.
	if dms.persistenceFile == "" {
		return nil
	}

	// 创建 persistenceFile 的临时文件 persistenceFile.in_progress
	f, err := ioutil.TempFile(path.Dir(dms.persistenceFile), path.Base(dms.persistenceFile)+".in_progress.",
	)
	if err != nil {
		return err
	}
	inProgressFileName := f.Name()

	// 定义编码器
	e := gob.NewEncoder(f)

	// 持久化的过程中会对 dms.metricGroups 加锁，避免持久化过程中出现并发的写操作
	dms.lock.RLock()

	// 把 dms.metricGroups 序列化后写入到临时文件中
	err = e.Encode(dms.metricGroups)
	dms.lock.RUnlock()
	if err != nil {
		f.Close()
		os.Remove(inProgressFileName)
		return err
	}

	// 写完后关闭文件
	if err := f.Close(); err != nil {
		os.Remove(inProgressFileName)
		return err
	}

	// 将临时文件重命名替换掉 persistenceFile
	return os.Rename(inProgressFileName, dms.persistenceFile)
}

func (dms *DiskMetricStore) restore() error {
	//1. 获取持久化文件名
	if dms.persistenceFile == "" {
		return nil
	}
	//2. 打开文件
	f, err := os.Open(dms.persistenceFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	//3. gob 是 Golang 包自带的序列化工具，其编解码效率高于JSON
	// - 3.1 初始化解码器
	d := gob.NewDecoder(f)
	// - 3.2 从文件 f 中解码并保存到 dms.metricGroups 结构体中
	if err := d.Decode(&dms.metricGroups); err != nil {
		return err
	}
	return nil
}

func copyMetricFamily(mf *dto.MetricFamily) *dto.MetricFamily {
	return &dto.MetricFamily{
		Name:   mf.Name,
		Help:   mf.Help,
		Type:   mf.Type,
		Metric: append([]*dto.Metric{}, mf.Metric...),
	}
}

// groupingKeyFor creates a grouping key from the provided map of grouping labels.
//
// The grouping key is created by joining all label names and values together with
// model.SeparatorByte as a separator.
//
// The label names are sorted lexicographically before joining.
//
// In that way, the grouping key is both reproducible and unique.
func groupingKeyFor(labels map[string]string) string {


	if len(labels) == 0 { // Super fast path.
		return ""
	}

	// Convert from map[labelName][value] to slice[labelName]
	labelNames := make([]string, 0, len(labels))
	for labelName := range labels {
		labelNames = append(labelNames, labelName)
	}

	// sort slice[labelName]
	sort.Strings(labelNames)

	// Joining all label names and values together with model.SeparatorByte as a separator.
	sb := strings.Builder{}
	for i, labelName := range labelNames {
		sb.WriteString(labelName)
		sb.WriteByte(model.SeparatorByte)
		sb.WriteString(labels[labelName])
		if i+1 < len(labels) { // No separator at the end.
			sb.WriteByte(model.SeparatorByte)
		}
	}

	// Return joined string
	return sb.String()
}

// extractPredefinedHelpStrings extracts all the HELP strings from the provided
// gatherer so that the DiskMetricStore can fix deviations in pushed metrics.
//
// extractPredefinedHelpStrings() 从提供的收集器 Gatherer 中获取所有 HELP 字符串，
// 以 map[metrics][helpstring] 返回，以便 DiskMetricStore 可以修复推送指标的偏差。
func extractPredefinedHelpStrings(g prometheus.Gatherer) (map[string]string, error) {

	if g == nil {
		return nil, nil
	}

	mfs, err := g.Gather()
	if err != nil {
		return nil, err
	}

	result := map[string]string{}
	for _, mf := range mfs {
		result[mf.GetName()] = mf.GetHelp()
	}

	return result, nil
}

func newPushTimestampGauge(groupingLabels map[string]string, t time.Time) *dto.MetricFamily {
	return newTimestampGauge(pushMetricName, pushMetricHelp, groupingLabels, t)
}

func newPushFailedTimestampGauge(groupingLabels map[string]string, t time.Time) *dto.MetricFamily {
	return newTimestampGauge(pushFailedMetricName, pushFailedMetricHelp, groupingLabels, t)
}


//
//
//
func newTimestampGauge(name, help string, groupingLabels map[string]string, t time.Time) *dto.MetricFamily {

	var ts float64

	if !t.IsZero() {
		ts = float64(t.UnixNano()) / 1e9		// 时间戳（纳秒）
	}

	mf := &dto.MetricFamily{
		Name: proto.String(name),				// 指标名
		Help: proto.String(help),				// 注释
		Type: dto.MetricType_GAUGE.Enum(),		// 类型
		Metric: []*dto.Metric{					// 指标数组
			{
				Gauge: &dto.Gauge{
					Value: proto.Float64(ts),
				},
			},
		},
	}


	sanitizeLabels(mf, groupingLabels)
	return mf
}

// sanitizeLabels ensures that all the labels in groupingLabels and the
// `instance` label are present in the MetricFamily.
//
// The label values from groupingLabels are set in each Metric, no matter what.
//
// After that, if the 'instance' label is not present at all in a Metric,
// it will be created (with an empty string as value).
//
// Finally, sanitizeLabels sorts the label pairs of all metrics.
//


// sanitizeLabels 确保 groupingLabels 中的所有标签和 `instance` 标签都存在于 metricFamilies 中的每个 MetricFamily 中。
// 无论如何，都要将 groupingLabels 中的标签值设置到每个 MetricFamily 上。
// 之后，如果 MetricFamily 中根本不存在 'instance' 标签，则会创建它（使用空字符串作为值）。
// 最后，sanitizeLabels 对所有 metrics 的标签对进行排序。
//
//
func sanitizeLabels(mf *dto.MetricFamily, groupingLabels map[string]string) {

	gLabelsNotYetDone := make(map[string]string, len(groupingLabels))

metric:

	//循环处理 mf 下每个 metrics
	for _, metric := range mf.GetMetric() {


		// 把 groupingLabels 中的每个 label<name,value> 保存到 gLabelsNotYetDone{} 中。
		//
		// 每次循环都要重新初始化这个 map，用来确保所有的 groupingLabels 都被设置到每个 metric 上。
		for ln, lv := range groupingLabels {
			gLabelsNotYetDone[ln] = lv
		}

		// 检查当前 metric 是否具有 "instance" 标签
		hasInstanceLabel := false

		// 遍历 m.labels, 如果 label 存在于 gLabelsNotYetDone{} 中，就用 gLabelsNotYetDone{} 中的值覆盖它，然后从 gLabelsNotYetDone 中移除。
		for _, lp := range metric.GetLabel() {

			// 取 label 名称
			ln := lp.GetName()

			// 若 label 存在于 gLabelsNotYetDone{} 中
			if lv, ok := gLabelsNotYetDone[ln]; ok {
				// 设置 lp.Value
				lp.Value = proto.String(lv)
				// 移除 gLabelsNotYetDone[ln]
				delete(gLabelsNotYetDone, ln)
			}

			// 检查 labelName 是否为 "instance"，若是则设置 hasInstanceLabel 为 true
			if ln == string(model.InstanceLabel) {
				hasInstanceLabel = true
			}

			if len(gLabelsNotYetDone) == 0 && hasInstanceLabel {
				sort.Sort(labelPairs(metric.Label))
				continue metric
			}
		}


		// 运行到这，意味着 metric 还缺少部分标签，需要把 gLabelsNotYetDone 中剩余的标签添加到 metric.Label 中。
		for ln, lv := range gLabelsNotYetDone {

			// 添加标签
			metric.Label = append(metric.Label, &dto.LabelPair{
				Name:  proto.String(ln),
				Value: proto.String(lv),
			})

			// 检查 labelName 是否为 "instance"，若是则设置 hasInstanceLabel 为 true
			if ln == string(model.InstanceLabel) {
				hasInstanceLabel = true
			}

			// 从 gLabelsNotYetDone 中移除已添加的标签。
			delete(gLabelsNotYetDone, ln) // To prepare map for next metric.
		}


		// 如果不存在 "instance" 标签，就构造并添加
		if !hasInstanceLabel {
			metric.Label = append(metric.Label, &dto.LabelPair{
				Name:  proto.String(string(model.InstanceLabel)),
				Value: proto.String(""),
			})
		}

		// 标签排序
		sort.Sort(labelPairs(metric.Label))
	}
}



// Checks if any timestamps have been specified.
func timestampsPresent(metricFamilies map[string]*dto.MetricFamily) bool {
	for _, mf := range metricFamilies {
		for _, m := range mf.GetMetric() {
			if m.TimestampMs != nil {
				return true
			}
		}
	}
	return false
}

// labelPairs implements sort.Interface. It provides a sortable version of a
// slice of dto.LabelPair pointers.
type labelPairs []*dto.LabelPair

func (s labelPairs) Len() int {
	return len(s)
}

func (s labelPairs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s labelPairs) Less(i, j int) bool {
	return s[i].GetName() < s[j].GetName()
}
