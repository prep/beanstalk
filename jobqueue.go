package beanstalk

import (
	"errors"
	"time"
)

// TouchSafetyMargin defines the margin between refreshing a reserved job with
// a beanstalk touch command and the time the job expires.
var TouchSafetyMargin = 100 * time.Millisecond

// JobQueueItem represents a reserved beanstalk Job that is in the JobQueue.
type JobQueueItem struct {
	job        *Job
	offered    bool
	reservedAt time.Time
	touchAt    time.Time
}

func NewJobQueueItem() *JobQueueItem {
	item := &JobQueueItem{}
	item.Clear()
	return item
}

// Clear out this item so it can be reused.
func (item *JobQueueItem) Clear() {
	item.job = nil
	item.offered = false
	item.reservedAt = time.Time{}
	item.touchAt = time.Time{}
}

// Refresh the time that indicates when the job in this item needs to be
// touched to keep it reserved.
func (item *JobQueueItem) Refresh() {
	if item.job != nil {
		item.touchAt = time.Now().Add(item.job.TTR)
	}
}

// SetJob a beanstalk job for this item.
func (item *JobQueueItem) SetJob(job *Job) {
	item.job = job
	item.offered = false
	item.reservedAt = time.Now()
	item.touchAt = item.reservedAt.Add(job.TTR)
}

// SetOffered marks this item as being offered to an external worker.
func (item *JobQueueItem) SetOffered() {
	item.offered = true
}

// ShouldBeTouched returns true when the job in this item is about to expire.
func (item *JobQueueItem) ShouldBeTouched() bool {
	if item.job == nil {
		return false
	}

	return item.touchAt.Before(time.Now().Add(TouchSafetyMargin))
}

// JobQueue maintains a queue of reserved beanstalk jobs.
type JobQueue struct {
	// TouchC triggers whenever there are 1 or more jobs that needs touching.
	TouchC <-chan time.Time

	queue      []*JobQueueItem
	itemCount  int
	size       int
	touchTimer *time.Timer
}

// NewJobQueue creates a new queue for beanstalk jobs.
func NewJobQueue(size int) *JobQueue {
	timer := time.NewTimer(time.Second)
	timer.Stop()

	queue := &JobQueue{TouchC: timer.C, touchTimer: timer}
	queue.Resize(size)

	return queue
}

// AddJob a new beanstalk job to the queue. This function returns nil when the
// queue is full.
func (queue *JobQueue) AddJob(job *Job) *JobQueueItem {
	for _, item := range queue.queue {
		if item.job == nil {
			item.SetJob(job)
			queue.itemCount++
			queue.UpdateTimer()
			return item
		}
	}

	return nil
}

// Clear the queue.
func (queue *JobQueue) Clear() {
	for _, item := range queue.queue {
		item.Clear()
	}

	queue.itemCount = 0
	queue.UpdateTimer()
}

// DelJob removes a job from the queue.
func (queue *JobQueue) DelJob(job *Job) error {
	for _, item := range queue.queue {
		if item.job != nil && item.job == job {
			item.Clear()
			queue.itemCount--
			queue.UpdateTimer()
			return nil
		}
	}

	return errors.New("Unable to find job")
}

// HasJob checks if the specified job is in the queue.
func (queue *JobQueue) HasJob(job *Job) bool {
	for _, item := range queue.queue {
		if item.job != nil && item.job == job {
			return true
		}
	}

	return false
}

// IsEmpty returns true when this queue is empty.
func (queue *JobQueue) IsEmpty() bool {
	return queue.itemCount == 0
}

// IsFull returns true when this queue is full.
func (queue *JobQueue) IsFull() bool {
	return queue.itemCount >= queue.size
}

// ItemsForTouch returns the items that need touching in order to keep their
// reserved status.
func (queue *JobQueue) ItemsForTouch() []*JobQueueItem {
	var items []*JobQueueItem

	for _, item := range queue.queue {
		if item.ShouldBeTouched() {
			items = append(items, item)
		}
	}

	return items
}

// ItemForOffer returns the oldest item in the queue that can be offered to an
// external worker or nil if there are no jobs available to be offered.
func (queue *JobQueue) ItemForOffer() *JobQueueItem {
	var jobItem *JobQueueItem

	for _, item := range queue.queue {
		if item.job != nil && !item.offered && (jobItem == nil || jobItem.reservedAt.After(item.reservedAt)) {
			jobItem = item
		}
	}

	return jobItem
}

// Jobs returns a slice of all the jobs in the queue.
func (queue *JobQueue) Jobs() []*Job {
	var jobs []*Job

	for _, item := range queue.queue {
		if item.job != nil {
			jobs = append(jobs, item.job)
		}
	}

	return jobs
}

// Resize the queue to the specified size. If the specified size is smaller
// than the amount of items currently in the queue, IsFull() will keep
// returning true until the number of items dip below the new size.
func (queue *JobQueue) Resize(size int) {
	if size < 1 {
		size = 1
	}
	if len(queue.queue) >= size {
		queue.size = size
		return
	}

	newQueue := make([]*JobQueueItem, size)
	for i, item := range queue.queue {
		newQueue[i] = item
	}

	for i := queue.size; i < size; i++ {
		newQueue[i] = NewJobQueueItem()
		newQueue[i].Clear()
	}

	queue.size = size
	queue.queue = newQueue
}

// UnofferedJobs returns a slice of all the jobs in the queue that were not
// yet offered up.
func (queue *JobQueue) UnofferedJobs() []*Job {
	var jobs []*Job

	for _, item := range queue.queue {
		if item.job != nil && !item.offered {
			jobs = append(jobs, item.job)
		}
	}

	return jobs
}

// UpdateTimer figures out when the next job(s) should be touched.
func (queue *JobQueue) UpdateTimer() {
	if queue.IsEmpty() {
		queue.touchTimer.Stop()
		return
	}

	var touchAt = time.Time{}
	for _, item := range queue.queue {
		if item.job != nil {
			if touchAt.Equal(time.Time{}) || item.touchAt.Before(touchAt) {
				touchAt = item.touchAt
			}
		}
	}

	queue.touchTimer.Reset(touchAt.Sub(time.Now().Add(TouchSafetyMargin)))
}
