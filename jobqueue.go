package beanstalk

import (
	"errors"
	"time"
)

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

	return item.touchAt.Before(time.Now().Add(50 * time.Millisecond))
}

// JobQueue maintains a queue of reserved beanstalk jobs.
type JobQueue struct {
	// TouchC triggers whenever there are 1 or more beanstalk jobs that needs
	// touching.
	TouchC <-chan time.Time

	queue      []*JobQueueItem
	count      int
	touchTimer *time.Timer
}

// NewJobQueue creates a new queue for beanstalk jobs.
func NewJobQueue(size int) *JobQueue {
	timer := time.NewTimer(time.Second)
	timer.Stop()

	queue := &JobQueue{
		queue:      make([]*JobQueueItem, size),
		touchTimer: timer,
		TouchC:     timer.C,
	}

	for i := 0; i < size; i++ {
		queue.queue[i] = NewJobQueueItem()
		queue.queue[i].Clear()
	}

	return queue
}

// AddJob a new beanstalk job to the queue. This function returns nil when the
// queue is full.
func (queue *JobQueue) AddJob(job *Job) *JobQueueItem {
	for _, item := range queue.queue {
		if item.job == nil {
			item.SetJob(job)
			queue.count++
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

	queue.count = 0
	queue.UpdateTimer()
}

// DelJob removes a job from the queue.
func (queue *JobQueue) DelJob(job *Job) error {
	for _, item := range queue.queue {
		if item.job == job {
			item.Clear()
			queue.count--
			queue.UpdateTimer()
			return nil
		}
	}

	return errors.New("Unable to find job")
}

// HasJob checks if the specified job is in the queue.
func (queue *JobQueue) HasJob(job *Job) bool {
	for _, item := range queue.queue {
		if item.job == job {
			return true
		}
	}

	return false
}

// IsEmpty returns true when this queue is empty.
func (queue *JobQueue) IsEmpty() bool {
	return queue.count == 0
}

// IsFull returns true when this queue is full.
func (queue *JobQueue) IsFull() bool {
	return queue.count == len(queue.queue)
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
	for i, item := range queue.queue {
		if i == 0 {
			touchAt = item.touchAt
		} else if item.touchAt.Before(touchAt) {
			touchAt = item.touchAt
		}
	}

	queue.touchTimer.Reset(touchAt.Sub(time.Now()))
}
