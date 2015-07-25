package beanstalk

import (
	//	"fmt"
	"testing"
	"time"
)

var job = &Job{ID: 12345, Body: []byte("Hello World"), TTR: 3 * time.Second}

// ****************************************************************************
// ***************************** [ JobQueueItem ] *****************************
// ****************************************************************************

func TestNewJobQueueItem(t *testing.T) {
	item := NewJobQueueItem()
	if item.job != nil {
		t.Error("JobQueueItem shouldn't have a job set")
	}
	if item.offered {
		t.Error("JobQueueItem shouldn't have been marked as offered")
	}
}

func TestJobQueueItemClear(t *testing.T) {
	item := NewJobQueueItem()
	item.job = job
	item.offered = true
	item.reservedAt = time.Now()
	item.touchAt = time.Now()

	item.Clear()
	if item.job != nil {
		t.Error("JobQueueItem shouldn't have a job set")
	}
	if item.offered {
		t.Error("JobQueueItem shouldn't have been marked as offered")
	}
	if !item.reservedAt.Equal(time.Time{}) {
		t.Error("JobQueueItem shouldn't have a set reservedAt variable")
	}
	if !item.touchAt.Equal(time.Time{}) {
		t.Error("JobQueueItem shouldn't have a set touchAt variable")
	}
}

func TestJobQueueItemRefresh(t *testing.T) {
	now := time.Now()

	item := NewJobQueueItem()
	item.job = job
	item.touchAt = now

	item.Refresh()
	if !item.touchAt.After(now) {
		t.Error("Expected touchAt to be increased")
	}
}

func TestJobQueueItemRefreshWithoutJob(t *testing.T) {
	now := time.Now()

	item := NewJobQueueItem()
	item.touchAt = now

	item.Refresh()
	if !item.touchAt.Equal(now) {
		t.Error("Expected touchAt to remain unchanged")
	}
}

func TestJobQueueItemSetJob(t *testing.T) {
	now := time.Now()

	item := NewJobQueueItem()
	item.offered = true
	item.SetJob(job)

	if item.job == nil {
		t.Error("Expected a job to be set")
	}
	if item.offered {
		t.Error("Expected the offered variable to be reset")
	}
	if !item.reservedAt.After(now) {
		t.Error("Expected reservedAt to be set with a recent time")
	}
	if !item.touchAt.After(now.Add(job.TTR)) {
		t.Error("Expected touchAt to be set with a recent time")
	}
}

func TestJobQueueItemSetOffered(t *testing.T) {
	item := NewJobQueueItem()
	if item.offered {
		t.Error("Expected JobQueueItem to be unoffered")
	}

	item.SetOffered()
	if !item.offered {
		t.Error("Expected JobQueueItem to be offered")
	}
}

func TestJobQueueItemShouldBeTouched(t *testing.T) {
	item := NewJobQueueItem()
	if item.ShouldBeTouched() {
		t.Error("JobQueueItem shouldn't be touched yet")
	}

	item.job = job
	item.touchAt = time.Now().Add(25 * time.Millisecond)

	if !item.ShouldBeTouched() {
		t.Error("JobQueueItem should be touched")
	}
}

// ****************************************************************************
// ******************************* [ JobQueue ] *******************************
// ****************************************************************************

func numberOfItemsInTheQueue(queue *JobQueue) int {
	count := 0
	for _, item := range queue.queue {
		if item.job != nil {
			count++
		}
	}

	return count
}

func TestNewJobQueue(t *testing.T) {
	queue := NewJobQueue(10)
	if len(queue.queue) != 10 {
		t.Error("Expected the new JobQueue to have a job-size of 10")
	}
	if queue.touchTimer == nil {
		t.Error("Expected timer to be set")
	}

	for _, item := range queue.queue {
		if item == nil {
			t.Fatal("Expected the JobQueue to have prepared its internal queue")
		}
	}
}

func TestJobQueueAddJob(t *testing.T) {
	queue := NewJobQueue(2)
	if queue.itemCount != 0 {
		t.Error("Expected the number of items in the queue to be 0")
	}

	queue.AddJob(job)
	if queue.itemCount != 1 {
		t.Error("Expected the number of items in the queue to be 1")
	}
	if count := numberOfItemsInTheQueue(queue); count != 1 {
		t.Errorf("Expected there to be 1 job set in the queue, but counted %d", count)
	}
}

func TestJobQueueClear(t *testing.T) {
	queue := NewJobQueue(2)
	queue.AddJob(job)
	if queue.itemCount != 1 {
		t.Error("Expected the number of items in the queue to be 1")
	}

	queue.Clear()
	if queue.itemCount != 0 {
		t.Error("Expected the number of items in the queue to be 0")
	}
	if count := numberOfItemsInTheQueue(queue); count != 0 {
		t.Errorf("Expected there to be 0 job set in the queue, but counted %d", count)
	}
}

func TestJobQueueDelJob(t *testing.T) {
	queue := NewJobQueue(2)
	queue.AddJob(job)
	if queue.itemCount != 1 {
		t.Error("Expected the number of items in the queue to be 1")
	}

	if err := queue.DelJob(job); err != nil {
		t.Errorf("Unexpected error while deleting job: %s", err)
	}
	if queue.itemCount != 0 {
		t.Error("Expected the number of items in the queue to be 0")
	}
	if count := numberOfItemsInTheQueue(queue); count != 0 {
		t.Errorf("Expected there to be 0 job set in the queue, but counted %d", count)
	}
}

func TestJobQueueDelJobWithUnknownJob(t *testing.T) {
	queue := NewJobQueue(1)
	if err := queue.DelJob(job); err == nil {
		t.Error("Expected error when removing unknown job, but got nothing")
	}
}

func TestJobQueueHasJob(t *testing.T) {
	queue := NewJobQueue(1)
	if queue.HasJob(job) {
		t.Error("Did not expect HasJob() to find a job that wasn't added")
	}

	queue.AddJob(job)
	if !queue.HasJob(job) {
		t.Error("Expected HasJob() to find a job that was just added")
	}
}

func TestJobQueueHasJobWithNil(t *testing.T) {
	queue := NewJobQueue(1)
	if queue.HasJob(nil) {
		t.Error("Expected HasJob() to not find a job when specified a nil value")
	}
}

func TestJobQueueIsEmpty(t *testing.T) {
	queue := NewJobQueue(2)
	if !queue.IsEmpty() {
		t.Error("Expected queue to be empty")
	}

	queue.AddJob(job)
	if queue.IsEmpty() {
		t.Error("Expected queue to not be empty")
	}
}

func TestJobQueueIsFull(t *testing.T) {
	queue := NewJobQueue(2)
	if queue.IsFull() {
		t.Error("Didn't expect the queue to be full")
	}

	queue.AddJob(job)
	if queue.IsFull() {
		t.Error("Didn't expect the queue to be full")
	}

	queue.AddJob(job)
	if !queue.IsFull() {
		t.Error("Expected the queue to be full")
	}
}

func TestJobQueueItemForOffer(t *testing.T) {
	queue := NewJobQueue(2)
	if item := queue.ItemForOffer(); item != nil {
		t.Error("Didn't expect an item from the queue")
	}

	queuedItem := queue.AddJob(job)
	item := queue.ItemForOffer()
	if item == nil {
		t.Error("Expected an item from the queue")
	}
	if queuedItem != item {
		t.Error("Expected the same item to be returned as was put in")
	}
}

func TestJobQueueJobs(t *testing.T) {
	queue := NewJobQueue(2)
	if len(queue.Jobs()) != 0 {
		t.Error("Expected 0 jobs in the queue")
	}

	queue.AddJob(job)
	if len(queue.Jobs()) != 1 {
		t.Error("Expected 1 job in the queue")
	}

	queue.AddJob(job)
	if len(queue.Jobs()) != 2 {
		t.Error("Expected 2 job in the queue")
	}
}

func TestJobQueueResize(t *testing.T) {
	queue := NewJobQueue(2)
	if queue.size != 2 {
		t.Error("Expected the queue size to be 2")
	}
	if len(queue.queue) != 2 {
		t.Error("Expected the queue slice size to be 2")
	}

	// Make the queue larger.
	queue.Resize(10)
	if queue.size != 10 {
		t.Error("Expected the queue size to be 10")
	}
	if len(queue.queue) != 10 {
		t.Error("Expected the queue slice size to be 10")
	}

	// Make the queue smaller.
	queue.Resize(5)
	if queue.size != 5 {
		t.Error("Expected the queue size to be 5")
	}
	if len(queue.queue) != 10 {
		t.Error("Expected the queue slice size to still be 10")
	}

	// Make the queue size larger again, but not large than cap.
	queue.Resize(8)
	if queue.size != 8 {
		t.Error("Expected the queue size to be 8")
	}
	if len(queue.queue) != 10 {
		t.Error("Expected the queue slice size to still be 10")
	}
}

func TestJobQueueResizeWithItems(t *testing.T) {
	queue := NewJobQueue(2)

	// Add 2 jobs to the queue with a limit of 2.
	queue.AddJob(job)
	queue.AddJob(job)
	if !queue.IsFull() {
		t.Error("Expected the queue to be full")
	}

	// Resize the queue to allow holding 3 jobs and add the 3rd job.
	queue.Resize(3)
	if queue.IsFull() {
		t.Error("Expected there to be room in the queue")
	}
	queue.AddJob(job)
	if !queue.IsFull() {
		t.Error("Expected the queue to be full")
	}

	// Resize the queue to hold just 1 item.
	queue.Resize(1)
	if !queue.IsFull() {
		t.Error("Expected the queue to be full")
	}

	queue.DelJob(queue.ItemForOffer().job)
	queue.DelJob(queue.ItemForOffer().job)
	if !queue.IsFull() {
		t.Error("Expected the queue to be full")
	}

	queue.DelJob(queue.ItemForOffer().job)
	if queue.IsFull() {
		t.Error("Expected there to be room in the queue")
	}
	if !queue.IsEmpty() {
		t.Error("Expected the queue to be empty")
	}

	queue.AddJob(job)
	if !queue.IsFull() {
		t.Error("Expected the queue to be full")
	}
}

func TestJobQueueUnofferedJobs(t *testing.T) {
	queue := NewJobQueue(2)
	if len(queue.UnofferedJobs()) != 0 {
		t.Error("Expected 0 jobs in the queue")
	}

	item := queue.AddJob(job)
	if len(queue.UnofferedJobs()) != 1 {
		t.Error("Expected 1 job in the queue")
	}

	item.SetOffered()
	if len(queue.UnofferedJobs()) != 0 {
		t.Error("Expected 0 jobs in the queue")
	}
}
