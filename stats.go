package beanstalk

import (
	"context"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	tagKeyCommand = tag.MustNewKey("command")
)

var (
	roundTripCommandLatencyMeasurement = stats.Int64(
		"beanstalk_client_command_roundtrip_latency",
		"Latency between sending the command request and receiving it's response",
		stats.UnitMilliseconds,
	)
	commandErrorCountMeasurement = stats.Int64(
		"beanstalk_client_command_error_count",
		"Errors returned from beanstalkd",
		"",
	)
	commandCountMeasurement = stats.Int64(
		"beanstalk_client_command_count",
		"Commands sent and received from beanstalkd",
		"",
	)
	commandQueueLatencyMeasurement = stats.Int64(
		"beanstalk_client_command_queue_latency",
		"Latency between acquiring the lock and performing the command in a scenario",
		stats.UnitMilliseconds,
	)
)

var (
	roundTripCommandLatencyView = &view.View{
		Name:        roundTripCommandLatencyMeasurement.Name(),
		Description: roundTripCommandLatencyMeasurement.Description(),
		Measure:     roundTripCommandLatencyMeasurement,
		Aggregation: view.Distribution(1, 2, 5, 10, 20, 50, 100, 1000),
		TagKeys:     []tag.Key{tagKeyCommand},
	}
	commandErrorCountView = &view.View{
		Name:        commandErrorCountMeasurement.Name(),
		Description: commandErrorCountMeasurement.Description(),
		Measure:     commandErrorCountMeasurement,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{tagKeyCommand},
	}
	commandCountView = &view.View{
		Name:        commandCountMeasurement.Name(),
		Description: commandCountMeasurement.Description(),
		Measure:     commandCountMeasurement,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{tagKeyCommand},
	}
	commandQueueLatencyView = &view.View{
		Name:        commandQueueLatencyMeasurement.Name(),
		Description: commandQueueLatencyMeasurement.Description(),
		Measure:     commandQueueLatencyMeasurement,
		Aggregation: view.Distribution(1, 2, 5, 10, 20, 50, 100, 1000),
		TagKeys:     []tag.Key{},
	}
)

func MetricViews() []*view.View {
	return []*view.View{
		roundTripCommandLatencyView,
		commandErrorCountView,
		commandCountView,
		commandQueueLatencyView,
	}
}

func addTagKey(ctx context.Context, mutators ...tag.Mutator) context.Context {
	newctx, _ := tag.New(ctx, mutators...)
	return newctx
}
