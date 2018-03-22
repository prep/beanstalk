package beanstalk

import (
	"strings"
	"sync"

	"gopkg.in/yaml.v2"
)

// TubeStat describes the statistics of a beanstalk tube.
type TubeStat struct {
	Name           string `yaml:"name"`
	UrgentJobs     int    `yaml:"current-jobs-urgent"`
	ReadyJobs      int    `yaml:"current-jobs-ready"`
	ReservedJobs   int    `yaml:"current-jobs-reserved"`
	DelayedJobs    int    `yaml:"current-jobs-delayed"`
	BuriedJobs     int    `yaml:"current-jobs-buried"`
	TotalJobs      int64  `yaml:"tota-jobs"`
	Using          int    `yaml:"current-using"`
	Watching       int    `yaml:"curent-watching"`
	Waiting        int    `yaml:"current-waiting"`
	DeleteCommands int64  `yaml:"cmd-delete"`
	PauseCommands  int64  `yaml:"cmd-pause-tube"`
	PausedFor      int64  `yaml:"pause"`
	PauseLeft      int64  `yaml:"pause-time-left"`
}

type stats struct {
	TubeStats []TubeStat
	err       error
}

// TubeStats returns a list of beanstalk tube statistics.
func TubeStats(urls []string, options *Options, tubePrefix string) ([]TubeStat, error) {
	if options == nil {
		options = DefaultOptions()
	}

	var wg sync.WaitGroup
	wg.Add(len(urls))

	// Request the beanstalk tube statistics for all the beanstalk servers.
	results := make([]stats, len(urls))
	for i, url := range urls {
		go func(i int, url string) {
			defer wg.Done()

			err := func() error {
				// Dial into the beanstalk server.
				conn, err := dial(url, options)
				if err != nil {
					return err
				}
				defer conn.Close()

				// Fetch the list of tubes on this server.
				client := NewClient(conn, options)
				_, data, err := client.requestResponse("list-tubes")
				if err != nil {
					return err
				}

				var tubes []string
				if err = yaml.Unmarshal(data, &tubes); err != nil {
					return err
				}

				// Cycle over the list of tube names and skip any tube that doesn't
				// match the specified prefix.
				var tubeStats []TubeStat
				for _, tube := range tubes {
					if !strings.HasPrefix(tube, tubePrefix) {
						continue
					}

					// Fetch the statistics for this tube.
					if _, data, err = client.requestResponse("stats-tube %s", tube); err != nil {
						return err
					}

					var tubeStat TubeStat
					if err = yaml.Unmarshal(data, &tubeStat); err != nil {
						return err
					}

					tubeStats = append(tubeStats, tubeStat)
				}

				results[i] = stats{TubeStats: tubeStats}
				return nil
			}()

			if err != nil {
				results[i] = stats{err: err}
			}
		}(i, url)
	}

	wg.Wait()

	// Return the first error it encounters.
	for _, result := range results {
		if result.err != nil {
			return []TubeStat{}, result.err
		}
	}

	// Collect and merge the results.
	var tubeStats []TubeStat
	for _, result := range results {
		for _, tubeStat := range result.TubeStats {
			found := false

			for i, stat := range tubeStats {
				if stat.Name == tubeStat.Name {
					tubeStats[i].UrgentJobs += tubeStat.UrgentJobs
					tubeStats[i].ReadyJobs += tubeStat.ReadyJobs
					tubeStats[i].ReservedJobs += tubeStat.ReservedJobs
					found = true
				}
			}

			if !found {
				tubeStats = append(tubeStats, tubeStat)
			}
		}
	}

	return tubeStats, nil
}
