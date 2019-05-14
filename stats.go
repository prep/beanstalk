package beanstalk

import (
	"strings"
	"sync"

	"gopkg.in/yaml.v2"
)

// TubeStat describes the statistics of a beanstalk tube.
type TubeStat struct {
	Name           string `yaml:"name"                  json:"name"`
	UrgentJobs     int    `yaml:"current-jobs-urgent"   json:"urgentJobs"`
	ReadyJobs      int    `yaml:"current-jobs-ready"    json:"readyJobs"`
	ReservedJobs   int    `yaml:"current-jobs-reserved" json:"reservedJobs"`
	DelayedJobs    int    `yaml:"current-jobs-delayed"  json:"delayedJobs"`
	BuriedJobs     int    `yaml:"current-jobs-buried"   json:"buriedJobs"`
	TotalJobs      int64  `yaml:"total-jobs"            json:"totalJobs"`
	Using          int    `yaml:"current-using"         json:"using"`
	Watching       int    `yaml:"current-watching"      json:"watching"`
	Waiting        int    `yaml:"current-waiting"       json:"waiting"`
	DeleteCommands int64  `yaml:"cmd-delete"            json:"deleteCommands"`
	PauseCommands  int64  `yaml:"cmd-pause-tube"        json:"pauseCommands"`
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
				conn, err := dial(url)
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
					tubeStats[i].DelayedJobs += tubeStat.DelayedJobs
					tubeStats[i].BuriedJobs += tubeStat.BuriedJobs
					tubeStats[i].TotalJobs += tubeStat.TotalJobs
					tubeStats[i].Using += tubeStat.Using
					tubeStats[i].Watching += tubeStat.Watching
					tubeStats[i].DeleteCommands += tubeStat.DeleteCommands
					tubeStats[i].PauseCommands += tubeStat.PauseCommands
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
