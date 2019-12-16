package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/prep/beanstalk"
)

var (
	help = flag.Bool("help", false, "Display usage information")
	tube = flag.String("tube", "", "The tube to operate on. The default is all tubes")
	uri  = flag.String("uri", "beanstalk://localhost:11300", "The URI of the beanstalk server")
)

// delete all the buried jobs in the specified tubes.
func delete(ctx context.Context, conn *beanstalk.Conn, tubes []beanstalk.TubeStats) error {
	for _, tube := range tubes {
		log.Printf("Deleting buried jobs in %s", tube.Name)

		var count int
		for {
			job, err := conn.PeekBuried(ctx, tube.Name)
			if err != nil {
				return err
			}
			if job == nil {
				break
			}

			if err = job.Delete(ctx); err != nil {
				log.Printf("\rUnable to delete job %d", job.ID)
			}

			count++
			_, _ = fmt.Fprint(os.Stdout, ".")
		}

		log.Printf("\rDeleted %d jobs in tube %s", count, tube.Name)
	}

	return nil
}

// kick all the buried jobs in the specified tubes.
func kick(ctx context.Context, conn *beanstalk.Conn, tubes []beanstalk.TubeStats) error {
	for _, tube := range tubes {
		log.Printf("Kicking buried jobs in %s", tube.Name)

		count, err := conn.Kick(ctx, tube.Name, int(tube.BuriedJobs))
		if err != nil {
			return err
		}

		log.Printf("Kicked %d jobs in tube %s", count, tube.Name)
	}

	return nil
}

func main() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s [flags] [command]\n\n", os.Args[0])
		fmt.Println("Delete or kick buried jobs on your beanstalk servers.")
		fmt.Println("\nFlags:")
		flag.PrintDefaults()

		fmt.Println("\nCommands:")
		fmt.Println("  delete    Delete one or more buried jobs")
		fmt.Println("  kick      Kick one or more jobs back to the ready queue")
		fmt.Println("")
	}

	flag.Parse()

	if *help {
		flag.Usage()
		return
	}

	var command string
	switch flag.Arg(0) {
	case "delete", "kick", "info":
		command = flag.Arg(0)
	case "":
		command = "info"
	default:
		_, _ = fmt.Fprintf(os.Stderr, "%s: invalid command", flag.Arg(0))
		flag.Usage()
		return
	}

	config := beanstalk.Config{
		ErrorLog: log.New(os.Stderr, "ERROR: ", log.LstdFlags),
		InfoLog:  log.New(os.Stdout, "INFO: ", log.LstdFlags),
	}

	// Create a connection to the beanstalk server.
	conn, err := beanstalk.Dial(*uri, config)
	if err != nil {
		log.Fatalf("Unable to connect to %s: %s", *uri, err)
	}
	defer conn.Close()

	ctx := context.Background()

	// Fetch the relevant tube names.
	var tubeNames []string
	if *tube == "" {
		if tubeNames, err = conn.ListTubes(ctx); err != nil {
			log.Fatalf("Unable to fetch tube names: %s", err)
		}

		sort.Strings(tubeNames)
	} else {
		tubeNames = []string{*tube}
	}

	// Fetch the tube statistics and skip any that don't have buried jobs.
	var tubes []beanstalk.TubeStats
	for _, tubeName := range tubeNames {
		stats, err := conn.TubeStats(ctx, tubeName)
		switch {
		case err != nil:
			log.Fatalf("Unable to fetch tube stats: %s", err)
		case stats.BuriedJobs == 0:
			continue
		}

		tubes = append(tubes, stats)
	}

	if len(tubes) == 0 {
		log.Printf("%d tubes found, but none have buried jobs", len(tubeNames))
		return
	}

	switch command {
	case "delete":
		if err = delete(ctx, conn, tubes); err != nil {
			log.Fatalf("Error deleting jobs: %s", err)
		}

	case "info":
		for _, tube := range tubes {
			log.Printf("%s has %d buried jobs", tube.Name, tube.BuriedJobs)
		}

	case "kick":
		if err = kick(ctx, conn, tubes); err != nil {
			log.Fatalf("Error kicking jobs: %s", err)
		}
	}
}
