package cmd

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/karust/gogetcrawl/common"
	"github.com/spf13/cobra"
)

type fileScenario struct {
	outputDir    string
	downloadRate float32
}

var fileScn = fileScenario{}

var fileCMD = &cobra.Command{
	Use:     "file",
	Aliases: []string{"download"},
	Short:   "Download files located in web arhives for desired domains",
	Args:    cobra.MatchAll(cobra.MinimumNArgs(1), cobra.OnlyValidArgs),
	Run:     fileScn.spawnWorkers,
}

// worker consumes request configs and fans each one out to all sources,
// which push batches into fetchResults. It returns once the configs channel
// is drained; every source goroutine it spawns is joined before the worker
// moves on or exits.
func (fs *fileScenario) worker(ctx context.Context, configs <-chan common.RequestConfig, fetchResults chan<- []*common.CdxResponse) {
	for config := range configs {
		var wg sync.WaitGroup
		for _, s := range sources {
			wg.Add(1)
			go func(s common.Source) {
				defer wg.Done()
				s.FetchPages(ctx, config, fetchResults, errors)
			}(s)
		}
		wg.Wait()
	}
}

// run drives the download fan-in and returns only after every goroutine it
// started has finished:
//
//   - workers fetch batches into an internal channel; they are cancelled
//     (via a child context) once maxResults is reached or ctx is done;
//   - a forwarder applies maxResults exactly to the total number of usable
//     results across all sources and closes the results channel when
//     fetching is over;
//   - a single SaveFiles consumer drains the results channel — it keeps
//     draining batches already forwarded after the limit is hit, so the
//     limit is exact;
//   - the errors channel is closed only after both fetchers and the saver
//     are done, so nothing ever sends on a closed channel.
func (fs *fileScenario) run(ctx context.Context, configs <-chan common.RequestConfig) {
	fetchCtx, stopFetch := context.WithCancel(ctx)
	defer stopFetch()

	fetchResults := make(chan []*common.CdxResponse)

	var wg sync.WaitGroup
	for i := uint(0); i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fs.worker(fetchCtx, configs, fetchResults)
		}()
	}
	go func() {
		wg.Wait()
		close(fetchResults)
	}()

	saverDone := make(chan struct{})
	go func() {
		defer close(saverDone)
		common.SaveFiles(ctx, results, fs.outputDir, errors, fs.downloadRate)
	}()

	go func() {
		defer close(results)
		var collected uint
		for batch := range fetchResults {
			if maxResults != 0 && collected+uint(len(batch)) > maxResults {
				batch = batch[:maxResults-collected]
			}
			if len(batch) > 0 {
				if !common.SendResults(ctx, results, batch) {
					return
				}
				collected += uint(len(batch))
			}
			if maxResults != 0 && collected >= maxResults {
				stopFetch()
			}
		}
	}()

	go func() {
		<-saverDone
		close(errors)
	}()

	for err := range errors {
		log.Printf("ERROR: %v\n", err)
	}
}

func (fs *fileScenario) spawnWorkers(cmd *cobra.Command, args []string) {
	fp, _ := filepath.Abs(fs.outputDir)
	err := os.MkdirAll(fp, os.ModePerm)
	if err != nil {
		log.Fatalf("Cannot get access to '%v' dir: %v", fileScn.outputDir, err)
	} else {
		log.Printf("Setting '%v' as output directorty", fp)
	}

	configs := getRequestConfigs(args)
	initSources()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	close(configs)
	fs.run(ctx, configs)
}

func init() {
	fileCMD.Flags().StringVarP(&fileScn.outputDir, "dir", "d", "", "Path to the output directory")
	fileCMD.Flags().Float32VarP(&fileScn.downloadRate, "rate", "", 1.0, "Download rate in seconds for each worker (thread). Ex: 5, 1.5")
	rootCmd.AddCommand(fileCMD)
	fileCMD.MarkFlagRequired("dir")
}
