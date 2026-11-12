package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/karust/gogetcrawl/common"
	"github.com/spf13/cobra"
)

type urlScenario struct {
	outputFile string
}

var urlScn = urlScenario{}

var urlCMD = &cobra.Command{
	Use:     "url",
	Aliases: []string{"collect"},
	Short:   "Collect URLs from web archives for desired domain",
	Args:    cobra.MatchAll(cobra.MinimumNArgs(1), cobra.OnlyValidArgs),
	Run:     urlScn.spawnWorkers,
}

// worker consumes request configs and fans each one out to all sources.
// It returns once the configs channel is drained; every source goroutine it
// spawns is joined before the worker moves on or exits.
func (us *urlScenario) worker(ctx context.Context, configs <-chan common.RequestConfig) {
	for config := range configs {
		var wg sync.WaitGroup

		for _, s := range sources {
			wg.Add(1)
			go func(s common.Source) {
				defer wg.Done()
				s.FetchPages(ctx, config, results, errors)
			}(s)
		}
		wg.Wait()
	}
}

// run drives the fan-in: it starts the workers, closes the result and error
// channels only after every worker finished (so nothing ever sends on a
// closed channel), and collects batches until both channels are closed.
// maxResults, when set, applies exactly to the total number of usable
// results written to the output, across all sources; reaching it cancels the
// remaining work. It returns the number of results written.
func (us *urlScenario) run(ctx context.Context, cancel context.CancelFunc, configs <-chan common.RequestConfig, output io.Writer) uint {
	var wg sync.WaitGroup

	// Spawn Workers
	for i := uint(0); i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			us.worker(ctx, configs)
		}()
	}

	go func() {
		wg.Wait()
		close(results)
		close(errors)
	}()

	resultsCh, errorsCh := results, errors
	var collected uint

	for resultsCh != nil || errorsCh != nil {
		select {
		case res, ok := <-resultsCh:
			if !ok {
				resultsCh = nil
				continue
			}
			if maxResults != 0 && collected+uint(len(res)) > maxResults {
				res = res[:maxResults-collected]
			}
			if len(res) == 0 {
				continue
			}
			fmt.Fprintf(output, "%v", us.formatResultOutput(res))
			collected += uint(len(res))
			if maxResults != 0 && collected >= maxResults {
				cancel()
			}
		case err, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			log.Println(err)
		}
	}

	return collected
}

func (us *urlScenario) spawnWorkers(cmd *cobra.Command, args []string) {
	output, err := us.getOutputTarget()
	if err != nil {
		log.Fatalf("Error obtaining output: %v", err)
	}

	configs := getRequestConfigs(args)
	initSources()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	close(configs)
	us.run(ctx, cancel, configs, output)
}

func (us *urlScenario) getOutputTarget() (io.Writer, error) {
	if us.outputFile != "" {
		file, err := os.OpenFile(us.outputFile, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return nil, err
		}
		return file, nil
	}

	return os.Stdout, nil
}

func (us *urlScenario) formatResultOutput(results []*common.CdxResponse) string {
	output := ""
	for _, r := range results {
		output += r.Original + "\n"
	}
	return output
}

func init() {
	urlCMD.Flags().StringVarP(&urlScn.outputFile, "output", "o", "", "Path to the output file")
	rootCmd.AddCommand(urlCMD)
}
