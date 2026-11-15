package cmd

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"

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

func (us *urlScenario) spawnWorkers(cmd *cobra.Command, args []string) {
	output, err := us.getOutputTarget()
	if err != nil {
		log.Fatalf("Error obtaining output: %v", err)
	}

	configs := getRequestConfigs(args)
	initSources()

	ctx, stop := newSignalContext()
	defer stop()

	var wg sync.WaitGroup
	var outMu sync.Mutex

	// Spawn Workers
	for i := uint(0); i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for config := range configs {
				results, err := common.FetchAll(ctx, sources, config)
				if err != nil {
					log.Println(err)
				}

				outMu.Lock()
				fmt.Fprintf(output, "%v", us.formatResultOutput(results))
				outMu.Unlock()
			}
		}()
	}

	close(configs)
	wg.Wait()
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
