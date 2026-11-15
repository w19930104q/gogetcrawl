package cmd

import (
	"log"
	"os"
	"path/filepath"
	"sync"

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

	ctx, stop := newSignalContext()
	defer stop()

	var wg sync.WaitGroup

	// Spawn Workers
	for i := uint(0); i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for config := range configs {
				results, err := common.FetchAll(ctx, sources, config)
				if err != nil {
					log.Printf("ERROR: %v\n", err)
				}

				if err := common.SaveResults(ctx, results, fs.outputDir, fs.downloadRate); err != nil {
					log.Printf("ERROR: %v\n", err)
				}
			}
		}()
	}

	close(configs)
	wg.Wait()
}

func init() {
	fileCMD.Flags().StringVarP(&fileScn.outputDir, "dir", "d", "", "Path to the output directory")
	fileCMD.Flags().Float32VarP(&fileScn.downloadRate, "rate", "", 1.0, "Download rate in seconds for each worker (thread). Ex: 5, 1.5")
	rootCmd.AddCommand(fileCMD)
	fileCMD.MarkFlagRequired("dir")
}
