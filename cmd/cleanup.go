package cmd

import (
	"flag"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/xorlev/slogd/storage"
)

// cleanupCmd runs automated cleanups once, offline.
var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := zap.NewDevelopmentConfig()
		logger, _ := cfg.Build()
		defer logger.Sync() // flushes buffer, if any

		if verbose {
			cfg.Level.SetLevel(zap.DebugLevel)
			cfg.Development = true
		} else {
			cfg.Level.SetLevel(zap.InfoLevel)
			cfg.Development = false
		}

		flag.Parse()

		logs, err := storage.OpenLogs(logger.Sugar(), dataDir)
		if err != nil {
			logger.Fatal("Failed to run cleanup", zap.Error(err))
		}

		for topic, log := range logs {
			if err := log.Cleanup(); err != nil {
				logger.Fatal("Failed to run cleanup for topic", zap.String("topic", topic), zap.Error(err))
			}
		}
	},
}

var (
	topics []string
)

func init() {
	RootCmd.AddCommand(cleanupCmd)
	cleanupCmd.Flags().StringArrayVarP(&topics, "topics", "t", []string{}, "topics")
	cleanupCmd.Flags().StringVarP(&dataDir, "data_dir", "", "./data", "data directory")
	cleanupCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "enable verbose debug logging")
}
