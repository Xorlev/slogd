package cmd

import (
	"flag"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/xorlev/slogd/server"
)

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "server",
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

		if err := server.Run(logger.Sugar(), rpcAddr, httpAddr, dataDir); err != nil {
			logger.Fatal("Failed to start slogd", zap.Error(err))
		}
	},
}

var (
	httpAddr string
	dataDir  string
	verbose  bool
)

func init() {
	RootCmd.AddCommand(serverCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// serverCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	serverCmd.Flags().StringVarP(&httpAddr, "http_addr", "", "localhost:8080", "slogd HTTP listener")
	serverCmd.Flags().StringVarP(&rpcAddr, "rpc_addr", "", "localhost:9090", "slogd gRPC listener")
	serverCmd.Flags().StringVarP(&dataDir, "data_dir", "", "./data", "data directory")
	serverCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "enable verbose debug logging")
}
