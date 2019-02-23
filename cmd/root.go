package cmd

import (
	"github.com/spf13/cobra"
)

var (
	topic   string
	rpcAddr string
)

var RootCmd = &cobra.Command{
	Use:   "slogd",
	Short: "slogd is a lightweight structured logging daemon",
	Long:  "slogd is a lightweight structured logging daemon exposed via HTTP and gRPC",
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Help()
	},
}
