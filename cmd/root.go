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
	Short: "Hugo is a very fast static site generator",
	Long: `A Fast and Flexible Static Site Generator built with
                love by spf13 and friends in Go.
                Complete documentation is available at http://hugo.spf13.com`,
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}
