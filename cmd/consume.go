// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"
	pb "github.com/xorlev/slogd/proto"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"os"
)

var (
	startOffset uint64
)

// produceCmd represents the produce command
var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		if topic == "" {
			fmt.Printf("Must provide a topic name\n")
			os.Exit(1)
		}

		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		opts := []grpc.DialOption{grpc.WithInsecure()}
		conn, err := grpc.Dial(rpcAddr, opts...)
		if err != nil {
			fmt.Printf("fail to dial: %v", err)
			os.Exit(1)
		}
		defer conn.Close()
		client := pb.NewStructuredLogClient(conn)

		req := &pb.GetLogsRequest{
			Topic: topic,
		}

		slc, err := client.StreamLogs(ctx, req)
		if err != nil {
			fmt.Printf("Failed to stream logs: %v\n", err)
			os.Exit(1)
		}

		for {
			log, err := slc.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("Failed to stream logs: %v\n", err)
				os.Exit(1)
			}

			log.GetLogs()

			b, _ := proto.Marshal(log)
			os.Stdout.Write(b)
		}
	},
}

func init() {
	RootCmd.AddCommand(consumeCmd)

	consumeCmd.Flags().StringVar(&rpcAddr, "server_addr", "localhost:8080", "slogd server")
	consumeCmd.Flags().StringVar(&topic, "topic", "", "slogd topic to produce to")
	consumeCmd.Flags().Uint64Var(&startOffset, "start_offset", 0, "offset to start streaming from")
}
