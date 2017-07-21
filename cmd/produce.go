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
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	pb "github.com/xorlev/slogd/proto"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"os"
)

var (
	topic string
)

// produceCmd represents the produce command
var produceCmd = &cobra.Command{
	Use:   "produce",
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

		scanner := bufio.NewScanner(os.Stdin)

		lineCh := make(chan []byte, 100)
		go func() {
			for scanner.Scan() {
				lineCh <- scanner.Bytes()
			}

			close(lineCh)
		}()

		var buffer [][]byte = make([][]byte, 0)

		for {
			line, more := <-lineCh
			if more {
				buffer = append(buffer, line)

				if len(buffer) >= 100 {
					if err := sendEntries(client, buffer); err != nil {
						fmt.Printf("Failed to send entries: %+v", err)
						os.Exit(1)
					}

					buffer = make([][]byte, 0)
				}
			} else {
				if err := sendEntries(client, buffer); err != nil {
					fmt.Printf("Failed to send entries: %+v", err)
					os.Exit(1)
				}
				buffer = nil
				break
			}
		}

		fmt.Printf("Done")
	},
}

func sendEntries(client pb.StructuredLogClient, buffer [][]byte) error {
	fmt.Printf("Sending %d entries\n", len(buffer))

	entries := make([]*pb.LogEntry, 0)
	for _, l := range buffer {
		entry := &pb.LogEntry{
			Entry: &pb.LogEntry_RawBytes{
				RawBytes: l,
			},
		}

		entries = append(entries, entry)
	}
	_, err := client.AppendLogs(context.Background(), &pb.AppendRequest{
		Topic: topic,
		Logs:  entries,
	})
	if err != nil {
		return err
	}

	return nil
}

func init() {
	RootCmd.AddCommand(produceCmd)

	produceCmd.Flags().StringVar(&rpcAddr, "server_addr", "localhost:8080", "slogd server")
	produceCmd.Flags().StringVar(&topic, "topic", "", "slogd topic to produce to")
}