package cmd

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/spf13/cobra"
	pb "github.com/xorlev/slogd/proto"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"os"
	"time"
)

var (
	startOffset    uint64
	startTimestamp string
)

// produceCmd represents the produce command
var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "consumes messages from slogd, exports as textproto/proto/JSON to stdout",
	Long:  "consumes messages from slogd, exports as textproto/proto/JSON to stdout",
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

		var req *pb.GetLogsRequest = nil
		if len(startTimestamp) == 0 {
			req = &pb.GetLogsRequest{
				Topic: topic,
				StartAt: &pb.GetLogsRequest_Offset{
					Offset: startOffset,
				},
			}
		} else {
			ts, err := time.Parse(time.RFC3339Nano, startTimestamp)
			if err != nil {
				fmt.Printf("Error parsing timestamp: %v", err)
				os.Exit(1)
			}

			timeProto, err := types.TimestampProto(ts)
			if err != nil {
				fmt.Printf("Error parsing timestamp: %v", err)
				os.Exit(1)
			}

			req = &pb.GetLogsRequest{
				Topic: topic,
				StartAt: &pb.GetLogsRequest_Timestamp{
					Timestamp: timeProto,
				},
			}
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
	consumeCmd.Flags().StringVar(&startTimestamp, "start_timestamp", "", "timestamp to start consuming from")
}
