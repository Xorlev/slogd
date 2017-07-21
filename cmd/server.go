package cmd

import (
	"flag"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/xorlev/slogd/proto"
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
		logger, _ := zap.NewProduction()
		defer logger.Sync() // flushes buffer, if any
		flag.Parse()

		if err := run(logger.Sugar()); err != nil {
			logger.Fatal("Failed to start slogd", zap.Error(err))
		}
	},
}

var (
	httpAddr string
	dataDir  string
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
}

func run(logger *zap.SugaredLogger) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	lis, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	logger.Infof("Starting RPC listener on %s", rpcAddr)

	grpcServer := grpc.NewServer()

	config := &server.Config{
		DataDir: dataDir,
	}

	sls, err := server.NewRpcHandler(logger, config)
	if err != nil {
		return err
	}

	pb.RegisterStructuredLogServer(grpcServer, sls)

	go grpcServer.Serve(lis)

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}
	err = pb.RegisterStructuredLogHandlerFromEndpoint(ctx, mux, rpcAddr, opts)
	if err != nil {
		return err
	}
	srv := &http.Server{Addr: httpAddr, Handler: mux}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			if sig == os.Interrupt {
				srv.Shutdown(ctx)
				grpcServer.GracefulStop()
				sls.Close()
				logger.Info("Shutting down.")
				cancel()
				os.Exit(0)
			}
		}
	}()

	logger.Infof("Starting HTTP listener on %s", httpAddr)
	go srv.ListenAndServe()

	<-ctx.Done()

	return nil
}
