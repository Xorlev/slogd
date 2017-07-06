package main

import (
	"flag"
	"go.uber.org/zap"
	"net"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/xorlev/slogd/proto"
	"github.com/xorlev/slogd/server"
)

var (
	httpAddr = flag.String("http_addr", "localhost:8080", "slogd HTTP listener")
	rpcAddr  = flag.String("rpc_addr", "localhost:9090", "slogd gRPC listener")
	dataDir  = flag.String("data_dir", "./data", "data directory")
)

func run(logger *zap.SugaredLogger) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lis, err := net.Listen("tcp", *rpcAddr)
	if err != nil {
		return err
	}

	logger.Infof("Starting RPC listener on %s", *rpcAddr)

	grpcServer := grpc.NewServer()

	config := &server.Config{
		DataDir: *dataDir,
	}

	sls, err := server.NewRpcHandler(logger, config)
	if err != nil {
		return err
	}

	pb.RegisterStructuredLogServer(grpcServer, sls)

	go grpcServer.Serve(lis)

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}
	err = pb.RegisterStructuredLogHandlerFromEndpoint(ctx, mux, *rpcAddr, opts)
	if err != nil {
		return err
	}

	logger.Infof("Starting HTTP listener on %s", *httpAddr)
	return http.ListenAndServe(*httpAddr, mux)
}

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync() // flushes buffer, if any
	flag.Parse()

	if err := run(logger.Sugar()); err != nil {
		logger.Fatal("Failed to start slogd", zap.Error(err))
	}
}

// func main() {
// 	url := "Test"

// 	logger, _ := zap.NewProduction()
// 	defer logger.Sync() // flushes buffer, if any
// 	sugar := logger.Sugar()
// 	sugar.Infow("Failed to fetch URL.",
// 		// Structured context as loosely-typed key-value pairs.
// 		"url", url,
// 		"attempt", 3,
// 		"backoff", time.Second,
// 	)
// 	sugar.Infof("Failed to fetch URL: %s", url)

// }
