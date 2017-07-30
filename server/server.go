package server

import (
	"go.uber.org/zap"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/xorlev/slogd/proto"
)

func Run(logger *zap.SugaredLogger, rpcAddr string, httpAddr string, dataDir string) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	lis, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	logger.Infof("Starting RPC listener on %s", rpcAddr)

	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_zap.StreamServerInterceptor(logger.Desugar()),
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			requestStartInterceptor,
			grpc_zap.UnaryServerInterceptor(logger.Desugar()),
		)))

	config := &Config{
		DataDir: dataDir,
	}

	sls, err := NewRpcHandler(logger, config)
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
				logger.Info("Shutting down...")

				go srv.Shutdown(ctx)

				gracefulStopChan := make(chan bool, 1)
				go func() {
					grpcServer.GracefulStop()
					gracefulStopChan <- true
				}()

				t := time.NewTimer(3 * time.Second)
				select {
				case <-gracefulStopChan:
				case <-t.C:
					logger.Info("Graceful stop failed, closing pending RPCs.")
					// Stop non-gracefully
					grpcServer.Stop()
				}

				sls.Close()
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

// type StreamServerInterceptor func(srv interface{}, ss ServerStream, info *StreamServerInfo, handler StreamHandler) error
// type UnaryServerInterceptor func(ctx context.Context, req interface{}, info *UnaryServerInfo, handler UnaryHandler) (resp interface{}, err error)

func requestStartInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	ctx = context.WithValue(ctx, "requestStart", time.Now())

	return handler(ctx, req)
}
