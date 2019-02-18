package server

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"go.uber.org/zap"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/xorlev/slogd/proto"

	"net/http/pprof"
)

func Run(logger *zap.SugaredLogger, rpcAddr string, httpAddr string, dataDir string) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	lis, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	logger.Infof("Starting RPC listener on %s", rpcAddr)

	desugaredLogger := logger.Desugar()
	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_zap.StreamServerInterceptor(desugaredLogger),
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			requestStartInterceptor,
			grpc_zap.UnaryServerInterceptor(desugaredLogger),
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

	r := http.NewServeMux()
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)

	r.Handle("/", mux)

	srv := &http.Server{Addr: httpAddr, Handler: r}

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

func requestStartInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	ctx = context.WithValue(ctx, "requestStart", time.Now())

	return handler(ctx, req)
}
