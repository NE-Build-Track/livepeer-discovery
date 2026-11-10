package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NE-Build-Track/livepeer-onchain-discovery/internal/directory"
	"github.com/NE-Build-Track/livepeer-onchain-discovery/internal/serve"
	"github.com/livepeer/node/eth"
)

func main() {
	if err := run(os.Args[1:], os.Getenv, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func run(args []string, getenv func(string) string, logOut *os.File) error {
	settings, err := serve.LoadSettings(args, getenv)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(logOut, nil))
	rpc, err := eth.OpenRPC(settings.RPCURL)
	if err != nil {
		return err
	}
	defer rpc.Close()
	contracts, err := eth.OpenContracts(rpc, settings.Controller)
	if err != nil {
		return err
	}
	settings.Config.Directory = &directory.Directory{
		Chain: contracts, ServiceRegistry: settings.ServiceRegistry, Logger: logger,
	}
	settings.Config.Logger = logger
	service, err := serve.New(settings.Config)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", settings.Listen)
	if err != nil {
		return fmt.Errorf("cannot listen on %s", settings.Listen)
	}
	defer listener.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	server := &http.Server{
		Handler: service, BaseContext: func(net.Listener) context.Context { return ctx },
		ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second,
		MaxHeaderBytes: 16 << 10,
	}
	errs := make(chan error, 1)
	go func() { errs <- server.Serve(listener) }()
	done := make(chan struct{})
	go func() {
		defer close(done)
		service.Run(ctx)
	}()
	logger.Info("discovery started", "listen", settings.Listen, "chain", settings.RPCURL != "")
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errs:
		stop()
	}
	stop()
	<-done
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}
