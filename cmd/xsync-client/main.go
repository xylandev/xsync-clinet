package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/xylandev/xsync-clinet/internal/config"
	"github.com/xylandev/xsync-clinet/internal/syncer"
)

var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	if len(os.Args[1]) > 0 && os.Args[1][0] == '-' {
		err = runCommand(os.Args[1:])
	} else {
		switch os.Args[1] {
		case "init":
			err = initCommand(os.Args[2:])
		case "validate-config":
			err = validateCommand(os.Args[2:])
		case "run":
			err = runCommand(os.Args[2:])
		case "version":
			fmt.Printf("xsync-client %s (commit %s, built %s)\n", version, commit, buildDate)
		default:
			usage()
			os.Exit(2)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "xsync-client:", err)
		os.Exit(1)
	}
}
func usage() {
	fmt.Fprintln(os.Stderr, "usage: xsync-client --client-config <account.yaml> --dist <directory>")
	fmt.Fprintln(os.Stderr, "       xsync-client <init|validate-config|run|version> [options]")
}
func configFlag(name string, args []string) (string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	p := fs.String("config", "./xsync-client.yaml", "configuration file")
	if err := fs.Parse(args); err != nil {
		return "", err
	}
	return *p, nil
}
func validateCommand(args []string) error {
	p, err := configFlag("validate-config", args)
	if err != nil {
		return err
	}
	_, err = config.Load(p)
	if err == nil {
		fmt.Println("configuration is valid")
	}
	return err
}
func runCommand(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	p := fs.String("config", "", "legacy downloader configuration file")
	bundlePath := fs.String("client-config", "", "server-generated account connection bundle")
	destination := fs.String("dist", "", "destination directory")
	prefix := fs.String("prefix", "", "optional remote path prefix")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if (*p == "") == (*bundlePath == "") {
		return fmt.Errorf("specify exactly one of --client-config or --config")
	}
	var cfg config.Config
	var err error
	if *bundlePath != "" {
		if *destination == "" {
			return fmt.Errorf("--dist is required with --client-config")
		}
		cfg, err = config.LoadBundle(*bundlePath, *destination)
	} else {
		cfg, err = config.Load(*p)
		if err == nil && *destination != "" {
			cfg.Destination = *destination
		}
	}
	if err != nil {
		return err
	}
	if *prefix != "" {
		cfg.Prefix = *prefix
	}
	if err = cfg.Validate(); err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	client, err := syncer.New(cfg, log)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return client.Run(ctx)
}
func initCommand(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	p := fs.String("config", "./xsync-client.yaml", "output configuration")
	endpoint := fs.String("endpoint", "https://127.0.0.1:9443", "server endpoint")
	key := fs.String("api-key", "", "download API key")
	ca := fs.String("ca-file", "./tls.crt", "server CA certificate")
	dest := fs.String("destination", "./downloads", "destination directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg := config.Default()
	cfg.Endpoint = *endpoint
	cfg.APIKey = *key
	cfg.CAFile = *ca
	cfg.Destination = *dest
	return config.Write(*p, cfg)
}
