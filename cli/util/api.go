package cliutil

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-project/lotus/lib/retry"
	"go.uber.org/atomic"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/node/repo"
)

const (
	metadataTraceContext = "traceContext"
)

// GetAPIInfo returns the API endpoint to use for the specified kind of repo.
//
// The order of precedence is as follows:
//
//  1. *-api-url command line flags.
//  2. *_API_INFO environment variables
//  3. deprecated *_API_INFO environment variables
//  4. *-repo command line flags.
func GetAPIInfoMulti(ctx *cli.Context, t repo.RepoType) ([]APIInfo, error) {
	// Check if there was a flag passed with the listen address of the API
	// server (only used by the tests)
	for _, f := range t.APIFlags() {
		if !ctx.IsSet(f) {
			continue
		}
		strma := ctx.String(f)
		strma = strings.TrimSpace(strma)

		return []APIInfo{APIInfo{Addr: strma}}, nil
	}

	//
	// Note: it is not correct/intuitive to prefer environment variables over
	// CLI flags (repo flags below).
	//
	primaryEnv, fallbacksEnvs, deprecatedEnvs := t.APIInfoEnvVars()
	env, ok := os.LookupEnv(primaryEnv)
	if ok {
		return ParseApiInfoMulti(env), nil
	}

	for _, env := range deprecatedEnvs {
		env, ok := os.LookupEnv(env)
		if ok {
			log.Warnf("Using deprecated env(%s) value, please use env(%s) instead.", env, primaryEnv)
			return ParseApiInfoMulti(env), nil
		}
	}

	for _, f := range t.RepoFlags() {
		// cannot use ctx.IsSet because it ignores default values
		path := ctx.String(f)
		if path == "" {
			continue
		}

		p, err := homedir.Expand(path)
		if err != nil {
			return []APIInfo{}, xerrors.Errorf("could not expand home dir (%s): %w", f, err)
		}

		r, err := repo.NewFS(p)
		if err != nil {
			return []APIInfo{}, xerrors.Errorf("could not open repo at path: %s; %w", p, err)
		}

		exists, err := r.Exists()
		if err != nil {
			return []APIInfo{}, xerrors.Errorf("repo.Exists returned an error: %w", err)
		}

		if !exists {
			return []APIInfo{}, errors.New("repo directory does not exist. Make sure your configuration is correct")
		}

		ma, err := r.APIEndpoint()
		if err != nil {
			return []APIInfo{}, xerrors.Errorf("could not get api endpoint: %w", err)
		}

		token, err := r.APIToken()
		if err != nil {
			log.Warnf("Couldn't load CLI token, capabilities may be limited: %v", err)
		}

		return []APIInfo{APIInfo{
			Addr:  ma.String(),
			Token: token,
		}}, nil
	}

	for _, env := range fallbacksEnvs {
		env, ok := os.LookupEnv(env)
		if ok {
			return ParseApiInfoMulti(env), nil
		}
	}

	return []APIInfo{}, fmt.Errorf("could not determine API endpoint for node type: %v", t.Type())
}

func GetAPIInfo(ctx *cli.Context, t repo.RepoType) (APIInfo, error) {
	ainfos, err := GetAPIInfoMulti(ctx, t)
	if err != nil {
		return APIInfo{}, err
	}

	if len(ainfos) > 1 {
		log.Warn("multiple API infos received when only one was expected")
	}

	return ainfos[0], nil

}

type HttpHead struct {
	addr   string
	header http.Header
}

func GetRawAPIMulti(ctx *cli.Context, t repo.RepoType, version string) ([]HttpHead, error) {

	var httpHeads []HttpHead
	ainfos, err := GetAPIInfoMulti(ctx, t)
	if err != nil {
		return httpHeads, xerrors.Errorf("could not get API info for %s: %w", t.Type(), err)
	}

	for _, ainfo := range ainfos {
		addr, err := ainfo.DialArgs(version)
		if err != nil {
			return httpHeads, xerrors.Errorf("could not get DialArgs: %w", err)
		}
		httpHeads = append(httpHeads, HttpHead{addr: addr, header: ainfo.AuthHeader()})
	}

	//addr, err := ainfo.DialArgs(version)
	//if err != nil {
	//	return "", nil, xerrors.Errorf("could not get DialArgs: %w", err)
	//}

	if IsVeryVerbose {
		_, _ = fmt.Fprintf(ctx.App.Writer, "using raw API %s endpoint: %s\n", version, httpHeads[0].addr)
	}

	return httpHeads, nil
}

func GetRawAPI(ctx *cli.Context, t repo.RepoType, version string) (string, http.Header, error) {
	heads, err := GetRawAPIMulti(ctx, t, version)
	if err != nil {
		return "", nil, err
	}

	if len(heads) > 1 {
		log.Warnf("More than 1 header received when expecting only one")
	}

	return heads[0].addr, heads[0].header, nil
}

func GetCommonAPI(ctx *cli.Context) (api.CommonNet, jsonrpc.ClientCloser, error) {
	ti, ok := ctx.App.Metadata["repoType"]
	if !ok {
		log.Errorf("unknown repo type, are you sure you want to use GetCommonAPI?")
		ti = repo.FullNode
	}
	t, ok := ti.(repo.RepoType)
	if !ok {
		log.Errorf("repoType type does not match the type of repo.RepoType")
	}

	if tn, ok := ctx.App.Metadata["testnode-storage"]; ok {
		return tn.(api.StorageMiner), func() {}, nil
	}
	if tn, ok := ctx.App.Metadata["testnode-full"]; ok {
		return tn.(api.FullNode), func() {}, nil
	}

	addr, headers, err := GetRawAPI(ctx, t, "v0")
	if err != nil {
		return nil, nil, err
	}

	return client.NewCommonRPCV0(ctx.Context, addr, headers)
}

func GetFullNodeAPI(ctx *cli.Context) (v0api.FullNode, jsonrpc.ClientCloser, error) {
	// use the mocked API in CLI unit tests, see cli/mocks_test.go for mock definition
	if mock, ok := ctx.App.Metadata["test-full-api"]; ok {
		return &v0api.WrapperV1Full{FullNode: mock.(v1api.FullNode)}, func() {}, nil
	}

	if tn, ok := ctx.App.Metadata["testnode-full"]; ok {
		return &v0api.WrapperV1Full{FullNode: tn.(v1api.FullNode)}, func() {}, nil
	}

	addr, headers, err := GetRawAPI(ctx, repo.FullNode, "v0")
	if err != nil {
		return nil, nil, err
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using full node API v0 endpoint:", addr)
	}

	return client.NewFullNodeRPCV0(ctx.Context, addr, headers)
}

func RouteRequest() {

}

func FullNodeProxy[T api.FullNode](ins []T, outstr *api.FullNodeStruct) {
	outs := api.GetInternalStructs(outstr)

	var rins []reflect.Value
	//peertoNode := make(map[peer.ID]reflect.Value)
	for _, in := range ins {
		rin := reflect.ValueOf(in)
		rins = append(rins, rin)
		//peertoNode[ins] = rin
	}

	for _, out := range outs {
		rint := reflect.ValueOf(out).Elem()
		//ra := reflect.ValueOf(in)

		for f := 0; f < rint.NumField(); f++ {
			field := rint.Type().Field(f)

			var fns []reflect.Value
			for _, rin := range rins {
				fns = append(fns, rin.MethodByName(field.Name))
			}
			//fn := ra.MethodByName(field.Name)

			//retryFunc := func(args []reflect.Value) (results []reflect.Value) {
			//	//ctx := args[0].Interface().(context.Context)
			//	//
			//	//rin := peertoNode[ins[0].Leader(ctx)]
			//	//fn := rin.MethodByName(field.Name)
			//	//
			//	//return fn.Call(args)
			//
			//	toCall := curr
			//	curr += 1 % total
			//	return fns[toCall].Call(args)
			//}

			rint.Field(f).Set(reflect.MakeFunc(field.Type, func(args []reflect.Value) (results []reflect.Value) {
				errorsToRetry := []error{&jsonrpc.RPCConnectionError{}, &jsonrpc.ErrClient{}}
				initialBackoff, err := time.ParseDuration("1s")
				if err != nil {
					return nil
				}
				var curr atomic.Int64
				curr.Store(-1)
				total := len(rins)
				ctx := args[0].Interface().(context.Context)
				result, err := retry.Retry(ctx, 5, initialBackoff, errorsToRetry, func() (results []reflect.Value, err2 error) {
					//ctx := args[0].Interface().(context.Context)
					//
					//rin := peertoNode[ins[0].Leader(ctx)]
					//fn := rin.MethodByName(field.Name)
					//
					//return fn.Call(args)

					toCall := curr.Inc() % int64(total)

					result := fns[toCall].Call(args)
					if result[len(result)-1].IsNil() {
						return result, nil
					}
					e := result[len(result)-1].Interface().(error)
					return result, e
				})
				return result
				//return fns[0].Call(args)
			}))
		}
	}
}

func GetFullNodeAPIV1(ctx *cli.Context) (v1api.FullNode, jsonrpc.ClientCloser, error) {
	if tn, ok := ctx.App.Metadata["testnode-full"]; ok {
		return tn.(v1api.FullNode), func() {}, nil
	}

	addr, headers, err := GetRawAPI(ctx, repo.FullNode, "v1")
	if err != nil {
		return nil, nil, err
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using full node API v1 endpoint:", addr)
	}

	v1API, closer, err := client.NewFullNodeRPCV1(ctx.Context, addr, headers)
	if err != nil {
		return nil, nil, err
	}

	v, err := v1API.Version(ctx.Context)
	if err != nil {
		return nil, nil, err
	}
	if !v.APIVersion.EqMajorMinor(api.FullAPIVersion1) {
		return nil, nil, xerrors.Errorf("Remote API version didn't match (expected %s, remote %s)", api.FullAPIVersion1, v.APIVersion)
	}
	return v1API, closer, nil
}

func GetFullNodeAPIV1New(ctx *cli.Context) (v1api.FullNode, jsonrpc.ClientCloser, error) {
	if tn, ok := ctx.App.Metadata["testnode-full"]; ok {
		return tn.(v1api.FullNode), func() {}, nil
	}

	heads, err := GetRawAPIMulti(ctx, repo.FullNode, "v1")
	if err != nil {
		return nil, nil, err
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using full node API v1 endpoint:", heads[0].addr)
	}

	var fullNodes []api.FullNode
	var closers []jsonrpc.ClientCloser

	for _, head := range heads {
		v1api, closer, err := client.NewFullNodeRPCV1(ctx.Context, head.addr, head.header)
		if err != nil {
			return nil, nil, err
		}
		fullNodes = append(fullNodes, v1api)
		closers = append(closers, closer)
	}

	finalCloser := func() {
		for _, c := range closers {
			c()
		}
	}

	var v1API api.FullNodeStruct
	FullNodeProxy(fullNodes, &v1API)

	v, err := v1API.Version(ctx.Context)
	if err != nil {
		return nil, nil, err
	}
	if !v.APIVersion.EqMajorMinor(api.FullAPIVersion1) {
		return nil, nil, xerrors.Errorf("Remote API version didn't match (expected %s, remote %s)", api.FullAPIVersion1, v.APIVersion)
	}
	return &v1API, finalCloser, nil
}

type GetStorageMinerOptions struct {
	PreferHttp bool
}

type GetStorageMinerOption func(*GetStorageMinerOptions)

func StorageMinerUseHttp(opts *GetStorageMinerOptions) {
	opts.PreferHttp = true
}

func GetStorageMinerAPI(ctx *cli.Context, opts ...GetStorageMinerOption) (api.StorageMiner, jsonrpc.ClientCloser, error) {
	var options GetStorageMinerOptions
	for _, opt := range opts {
		opt(&options)
	}

	if tn, ok := ctx.App.Metadata["testnode-storage"]; ok {
		return tn.(api.StorageMiner), func() {}, nil
	}

	addr, headers, err := GetRawAPI(ctx, repo.StorageMiner, "v0")
	if err != nil {
		return nil, nil, err
	}

	if options.PreferHttp {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, nil, xerrors.Errorf("parsing miner api URL: %w", err)
		}

		switch u.Scheme {
		case "ws":
			u.Scheme = "http"
		case "wss":
			u.Scheme = "https"
		}

		addr = u.String()
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using miner API v0 endpoint:", addr)
	}

	return client.NewStorageMinerRPCV0(ctx.Context, addr, headers)
}

func GetWorkerAPI(ctx *cli.Context) (api.Worker, jsonrpc.ClientCloser, error) {
	addr, headers, err := GetRawAPI(ctx, repo.Worker, "v0")
	if err != nil {
		return nil, nil, err
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using worker API v0 endpoint:", addr)
	}

	return client.NewWorkerRPCV0(ctx.Context, addr, headers)
}

func GetMarketsAPI(ctx *cli.Context) (api.StorageMiner, jsonrpc.ClientCloser, error) {
	// to support lotus-miner cli tests.
	if tn, ok := ctx.App.Metadata["testnode-storage"]; ok {
		return tn.(api.StorageMiner), func() {}, nil
	}

	addr, headers, err := GetRawAPI(ctx, repo.Markets, "v0")
	if err != nil {
		return nil, nil, err
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using markets API v0 endpoint:", addr)
	}

	// the markets node is a specialised miner's node, supporting only the
	// markets API, which is a subset of the miner API. All non-markets
	// operations will error out with "unsupported".
	return client.NewStorageMinerRPCV0(ctx.Context, addr, headers)
}

func GetGatewayAPI(ctx *cli.Context) (api.Gateway, jsonrpc.ClientCloser, error) {
	addr, headers, err := GetRawAPI(ctx, repo.FullNode, "v1")
	if err != nil {
		return nil, nil, err
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using gateway API v1 endpoint:", addr)
	}

	return client.NewGatewayRPCV1(ctx.Context, addr, headers)
}

func GetGatewayAPIV0(ctx *cli.Context) (v0api.Gateway, jsonrpc.ClientCloser, error) {
	addr, headers, err := GetRawAPI(ctx, repo.FullNode, "v0")
	if err != nil {
		return nil, nil, err
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using gateway API v0 endpoint:", addr)
	}

	return client.NewGatewayRPCV0(ctx.Context, addr, headers)
}

func DaemonContext(cctx *cli.Context) context.Context {
	if mtCtx, ok := cctx.App.Metadata[metadataTraceContext]; ok {
		return mtCtx.(context.Context)
	}

	return context.Background()
}

// ReqContext returns context for cli execution. Calling it for the first time
// installs SIGTERM handler that will close returned context.
// Not safe for concurrent execution.
func ReqContext(cctx *cli.Context) context.Context {
	tCtx := DaemonContext(cctx)

	ctx, done := context.WithCancel(tCtx)
	sigChan := make(chan os.Signal, 2)
	go func() {
		<-sigChan
		done()
	}()
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	return ctx
}
