package main

import (
	"fmt"

	"crypto/rand"
	"strconv"

	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/urfave/cli/v2"
)

var miscCmd = &cli.Command{
	Name:  "misc",
	Usage: "Assorted unsorted commands for various purposes",
	Flags: []cli.Flag{},
	Subcommands: []*cli.Command{
		dealStateMappingCmd,
		computeWinCountCmd,
	},
}

var dealStateMappingCmd = &cli.Command{
	Name: "deal-state",
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return cli.ShowCommandHelp(cctx, cctx.Command.Name)
		}

		num, err := strconv.Atoi(cctx.Args().First())
		if err != nil {
			return err
		}

		ststr, ok := storagemarket.DealStates[uint64(num)]
		if !ok {
			return fmt.Errorf("no such deal state %d", num)
		}
		fmt.Println(ststr)
		return nil
	},
}

var computeWinCountCmd = &cli.Command{
	Name:      "compute-win-count",
	Usage:     "Invoke wincount calculation against randomly generated input",
	ArgsUsage: "[minerQAPower networkQAPower]",
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return cli.ShowCommandHelp(cctx, cctx.Command.Name)
		}

		minerPow, err := big.FromString(cctx.Args().Get(0))
		if err != nil {
			return err
		}
		netPow, err := big.FromString(cctx.Args().Get(1))
		if err != nil {
			return err
		}

		// it doesn't matter how large / what this is
		// we are blake2b hashing it anyway
		randBuf := make([]byte, 64)
		rand.Read(randBuf)
		ep := &types.ElectionProof{VRFProof: randBuf}
		ep.ComputeWinCount(minerPow, netPow)

		return nil
	},
}
