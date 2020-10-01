package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/stmgr"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/vm"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/lib/sqlblockstore"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/specs-actors/actors/runtime/proof"
	"github.com/ipfs/go-cid"

	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var resolvedActors map[address.Address]address.Address = make(map[address.Address]address.Address, 1024)

func resolveActor(sm *stmgr.StateManager, t *types.TipSet, a address.Address) address.Address {
	if _, known := resolvedActors[a]; !known {
		resolvedActors[a], _ = sm.ResolveToKeyAddress(context.Background(), a, t)
	}
	return resolvedActors[a]
}

const safetyLookBack = 5

var exportDealLifecyclesCmd = &cli.Command{
	Name:        "export-deal-lifecycles",
	Description: "Exports a list of deal termination events (requires node to be offline)",
	Flags:       []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		ctx := context.TODO()

		r, err := repo.NewFS(cctx.String("repo"))
		if err != nil {
			return xerrors.Errorf("opening fs repo: %w", err)
		}

		exists, err := r.Exists()
		if err != nil {
			return err
		}
		if !exists {
			return xerrors.Errorf("lotus repo doesn't exist")
		}

		lr, err := r.LockRO(repo.FullNode)
		if err != nil {
			return err
		}
		defer lr.Close() //nolint:errcheck

		mds, err := lr.Datastore("/metadata")
		if err != nil {
			return err
		}

		cbs, err := lr.ChainBlockstore()
		if err != nil {
			return xerrors.Errorf("failed to get chain blockstore: %w", err)
		}

		cs := store.NewChainStore(cbs, mds, vm.Syscalls(&fakeVerifier{}), nil)

		headKey, err := getMasterTsKey(safetyLookBack)
		if err != nil {
			return err
		}
		headTs, err := cs.LoadTipSet(*headKey)
		if err != nil {
			return xerrors.Errorf("failed to load our own chainhead: %w", err)
		}
		if err := cs.SetHead(headTs); err != nil {
			return xerrors.Errorf("failed to set our own chainhead: %w", err)
		}

		sm := stmgr.NewStateManager(cs)

		fmt.Println("DisappearEpoch,Miner,Client,DealId,PieceCID,PayloadCID,Size,aFil/GiB/Epoch,FirstSeenEpoch,SectorStartEpoch,DealStartEpoch,DealEndEpoch")

		type seenDeal struct {
			firstSeenEpoch   abi.ChainEpoch
			sectorStartEpoch abi.ChainEpoch
			proposal         *market.DealProposal
		}
		seenDeals := make(map[abi.DealID]*seenDeal, 1024*1024)

		// Genesis is at 1598306400
		// Start of SR2 is 1600884000
		// scanStartHeight := abi.ChainEpoch((1600884000 - 1598306400) / 30)

		curEpoch := headTs.Height() - 2

		// infloop with a sleep to get next block
		for {
			curEpoch++

			if curEpoch == headTs.Height() {
				log.Warn("   ...  Waiting for next tipset  ...")
				time.Sleep(35 * time.Second)

				headKey, err := getMasterTsKey(safetyLookBack)
				if err != nil {
					return err
				}
				headTs, err = cs.LoadTipSet(*headKey)
				if err != nil {
					return xerrors.Errorf("failed to load our own chainhead: %w", err)
				}
			}

			if curEpoch >= headTs.Height() {
				panic("Time did not advance???")
			}

			tipset, err := cs.GetTipsetByHeight(ctx, curEpoch, headTs, true)
			if err != nil {
				return xerrors.Errorf("Unable to get tipset at height %d: %w", curEpoch, err)
			}

			state, err := sm.GetMarketState(ctx, tipset)
			if err != nil {
				return xerrors.Errorf("Unable to read state at height %d: %w", curEpoch, err)
			}

			dealProposals, err := state.Proposals()
			if err != nil {
				return xerrors.Errorf("Unable to get deal proposals at height %d: %w", curEpoch, err)
			}

			dealStates, err := state.States()
			if err != nil {
				return xerrors.Errorf("Unable to get deal states at height %d: %w", curEpoch, err)
			}

			dealsCurrentRound := make(map[abi.DealID]*seenDeal, 1024)

			if err := dealProposals.ForEach(func(dID abi.DealID, d market.DealProposal) error {

				dealsCurrentRound[dID] = &seenDeal{proposal: &d}

				dState, found, err := dealStates.Get(dID)
				if err != nil {
					return xerrors.Errorf("failed to get state for deal %d at height %d in proposals array: %w", dID, curEpoch, err)
				} else if found && dState.SectorStartEpoch > 0 {
					dealsCurrentRound[dID].sectorStartEpoch = dState.SectorStartEpoch
				}
				return nil
			}); err != nil {
				return err
			}

			for dID, s := range seenDeals {
				if _, found := dealsCurrentRound[dID]; !found {

					var dataCidStr string
					if payloadCid, err := cid.Decode(s.proposal.Label); err == nil {
						dataCidStr = payloadCid.String()
					} else {
						dataCidStr = fmt.Sprintf("label '%x' is not a CID", s.proposal.Label)
					}

					fmt.Printf("%d,%s,%s,%d,%s,%s,%d,%s,%s,%s,%s,%s\n",
						curEpoch,
						s.proposal.Provider,
						resolveActor(sm, tipset, s.proposal.Client),
						dID,
						s.proposal.PieceCID,
						dataCidStr,
						s.proposal.PieceSize.Unpadded(),
						s.proposal.StoragePricePerEpoch,
						s.firstSeenEpoch,
						s.sectorStartEpoch,
						s.proposal.StartEpoch,
						s.proposal.EndEpoch,
					)
					delete(seenDeals, dID)
				}
			}

			for dID, d := range dealsCurrentRound {
				if _, seen := seenDeals[dID]; !seen {
					seenDeals[dID] = &seenDeal{firstSeenEpoch: curEpoch}
				}
				seenDeals[dID].proposal = d.proposal
				if d.sectorStartEpoch > 0 {
					seenDeals[dID].sectorStartEpoch = d.sectorStartEpoch
				}
			}
		}
	},
}

func getMasterTsKey(lookback int) (*types.TipSetKey, error) {

	var headCids string
	if err := sqlblockstore.DB().QueryRow(
		context.TODO(),
		"SELECT blockcids FROM heads WHERE height = -5 + ( SELECT MAX(height) FROM heads ) ORDER BY seq DESC LIMIT 1",
	).Scan(&headCids); err != nil {
		return nil, err
	}

	cidStrs := strings.Split(headCids, " ")
	cids := make([]cid.Cid, len(cidStrs))
	for _, cs := range cidStrs {
		c, err := cid.Parse(cs)
		if err != nil {
			return nil, err
		}
		cids = append(cids, c)
	}

	tk := types.NewTipSetKey(cids...)
	return &tk, nil
}

type fakeVerifier struct{}

var _ ffiwrapper.Verifier = (*fakeVerifier)(nil)

func (m fakeVerifier) VerifySeal(svi proof.SealVerifyInfo) (bool, error) {
	return true, nil
}

func (m fakeVerifier) VerifyWinningPoSt(ctx context.Context, info proof.WinningPoStVerifyInfo) (bool, error) {
	return true, nil
}

func (m fakeVerifier) VerifyWindowPoSt(ctx context.Context, info proof.WindowPoStVerifyInfo) (bool, error) {
	return true, nil
}

func (m fakeVerifier) GenerateWinningPoStSectorChallenge(ctx context.Context, proof abi.RegisteredPoStProof, id abi.ActorID, randomness abi.PoStRandomness, u uint64) ([]uint64, error) {
	panic("GenerateWinningPoStSectorChallenge not supported")
}
