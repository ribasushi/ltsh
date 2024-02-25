package kit

import (
	"testing"

	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"

	"github.com/filecoin-project/go-state-types/abi"
)

type DealHarness struct {
	t      *testing.T
	client *TestFullNode
	main   *TestMiner
	market *TestMiner
}

type MakeFullDealParams struct {
	Rseed                    int
	FastRet                  bool
	StartEpoch               abi.ChainEpoch
	UseCARFileForStorageDeal bool

	// SuspendUntilCryptoeconStable suspends deal-making, until cryptoecon
	// parameters are stabilised. This affects projected collateral, and tests
	// will fail in network version 13 and higher if deals are started too soon
	// after network birth.
	//
	// The reason is that the formula for collateral calculation takes
	// circulating supply into account:
	//
	//   [portion of power this deal will be] * [~1% of tokens].
	//
	// In the first epochs after genesis, the total circulating supply is
	// changing dramatically in percentual terms. Therefore, if the deal is
	// proposed too soon, by the time it gets published on chain, the quoted
	// provider collateral will no longer be valid.
	//
	// The observation is that deals fail with:
	//
	//   GasEstimateMessageGas error: estimating gas used: message execution
	//   failed: exit 16, reason: Provider collateral out of bounds. (RetCode=16)
	//
	// Enabling this will suspend deal-making until the network has reached a
	// height of 300.
	SuspendUntilCryptoeconStable bool
}

// NewDealHarness creates a test harness that contains testing utilities for deals.
func NewDealHarness(t *testing.T, client *TestFullNode, main *TestMiner, market *TestMiner) *DealHarness {
	return &DealHarness{
		t:      t,
		client: client,
		main:   main,
		market: market,
	}
}
