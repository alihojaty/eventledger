package main

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestComputeExpectedBalances_MatchesIterativeSum(t *testing.T) {
	const sampleN = 1000 // Covers multiple 420-cycles plus a remainder of 160

	iterative := make(map[int]map[string]decimal.Decimal)
	for u := 1; u <= numUsers; u++ {
		iterative[u] = map[string]decimal.Decimal{
			"USDT": decimal.Zero,
			"BTC":  decimal.Zero,
			"IRT":  decimal.Zero,
		}
	}

	for seq := int64(1); seq <= sampleN; seq++ {
		u, cur, _, amt := DeterministicDeposit(seq)
		iterative[u][cur] = iterative[u][cur].Add(amt)
	}

	fast := ComputeExpectedBalances(sampleN)

	for u := 1; u <= numUsers; u++ {
		for _, cur := range currencies {
			got := fast[u][cur]
			want := iterative[u][cur]
			if !got.Equal(want) {
				t.Fatalf("user %d currency %s: fast=%s != iterative=%s", u, cur, got.String(), want.String())
			}
		}
	}
}

func TestComputeExpectedBalances_5Million_InstantExecution(t *testing.T) {
	const fiveMillion = 5_000_000
	expected := ComputeExpectedBalances(fiveMillion)

	if len(expected) != 20 {
		t.Fatalf("expected 20 users, got %d", len(expected))
	}

	// Verify every user has positive deterministic balances across all 3 currencies
	for u := 1; u <= 20; u++ {
		for _, cur := range currencies {
			if !expected[u][cur].IsPositive() {
				t.Fatalf("expected positive balance for user %d currency %s, got %s", u, cur, expected[u][cur])
			}
		}
	}
}
