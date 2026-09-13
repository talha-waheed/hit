package main

import (
	"testing"
	"time"
)

func TestIntervalNextPreservesFractionalMilliseconds(t *testing.T) {
	interval := Interval{}
	interval.Initialize(0.5, "none")

	if got, want := interval.Next(), 500*time.Microsecond; got != want {
		t.Fatalf("Interval.Next() = %s, want %s", got, want)
	}
}
