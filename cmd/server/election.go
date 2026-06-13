package server

import (
	"time"
)

type Election struct {
	ElectionInterval  int
	ElectionDuration  time.Duration
	ElectionTicker    *time.Ticker
	ElectionResetChan chan bool
}

func NewElection(electionInterval int) *Election {
	return &Election{
		ElectionInterval:  electionInterval,
		ElectionDuration:  time.Second * time.Duration(electionInterval),
		ElectionTicker:    time.NewTicker(time.Second * time.Duration(electionInterval)),
		ElectionResetChan: make(chan bool),
	}
}
