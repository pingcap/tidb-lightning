package config

import (
	pb "gopkg.in/cheggaaa/pb.v1"
)

type MultiProgress interface {
	Reset(id int, label string, max int)
	Inc(id int)
	AcquireBars(count int, format string) int
	RecycleBars(count int)
	Close()
}

var progress MultiProgress = &nullProgress{}

type nullProgress struct{}

func (*nullProgress) Reset(int, string, int)      {}
func (*nullProgress) Inc(int)                     {}
func (*nullProgress) AcquireBars(int, string) int { return -1 }
func (*nullProgress) RecycleBars(int)             {}
func (*nullProgress) Close()                      {}

type resetMessage struct {
	id    int
	label string
	max   int
}
type incMessage struct {
	id int
}
type acquireMessage struct {
	count  int
	format string
	reply  chan<- int
}
type recycleMessage struct {
	count int
}
type closeMessage struct {
	reply chan<- struct{}
}

func loopMultiProgress(messages <-chan interface{}) {
	pool, _ := pb.StartPool()

	bars := make([]*pb.ProgressBar, 0)
	activeBarsCount := 0

	for {
		switch msg := (<-messages).(type) {
		case *resetMessage:
			bars[msg.id].Prefix(msg.label).SetTotal(msg.max).Set(0).Update()

		case *incMessage:
			bars[msg.id].Increment()

		case *acquireMessage:
			oldCount := activeBarsCount
			activeBarsCount += msg.count

			excessBarsCount := activeBarsCount - len(bars)
			if excessBarsCount > 0 {
				newBars := make([]*pb.ProgressBar, excessBarsCount)
				for i := range newBars {
					newBars[i] = pb.New(1)
				}
				bars = append(bars, newBars...)
				pool.Add(newBars...)
			}

			for i := oldCount; i < activeBarsCount; i++ {
				bars[i].Format(msg.format)
			}
			msg.reply <- oldCount

		case *recycleMessage:
			activeBarsCount -= msg.count

		case *closeMessage:
			pool.Stop()
			msg.reply <- struct{}{}
			return

		default:
			// unknown message, drop it.
		}
	}
}

type multiProgress struct {
	ch chan<- interface{}
}

func (p *multiProgress) Reset(id int, label string, max int) {
	p.ch <- &resetMessage{id: id, label: label, max: max}
}

func (p *multiProgress) Inc(id int) {
	p.ch <- &incMessage{id: id}
}

func (p *multiProgress) AcquireBars(count int, format string) int {
	reply := make(chan int)
	p.ch <- &acquireMessage{count: count, format: format, reply: reply}
	return <-reply
}

func (p *multiProgress) RecycleBars(count int) {
	p.ch <- &recycleMessage{count: count}
}

func (p *multiProgress) Close() {
	// Do not close the channel, sending to a closed channel will panic,
	// and we don't know who will still call `Inc()` after `Close()`.
	// https://go101.org/article/channel-closing.html
	reply := make(chan struct{})
	p.ch <- &closeMessage{reply: reply}
	<-reply
}

func InitProgress(cfg *Config) {
	if !cfg.Progress {
		progress = &nullProgress{}
		return
	}

	ch := make(chan interface{}, 16)
	progress = &multiProgress{ch: ch}
	go loopMultiProgress(ch)
}

func Progress() MultiProgress {
	return progress
}
