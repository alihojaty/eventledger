package worker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

type WorkerStopper interface {
	StopWorker(workerID int) error
	WorkerCount() int
}

type TerminalController struct {
	stopper WorkerStopper
	in      io.Reader
	out     io.Writer
	logger  *zap.Logger
}

func NewTerminalController(stopper WorkerStopper, in io.Reader, out io.Writer, logger *zap.Logger) *TerminalController {
	return &TerminalController{
		stopper: stopper,
		in:      in,
		out:     out,
		logger:  logger,
	}
}

func (tc *TerminalController) Start(ctx context.Context) {
	linesCh := make(chan string)

	go func() {
		defer close(linesCh)
		scanner := bufio.NewScanner(tc.in)
		for scanner.Scan() {
			text := scanner.Text()
			select {
			case linesCh <- text:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-linesCh:
			if !ok {
				return
			}
			tc.processInputLine(line)
		}
	}
}

func (tc *TerminalController) processInputLine(line string) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return
	}

	workerID, err := strconv.Atoi(trimmed)
	if err != nil {
		_, _ = fmt.Fprintf(tc.out, "Invalid input %q: please enter a worker number between 1 and %d.\n",
			trimmed, tc.stopper.WorkerCount())
		return
	}

	if workerID < 1 || workerID > tc.stopper.WorkerCount() {
		_, _ = fmt.Fprintf(tc.out, "Invalid worker number %d: valid range is 1 to %d.\n",
			workerID, tc.stopper.WorkerCount())
		return
	}

	err = tc.stopper.StopWorker(workerID)
	if err != nil {
		if errors.Is(err, ErrWorkerAlreadyStopped) {
			_, _ = fmt.Fprintf(tc.out, "Worker %d is already stopped.\n", workerID)
			return
		}
		_, _ = fmt.Fprintf(tc.out, "Failed to stop worker %d: %v\n", workerID, err)
		return
	}

	_, _ = fmt.Fprintf(tc.out, "Worker %d stopped (drained).\n", workerID)
}
