package common

import (
	"context"
	"sync"
)

type Runnable interface {
	Run(ctx context.Context, wg *sync.WaitGroup)
}
