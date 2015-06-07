package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/context"
)

const ramSpecDefaultInstance = "default"
const ramSpecDefaultLimit = 1024 // 1GiB

var ramSpecRE = regexp.MustCompile(`^ram:([^,:\[\]]*)(?:,([^,:\[\]]*))?$`)

type ramSpec struct {
	Name  string
	Limit int64
}

type ramCAS struct {
	Instance *ramInstance
	Limit    int64
	Name     string
	Mode     Mode
}

// InProcessRAM returns a Spec for a non-persistent CAS in Go's heap.
//
// Distinct names map to distinct CAS backends, with the exception that the
// empty string is mapped to the CAS backend with name "default".
func InProcessRAM(name string, limit int64) Spec {
	if name == "" {
		name = ramSpecDefaultInstance
	}
	if limit < 0 {
		limit = ramSpecDefaultLimit
	}
	return ramSpec{name, limit}
}

func parseInProcessRAMSpec(input string) (Spec, error) {
	if !strings.HasPrefix(input, "ram:") {
		return nil, errNoMatch
	}
	match := ramSpecRE.FindStringSubmatch(input)
	if match == nil {
		return nil, SpecParseError{
			Input:   input,
			Problem: fmt.Errorf("does not match %s", ramSpecRE),
		}
	}
	name := match[1]
	if name == "" {
		name = ramSpecDefaultInstance
	}
	limit, err := strconv.ParseInt(match[2], 0, 64)
	if err != nil {
		if match[2] != "" && match[2] != "auto" {
			return nil, SpecParseError{Input: input, Problem: err}
		}
		limit = ramSpecDefaultLimit
	}
	return InProcessRAM(name, limit), nil
}

func (spec ramSpec) Open(mode Mode) (CAS, error) {
	return &ramCAS{
		Instance: getRamInstance(spec.Name, mode),
		Limit:    spec.Limit,
		Name:     spec.Name,
		Mode:     mode,
	}, nil
}

func (spec ramSpec) String() string {
	return fmt.Sprintf("ram:%s,%d", spec.Name, spec.Limit)
}

func (cas *ramCAS) Spec() Spec {
	return InProcessRAM(cas.Name, cas.Limit)
}

func (cas *ramCAS) Get(_ context.Context, addr Addr) ([]byte, error) {
	if cas.Instance != nil {
		cas.Instance.Mutex.RLock()
		block, found := cas.Instance.Blocks[addr]
		cas.Instance.Mutex.RUnlock()
		if found {
			return block, nil
		}
	}
	return nil, BlockNotFoundError{Addr: addr}
}

func (cas *ramCAS) Put(_ context.Context, raw []byte) (Addr, error) {
	cas.Mode.MustWrite()
	block := PadBlock(raw)
	addr := Hash(block)

	cas.Instance.Mutex.Lock()
	defer cas.Instance.Mutex.Unlock()

	// Duplicate data?
	if _, found := cas.Instance.Blocks[addr]; found {
		return addr, nil
	}

	// Over the limit?
	if cas.Instance.Used >= cas.Limit {
		return Addr{}, NoSpaceError{"ram"}
	}

	// Store the block.
	cas.Instance.Used += 1
	cas.Instance.Blocks[addr] = block
	return addr, nil
}

func (cas *ramCAS) Release(_ context.Context, addr Addr, _ bool) error {
	cas.Mode.MustWrite()
	cas.Instance.Mutex.Lock()
	defer cas.Instance.Mutex.Unlock()
	if _, found := cas.Instance.Blocks[addr]; found {
		delete(cas.Instance.Blocks, addr)
		cas.Instance.Used -= 1
	}
	return nil
}

func (cas *ramCAS) Walk(ctx context.Context, _ bool) <-chan Walk {
	addrs, blocks := cas.Instance.snapshot()
	send := make(chan Walk)
	go func() {
	Loop:
		for i, addr := range addrs {
			select {
			case <-ctx.Done():
				send <- Walk{
					IsValid: true,
					Err:     ctx.Err(),
				}
				break Loop
			default:
				send <- Walk{
					IsValid: true,
					Addr:    addr,
					Block:   blocks[i],
				}
			}
		}
		close(send)
	}()
	return send
}

func (cas *ramCAS) Stat(_ context.Context) (Stat, error) {
	var used int64
	if cas.Instance != nil {
		cas.Instance.Mutex.RLock()
		defer cas.Instance.Mutex.RUnlock()
		used = cas.Instance.Used
	}
	return Stat{Used: used, Limit: cas.Limit}, nil
}

func (cas *ramCAS) Close() {}

func getRamInstance(name string, mode Mode) *ramInstance {
	ramInstanceMutex.Lock()
	defer ramInstanceMutex.Unlock()
	instance, found := ramInstances[name]
	if !found && mode.CanWrite() {
		instance = &ramInstance{Blocks: make(map[Addr][]byte)}
		ramInstances[name] = instance
	}
	return instance
}

type ramInstance struct {
	Mutex  sync.RWMutex
	Blocks map[Addr][]byte
	Used   int64
}

func (orig *ramInstance) snapshot() ([]Addr, [][]byte) {
	if orig == nil {
		return nil, nil
	}
	orig.Mutex.RLock()
	defer orig.Mutex.RUnlock()
	addrs := make([]Addr, 0, len(orig.Blocks))
	blocks := make([][]byte, 0, len(orig.Blocks))
	for addr, block := range orig.Blocks {
		addrs = append(addrs, addr)
		blocks = append(blocks, block)
	}
	return addrs, blocks
}

var ramInstanceMutex sync.Mutex
var ramInstances = make(map[string]*ramInstance)
