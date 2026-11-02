package rfsnotify

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// serialBackend mimics fsnotify Windows backend: Add and Remove are served by the goroutine delivering events,
// which serves no request while it is blocked delivering one.
type serialBackend struct {
	events   chan fsnotify.Event
	errors   chan error
	requests chan request
	toSend   chan fsnotify.Event
	done     chan struct{}

	mu      sync.Mutex
	watches map[string]struct{}
}

type request struct {
	path   string
	remove bool
	reply  chan error
}

func newSerialBackend() *serialBackend {
	b := &serialBackend{
		events:   make(chan fsnotify.Event),
		errors:   make(chan error),
		requests: make(chan request),
		toSend:   make(chan fsnotify.Event, 100),
		done:     make(chan struct{}),
		watches:  make(map[string]struct{}),
	}
	go b.run()
	return b
}

func (b *serialBackend) run() {
	defer close(b.events)
	defer close(b.errors)
	for {
		select {
		case <-b.done:
			return
		case r := <-b.requests:
			r.reply <- b.apply(r)
		case e := <-b.toSend:
			select {
			case <-b.done:
				return
			case b.events <- e:
			}
		}
	}
}

func (b *serialBackend) apply(r request) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.remove {
		if _, ok := b.watches[r.path]; !ok {
			return fmt.Errorf("%w: %s", fsnotify.ErrNonExistentWatch, r.path)
		}
		delete(b.watches, r.path)
		return nil
	}
	if _, err := os.Stat(r.path); err != nil {
		return err
	}
	b.watches[r.path] = struct{}{}
	return nil
}

func (b *serialBackend) call(path string, remove bool) error {
	r := request{path: path, remove: remove, reply: make(chan error)}
	b.requests <- r
	return <-r.reply
}

func (b *serialBackend) Add(name string) error    { return b.call(name, false) }
func (b *serialBackend) Remove(name string) error { return b.call(name, true) }
func (b *serialBackend) Close() error             { close(b.done); return nil }

func (b *serialBackend) WatchList() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	list := make([]string, 0, len(b.watches))
	for w := range b.watches {
		list = append(list, w)
	}
	return list
}

func (b *serialBackend) watched(path string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.watches[path]
	return ok
}

func TestRWatcher_noDeadlockOnSerialBackend(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "a", "b"), 0o750); err != nil {
		t.Fatal(err)
	}
	b := newSerialBackend()
	watcher := newWatcher(b, b.events, b.errors)
	defer func() {
		if err := watcher.Close(); err != nil {
			t.Errorf("could not close watcher: %s", err)
		}
	}()
	if err := watcher.AddRecursive(root); err != nil {
		t.Fatalf("could not add recursive folder: %s", err)
	}

	newDir := filepath.Join(root, "new")
	if err := os.MkdirAll(filepath.Join(newDir, "sub"), 0o750); err != nil {
		t.Fatal(err)
	}
	// a removal followed by other events, delivered in a row: the removal's watch update
	// must not wait on the backend while it is delivering the next events
	want := []fsnotify.Event{
		{Name: filepath.Join(root, "a", "b"), Op: fsnotify.Remove},
		{Name: newDir, Op: fsnotify.Create},
		{Name: filepath.Join(root, "file"), Op: fsnotify.Write},
	}
	for _, e := range want {
		b.toSend <- e
	}

	var got []fsnotify.Event
	timeout := time.After(5 * time.Second)
	for len(got) < len(want) {
		select {
		case e := <-watcher.Events:
			got = append(got, e)
		case err := <-watcher.Errors:
			t.Fatalf("unexpected watcher error: %s", err)
		case <-timeout:
			t.Fatalf("deadlock: received %d events out of %d", len(got), len(want))
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("got events %v, want %v", got, want)
	}

	deadline := time.Now().Add(5 * time.Second)
	for !b.watched(filepath.Join(newDir, "sub")) || b.watched(filepath.Join(root, "a", "b")) {
		if time.Now().After(deadline) {
			t.Fatalf("watches not updated, got %v", b.WatchList())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRWatcher_closeWithoutReader(t *testing.T) {
	b := newSerialBackend()
	watcher := newWatcher(b, b.events, b.errors)
	// nobody reads watcher.Events
	b.toSend <- fsnotify.Event{Name: "x", Op: fsnotify.Write}
	b.toSend <- fsnotify.Event{Name: "y", Op: fsnotify.Write}

	closed := make(chan error)
	go func() { closed <- watcher.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("could not close watcher: %s", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked")
	}
	if _, ok := <-watcher.Events; ok {
		t.Error("Events not closed")
	}
	if err := watcher.AddRecursive(t.TempDir()); !errors.Is(err, ErrClosed) {
		t.Errorf("AddRecursive after Close returned %v, want %v", err, ErrClosed)
	}
}

func TestRWatcher_RemoveRecursive(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "a", "b"), 0o750); err != nil {
		t.Fatal(err)
	}
	sibling := root + "-sibling" // sharing root as prefix, removed with t.TempDir parent
	if err := os.Mkdir(sibling, 0o750); err != nil {
		t.Fatal(err)
	}

	b := newSerialBackend()
	watcher := newWatcher(b, b.events, b.errors)
	defer watcher.Close() //nolint:errcheck // nothing to check
	if err := watcher.AddRecursive(root); err != nil {
		t.Fatal(err)
	}
	if err := watcher.Add(sibling); err != nil {
		t.Fatal(err)
	}
	// works for deleted folders
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}

	if err := watcher.RemoveRecursive(root); err != nil {
		t.Fatalf("could not remove recursive folder: %s", err)
	}
	if got := b.WatchList(); !slices.Equal(got, []string{sibling}) {
		t.Errorf("got watches %v, want only %s", got, sibling)
	}
	if err := watcher.RemoveRecursive(root); !errors.Is(err, fsnotify.ErrNonExistentWatch) {
		t.Errorf("RemoveRecursive of unwatched folder returned %v, want %v", err, fsnotify.ErrNonExistentWatch)
	}
}
