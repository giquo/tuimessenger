package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/id"

	"github.com/whyyor/tuimessenger/internal/config"
	"github.com/whyyor/tuimessenger/internal/csapi"
)

// A network turned on while the app runs starts right away, shows as running, and is saved for the next start.
// Run with -race: the network list is read while it changes.
func TestEnableNetwork(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	a, err := New(ctx, Options{Log: zerolog.Nop(), Config: &config.Config{}, DataDir: dir, Networks: []string{"slack"},
		ServerName: "tuimessenger.local", Owner: id.UserID("@you:tuimessenger.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Stop)

	setup := func(name string) (running bool, setup string, found bool) {
		for _, n := range a.Networks(ctx) {
			if n.Name == name {
				return n.Running, n.Setup, true
			}
		}
		return false, "", false
	}
	if running, s, _ := setup("instagram"); running || s != csapi.SetupTurnOn {
		t.Errorf("instagram before: running=%v setup=%q, want it offered to turn on", running, s)
	}
	if running, _, _ := setup("slack"); !running {
		t.Error("slack should be running")
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = a.Networks(ctx)
				_ = a.Bridge("slack")
				a.SetViewing(ctx, "")
			}
		}
	}()
	for _, name := range []string{"instagram", "whatsapp"} {
		if err = a.EnableNetwork(ctx, name); err != nil {
			break
		}
	}
	close(stop)
	readers.Wait()
	if err != nil {
		t.Fatal(err)
	}

	if running, s, _ := setup("instagram"); !running || s != "" {
		t.Errorf("instagram after: running=%v setup=%q", running, s)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "config.yaml"))
	if !strings.Contains(string(data), "networks: [whatsapp, telegram, slack, instagram]") {
		t.Errorf("config.yaml should list the defaults:\n%s", data)
	}
	if err = a.EnableNetwork(ctx, "instagram"); err != nil {
		t.Errorf("turning on a running network should do nothing: %v", err)
	}
	if err = a.EnableNetwork(ctx, "nope"); !errors.Is(err, csapi.ErrUnknownNetwork) {
		t.Errorf("unknown network: %v", err)
	}
	if _, s, found := setup("telegram"); !found || s != csapi.SetupTelegramKeys {
		t.Errorf("telegram without keys should ask for them: found=%v setup=%q", found, s)
	}
}
