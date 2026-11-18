// Package app runs everything in one process: the networks' bridges, the store, and the client API on top.
package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/whyyor/tuimessenger/internal/bridges"
	"github.com/whyyor/tuimessenger/internal/config"
	"github.com/whyyor/tuimessenger/internal/csapi"
	"github.com/whyyor/tuimessenger/internal/matrixconn"
	"github.com/whyyor/tuimessenger/internal/media"
	"github.com/whyyor/tuimessenger/internal/store"
	"github.com/whyyor/tuimessenger/internal/typing"
	"golang.org/x/sync/singleflight"
)

type Options struct {
	Log        zerolog.Logger
	Config     *config.Config
	DataDir    string
	Networks   []string
	ServerName string
	Owner      id.UserID
}

type App struct {
	opts  Options
	Store *store.Store
	API   *csapi.Server

	// bridgesLock guards bridges, dbs and afterStart: a network can be turned on while everything else runs.
	bridgesLock sync.RWMutex
	bridges     []*bridgev2.Bridge
	dbs         []*dbutil.Database
	// afterStart holds each network's optional background step, run once its bridge has started.
	afterStart map[*bridgev2.Bridge]func(context.Context, *bridgev2.Bridge, *bridgev2.User)
	enableLock sync.Mutex // one network turned on at a time

	media  *media.Store // shared by every network, kept for networks turned on later
	typing *typing.Tracker

	mediaCache *media.Cache
	downloads  singleflight.Group // one download per file, even if it's opened twice at once

	roomLock    sync.Mutex
	roomBridges map[id.RoomID]*bridgev2.Bridge // which network each room belongs to, learnt on first use

	historyLock sync.Mutex // one older-history fetch at a time is plenty, and gentle on the networks

	reconnectLock sync.Mutex
	lastReconnect time.Time // when a client last made us reconnect a network

	settings     map[string]map[string]*bridges.Setting // network name → setting key → setting
	settingsLock sync.Mutex                             // one settings change at a time

	logins     map[string]*loginSession // logins in progress, by ID
	loginsLock sync.Mutex
}

var _ csapi.Bridges = (*App)(nil)

func New(ctx context.Context, opts Options) (*App, error) {
	if len(opts.Networks) == 0 {
		return nil, errors.New("no networks chosen")
	}
	if err := os.MkdirAll(opts.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create data folder: %w", err)
	}
	a := &App{
		opts:        opts,
		roomBridges: make(map[id.RoomID]*bridgev2.Bridge),
		afterStart:  make(map[*bridgev2.Bridge]func(context.Context, *bridgev2.Bridge, *bridgev2.User)),
	}
	var err error
	if a.Store, err = store.Open(ctx, filepath.Join(opts.DataDir, "store.db"), opts.Log); err != nil {
		return nil, err
	}
	med, err := media.New(filepath.Join(opts.DataDir, "media"))
	if err != nil {
		a.Close()
		return nil, err
	}
	if a.mediaCache, err = media.NewCache(filepath.Join(opts.DataDir, "media", "cache"), mediaCacheSize); err != nil {
		a.Close()
		return nil, err
	}
	typ := typing.New()
	a.media, a.typing = med, typ
	for _, name := range opts.Networks {
		if _, err = a.addBridge(name); err != nil {
			a.Close()
			return nil, fmt.Errorf("%s: %w", name, err)
		}
	}
	a.API = csapi.New(csapi.Options{
		Log: opts.Log.With().Str("component", "api").Logger(), Store: a.Store, Media: med, Typing: typ,
		Bridges: a, ServerName: opts.ServerName, Owner: opts.Owner,
	})
	return a, nil
}

// addBridge creates a network's bridge (not started yet) and adds it to the running ones.
func (a *App) addBridge(name string) (*bridgev2.Bridge, error) {
	network, err := bridges.New(name, a.opts.Config)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(a.opts.DataDir, name)
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	log := a.opts.Log.With().Str("network", name).Logger()
	db, err := openBridgeDB(filepath.Join(dir, "bridge.db"), log)
	if err != nil {
		return nil, err
	}
	conn := matrixconn.New(log, a.Store, a.media, a.typing, name, a.opts.ServerName, a.opts.Owner)
	conn.FetchDirect = func(ctx context.Context, fileID string) (*os.File, error) {
		file, _, err := a.FetchMedia(ctx, fileID, nil)
		return file, err
	}
	br := bridgev2.NewBridge(networkid.BridgeID(name), db, log, &network.Config, conn, network.Connector, commands.NewProcessor)
	// Direct media: files stay on the network until opened. This must come after NewBridge (which sets the
	// network up) and before Start.
	if dmn, ok := network.Connector.(bridgev2.DirectMediableNetwork); ok {
		dmn.SetUseDirectMedia()
	}
	a.bridgesLock.Lock()
	a.dbs = append(a.dbs, db)
	a.bridges = append(a.bridges, br)
	if network.AfterStart != nil {
		a.afterStart[br] = network.AfterStart
	}
	a.bridgesLock.Unlock()
	if len(network.Settings) > 0 {
		a.settingsLock.Lock()
		if a.settings == nil {
			a.settings = make(map[string]map[string]*bridges.Setting)
		}
		a.settings[name] = network.Settings
		a.settingsLock.Unlock()
	}
	return br, nil
}

// removeBridge undoes addBridge for a network that failed to start.
func (a *App) removeBridge(br *bridgev2.Bridge) {
	a.bridgesLock.Lock()
	if i := slices.Index(a.bridges, br); i >= 0 {
		a.bridges = slices.Delete(a.bridges, i, i+1)
		if db := a.dbs[i]; db != nil {
			_ = db.Close()
		}
		a.dbs = slices.Delete(a.dbs, i, i+1)
	}
	delete(a.afterStart, br)
	a.bridgesLock.Unlock()
	a.settingsLock.Lock()
	delete(a.settings, string(br.ID))
	a.settingsLock.Unlock()
}

// allBridges is a snapshot of the running networks, safe to loop over while one is being turned on.
func (a *App) allBridges() []*bridgev2.Bridge {
	a.bridgesLock.RLock()
	defer a.bridgesLock.RUnlock()
	return slices.Clone(a.bridges)
}

func openBridgeDB(path string, log zerolog.Logger) (*dbutil.Database, error) {
	db, err := dbutil.NewFromConfig("tuimessenger", dbutil.Config{
		PoolConfig: dbutil.PoolConfig{
			// FULL sync: this database holds the network's encryption keys, which must never be lost.
			Type:         "sqlite3-fk-wal-fullsync",
			URI:          "file:" + path + "?_txlock=immediate",
			MaxOpenConns: 5,
			MaxIdleConns: 1,
		},
	}, dbutil.ZeroLogger(log.With().Str("component", "db").Logger()))
	if err != nil {
		return nil, fmt.Errorf("failed to open bridge database: %w", err)
	}
	return db, nil
}

func (a *App) Start(ctx context.Context) error {
	for _, br := range a.allBridges() {
		if err := a.startBridge(ctx, br); err != nil {
			return err
		}
	}
	return nil
}

// startBridge starts one network and its optional background step.
func (a *App) startBridge(ctx context.Context, br *bridgev2.Bridge) error {
	if err := br.Start(ctx); err != nil {
		return fmt.Errorf("failed to start %s: %w", br.ID, err)
	}
	a.bridgesLock.RLock()
	hook := a.afterStart[br]
	a.bridgesLock.RUnlock()
	if hook != nil {
		if owner, err := br.GetUserByMXID(ctx, a.opts.Owner); err == nil {
			go hook(br.BackgroundCtx, br, owner)
		}
	}
	return nil
}

// Stop stops the bridges and closes the databases.
func (a *App) Stop() {
	for _, br := range a.allBridges() {
		br.StopWithTimeout(5 * time.Second)
	}
	a.Close()
}

func (a *App) Close() {
	a.bridgesLock.RLock()
	dbs := slices.Clone(a.dbs)
	a.bridgesLock.RUnlock()
	for _, db := range dbs {
		_ = db.Close()
	}
	if a.Store != nil {
		_ = a.Store.Close()
	}
}

// Bridge returns the bridge for a network, e.g. to log in to it.
func (a *App) Bridge(name string) *bridgev2.Bridge {
	for _, br := range a.allBridges() {
		if string(br.ID) == name {
			return br
		}
	}
	return nil
}

// Queue hands an event from the client to the bridge whose room it is.
func (a *App) Queue(ctx context.Context, evt *event.Event) error {
	br, _, err := a.portalFor(ctx, evt.RoomID)
	if err != nil {
		return err
	}
	res := br.QueueMatrixEvent(ctx, evt)
	if res.Error != nil {
		return res.Error
	} else if !res.Success && !res.Queued && !res.Ignored {
		return errors.New("the bridge couldn't handle the event")
	}
	return nil
}

// LoadOlder asks the room's network for one more page of older messages, like scrolling up in its own app.
func (a *App) LoadOlder(ctx context.Context, roomID id.RoomID) (bool, error) {
	br, portal, err := a.portalFor(ctx, roomID)
	if err != nil {
		return false, err
	}
	a.historyLock.Lock()
	defer a.historyLock.Unlock()
	task, err := br.DB.BackfillTask.GetNextForPortal(ctx, portal.PortalKey, false)
	if err != nil || task == nil {
		return false, err
	}
	br.DoBackfillTask(ctx, task)
	task, err = br.DB.BackfillTask.GetNextForPortal(ctx, portal.PortalKey, false)
	return task != nil, err
}

func (a *App) portalFor(ctx context.Context, roomID id.RoomID) (*bridgev2.Bridge, *bridgev2.Portal, error) {
	a.roomLock.Lock()
	known := a.roomBridges[roomID]
	a.roomLock.Unlock()
	candidates := a.allBridges()
	if known != nil {
		candidates = []*bridgev2.Bridge{known}
	}
	for _, br := range candidates {
		portal, err := br.GetPortalByMXID(ctx, roomID)
		if err != nil {
			return nil, nil, err
		} else if portal != nil {
			a.roomLock.Lock()
			a.roomBridges[roomID] = br
			a.roomLock.Unlock()
			return br, portal, nil
		}
	}
	return nil, nil, fmt.Errorf("room %s doesn't belong to any network", roomID)
}

// SetViewing tells every logged-in account which chat is open: the one it belongs to gets the chat, the others nil.
func (a *App) SetViewing(ctx context.Context, roomID id.RoomID) {
	var viewedBridge *bridgev2.Bridge
	var viewedPortal *bridgev2.Portal
	if roomID != "" {
		var err error
		if viewedBridge, viewedPortal, err = a.portalFor(ctx, roomID); err != nil {
			zerolog.Ctx(ctx).Debug().Err(err).Msg("Viewing a room that isn't a chat")
		}
	}
	for _, br := range a.allBridges() {
		user, err := br.GetUserByMXID(ctx, a.opts.Owner)
		if err != nil {
			continue
		}
		msg := &bridgev2.MatrixViewingChat{}
		if br == viewedBridge {
			msg.Portal = viewedPortal
		}
		for _, login := range user.GetUserLogins() {
			if viewer, ok := login.Client.(bridgev2.ChatViewingNetworkAPI); ok {
				if err = viewer.HandleMatrixViewingChat(ctx, msg); err != nil {
					zerolog.Ctx(ctx).Warn().Err(err).Str("network", string(br.ID)).Msg("Failed to update viewed chat")
				}
			}
		}
	}
}
