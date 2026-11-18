package csapi_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
	"maunium.net/go/mautrix/pushrules"

	"io"

	"github.com/whyyor/tuimessenger/internal/csapi"
	"github.com/whyyor/tuimessenger/internal/matrixconn"
	"github.com/whyyor/tuimessenger/internal/media"
	"github.com/whyyor/tuimessenger/internal/store"
	"github.com/whyyor/tuimessenger/internal/typing"
)

const (
	server   = "test.localhost"
	owner    = id.UserID("@me:" + server)
	password = "correct horse battery"
)

// fakeBridges records what the API would hand to the networks.
type fakeBridges struct {
	lock          sync.Mutex
	queued        []*event.Event
	older         func(roomID id.RoomID) (bool, error)
	refuse        error // when set, the "network" rejects everything with this error
	refused       []*event.Event
	slowFirstView bool // the first SetViewing takes a while, as a slow network might
	viewing       []id.RoomID
	remote        map[string]string // direct media "still on the network", by file ID
	fetches       int
	broken        []string // networks ReconnectUnhealthy reports as reconnecting
	slackChannels bool
	loginInput    map[string]string
	cancelled     []string
	loggedOut     []string
	telegramKeys  string
	enabled       []string
}

func (f *fakeBridges) Queue(ctx context.Context, evt *event.Event) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.refuse != nil {
		f.refused = append(f.refused, evt)
		return f.refuse
	}
	f.queued = append(f.queued, evt)
	return nil
}

func (f *fakeBridges) Networks(ctx context.Context) []csapi.Network {
	f.lock.Lock()
	defer f.lock.Unlock()
	return []csapi.Network{{Name: "slack", State: "CONNECTED", Settings: map[string]bool{"include_channels": f.slackChannels}}}
}

func (f *fakeBridges) SetNetworkSetting(ctx context.Context, network, key string, value bool) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if network != "slack" || key != "include_channels" {
		return csapi.ErrUnknownSetting
	}
	f.slackChannels = value
	return nil
}

// The fake login: a password form, then done. It records what reached the "network".
func (f *fakeBridges) StartLogin(ctx context.Context, network, flowID string) (*csapi.LoginStep, error) {
	if network != "slack" {
		return nil, mautrix.MNotFound.WithMessage("No such network is running")
	}
	return &csapi.LoginStep{LoginID: "L1", Step: &bridgev2.LoginStep{Type: bridgev2.LoginStepTypeUserInput, StepID: "pw",
		UserInputParams: &bridgev2.LoginUserInputParams{Fields: []bridgev2.LoginInputDataField{{ID: "password", Name: "Password"}}}}}, nil
}

func (f *fakeBridges) SubmitLogin(ctx context.Context, loginID, stepID string, stepType bridgev2.LoginStepType, input map[string]string) (*csapi.LoginStep, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.loginInput = input
	if loginID != "L1" || stepID != "pw" || stepType != bridgev2.LoginStepTypeUserInput {
		return nil, mautrix.MNotFound.WithMessage("This login has ended; start it again")
	}
	return &csapi.LoginStep{LoginID: "L1", Step: &bridgev2.LoginStep{Type: bridgev2.LoginStepTypeComplete, StepID: "done",
		CompleteParams: &bridgev2.LoginCompleteParams{UserLoginID: "T1-U1"}}}, nil
}

func (f *fakeBridges) CancelLogin(ctx context.Context, loginID string) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.cancelled = append(f.cancelled, loginID)
	return nil
}

func (f *fakeBridges) Logout(ctx context.Context, network string, loginID networkid.UserLoginID) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.loggedOut = append(f.loggedOut, network+"/"+string(loginID))
	return nil
}

func (f *fakeBridges) EnableNetwork(_ context.Context, network string) error {
	if network != "mattermost" {
		return fmt.Errorf("%w: %s", csapi.ErrUnknownNetwork, network)
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.enabled = append(f.enabled, network)
	return nil
}

func (f *fakeBridges) SetTelegramKeys(ctx context.Context, apiID int, apiHash string) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.telegramKeys = fmt.Sprintf("%d:%s", apiID, apiHash)
	return nil
}

func (f *fakeBridges) ReconnectUnhealthy(ctx context.Context) []string {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.broken
}

func (f *fakeBridges) SetViewing(ctx context.Context, roomID id.RoomID) {
	f.lock.Lock()
	slow := len(f.viewing) == 0 && f.slowFirstView
	f.lock.Unlock()
	if slow {
		time.Sleep(50 * time.Millisecond) // a network taking its time with the first change
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.viewing = append(f.viewing, roomID)
}

func (f *fakeBridges) viewed() []id.RoomID {
	f.lock.Lock()
	defer f.lock.Unlock()
	return append([]id.RoomID(nil), f.viewing...)
}

// FetchMedia serves "network" files from f.remote, like a bridge downloading direct media.
func (f *fakeBridges) FetchMedia(ctx context.Context, fileID string, params map[string]string) (*os.File, string, error) {
	f.lock.Lock()
	data, ok := f.remote[fileID]
	f.fetches++
	f.lock.Unlock()
	if !ok {
		return nil, "", mautrix.MNotFound.WithMessage("Unknown media")
	}
	tmp, err := os.CreateTemp("", "fake-media-*")
	if err != nil {
		return nil, "", err
	}
	_ = os.Remove(tmp.Name()) // stays readable while open
	_, _ = tmp.WriteString(data)
	_, _ = tmp.Seek(0, 0)
	return tmp, "image/jpeg", nil
}

func (f *fakeBridges) LoadOlder(ctx context.Context, roomID id.RoomID) (bool, error) {
	if f.older == nil {
		return false, nil
	}
	return f.older(roomID)
}

// waitQueued waits until an event of the given type reached the fake bridge, and returns it.
func (f *fakeBridges) waitQueued(t *testing.T, evtType event.Type) *event.Event {
	t.Helper()
	for range 100 {
		f.lock.Lock()
		for _, evt := range f.queued {
			if evt.Type == evtType {
				f.lock.Unlock()
				return evt
			}
		}
		f.lock.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no %s event reached the bridge", evtType.Type)
	return nil
}

type env struct {
	cli     *mautrix.Client
	conn    *matrixconn.Connector
	bridges *fakeBridges
	store   *store.Store
	roomID  id.RoomID
	alice   bridgev2.MatrixAPI
}

func setup(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "store.db"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	med, err := media.New(filepath.Join(dir, "media"))
	if err != nil {
		t.Fatal(err)
	}
	typ := typing.New()
	e := &env{store: st, bridges: &fakeBridges{}}
	e.conn = matrixconn.New(zerolog.Nop(), st, med, typ, "whatsapp", server, owner)
	srv := csapi.New(csapi.Options{
		Log: zerolog.Nop(), Store: st, Media: med, Typing: typ, Bridges: e.bridges, ServerName: server, Owner: owner,
	})
	if err = srv.SetPassword(ctx, password); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	e.alice = e.conn.GhostIntent("alice")
	if err = e.alice.SetDisplayName(ctx, "Alice"); err != nil {
		t.Fatal(err)
	}
	e.roomID, err = e.conn.BotIntent().CreateRoom(ctx, &mautrix.ReqCreateRoom{
		BeeperLocalRoomID:    e.conn.GenerateDeterministicRoomID(networkid.PortalKey{ID: "chat"}),
		BeeperInitialMembers: []id.UserID{owner, e.alice.GetMXID()},
	})
	if err != nil {
		t.Fatal(err)
	}
	e.ghostSays(t, "hello from alice")

	e.cli, err = mautrix.NewClient(ts.URL, "", "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.cli.Login(ctx, &mautrix.ReqLogin{
		Type:             mautrix.AuthTypePassword,
		Identifier:       mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: "me"},
		Password:         password,
		StoreCredentials: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) ghostSays(t *testing.T, body string) id.EventID {
	t.Helper()
	resp, err := e.alice.SendMessage(context.Background(), e.roomID, event.EventMessage,
		&event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: body}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return resp.EventID
}

func bodies(events []*event.Event) []string {
	var out []string
	for _, evt := range events {
		if evt.Type == event.EventMessage {
			_ = evt.Content.ParseRaw(evt.Type)
			out = append(out, evt.Content.AsMessage().Body)
		}
	}
	return out
}

func TestLoginRules(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	other, _ := mautrix.NewClient(e.cli.HomeserverURL.String(), "", "")
	_, err := other.Login(ctx, &mautrix.ReqLogin{
		Type:       mautrix.AuthTypePassword,
		Identifier: mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: "me"},
		Password:   "wrong",
	})
	if !errors.Is(err, mautrix.MForbidden) {
		t.Errorf("wrong password: %v", err)
	}
	if _, err = other.JoinedRooms(ctx); !errors.Is(err, mautrix.MMissingToken) {
		t.Errorf("no token: %v", err)
	}
	who, err := e.cli.Whoami(ctx)
	if err != nil || who.UserID != owner || who.DeviceID == "" {
		t.Errorf("whoami: %+v %v", who, err)
	}
	if _, err = e.cli.MakeRequest(ctx, "POST", e.cli.BuildClientURL("v3", "search"), map[string]any{}, nil); !errors.Is(err, mautrix.MUnrecognized) {
		t.Errorf("unsupported endpoint: %v", err)
	}
}

func TestSync(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	first, err := e.cli.FullSyncRequest(ctx, mautrix.ReqSync{})
	if err != nil {
		t.Fatal(err)
	}
	room := first.Rooms.Join[e.roomID]
	if room == nil {
		t.Fatalf("room missing from first sync: %+v", first.Rooms)
	}
	if got := bodies(room.Timeline.Events); len(got) != 1 || got[0] != "hello from alice" {
		t.Errorf("first sync timeline = %v", got)
	}
	if len(room.Summary.Heroes) != 1 || room.Summary.Heroes[0] != e.alice.GetMXID() {
		t.Errorf("heroes = %v (the bot and you should be left out)", room.Summary.Heroes)
	}
	if room.Timeline.PrevBatch == "" || len(room.State.Events) == 0 {
		t.Errorf("prev_batch %q, %d state events", room.Timeline.PrevBatch, len(room.State.Events))
	}
	for _, evt := range room.Timeline.Events {
		if evt.StateKey != nil {
			t.Errorf("room setup and joins should not be timeline lines, got %s", evt.Type.Type)
		}
	}

	// A long poll returns as soon as a new message arrives.
	go func() {
		time.Sleep(100 * time.Millisecond)
		e.ghostSays(t, "second")
	}()
	start := time.Now()
	next, err := e.cli.FullSyncRequest(ctx, mautrix.ReqSync{Since: first.NextBatch, Timeout: 5000})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 3*time.Second {
		t.Error("long poll didn't return when the message arrived")
	}
	if got := bodies(next.Rooms.Join[e.roomID].Timeline.Events); len(got) != 1 || got[0] != "second" {
		t.Errorf("incremental timeline = %v", got)
	}

	// Typing from a contact shows up as an ephemeral event.
	if err = e.alice.MarkTyping(ctx, e.roomID, bridgev2.TypingTypeText, time.Minute); err != nil {
		t.Fatal(err)
	}
	third, err := e.cli.FullSyncRequest(ctx, mautrix.ReqSync{Since: next.NextBatch, Timeout: 2000})
	if err != nil {
		t.Fatal(err)
	}
	typingSeen := false
	for _, evt := range third.Rooms.Join[e.roomID].Ephemeral.Events {
		if evt.Type == event.EphemeralEventTyping {
			_ = evt.Content.ParseRaw(evt.Type)
			typingSeen = len(evt.Content.AsTyping().UserIDs) == 1
		}
	}
	if !typingSeen {
		t.Error("typing notice missing from sync")
	}

	// An empty poll times out with nothing new.
	empty, err := e.cli.FullSyncRequest(ctx, mautrix.ReqSync{Since: third.NextBatch, Timeout: 100})
	if err != nil || len(empty.Rooms.Join) != 0 {
		t.Errorf("idle sync: %+v %v", empty.Rooms.Join, err)
	}
}

func TestSendFromClient(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	first, _ := e.cli.FullSyncRequest(ctx, mautrix.ReqSync{})

	content := &event.MessageEventContent{MsgType: event.MsgText, Body: "hi alice"}
	sent, err := e.cli.SendMessageEvent(ctx, e.roomID, event.EventMessage, content, mautrix.ReqSendEvent{TransactionID: "txn1"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := e.cli.SendMessageEvent(ctx, e.roomID, event.EventMessage, content, mautrix.ReqSendEvent{TransactionID: "txn1"})
	if err != nil || again.EventID != sent.EventID {
		t.Errorf("retrying the same transaction gave %s, want %s (%v)", again.EventID, sent.EventID, err)
	}

	queued := e.bridges.waitQueued(t, event.EventMessage)
	if queued.Sender != owner || queued.ID != sent.EventID || queued.Content.AsMessage().Body != "hi alice" {
		t.Errorf("bridge got %+v", queued)
	}

	next, err := e.cli.FullSyncRequest(ctx, mautrix.ReqSync{Since: first.NextBatch, Timeout: 1000})
	if err != nil {
		t.Fatal(err)
	}
	events := next.Rooms.Join[e.roomID].Timeline.Events
	if len(events) != 1 || events[0].ID != sent.EventID || events[0].Unsigned.TransactionID != "txn1" {
		t.Errorf("own message in sync: %+v", events)
	}

	if _, err = e.cli.RedactEvent(ctx, e.roomID, sent.EventID); err != nil {
		t.Fatal(err)
	}
	if redaction := e.bridges.waitQueued(t, event.EventRedaction); redaction.Redacts != sent.EventID {
		t.Errorf("redaction target = %s", redaction.Redacts)
	}
}

func TestReceiptsAndSettings(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	first, _ := e.cli.FullSyncRequest(ctx, mautrix.ReqSync{})
	msgID := e.ghostSays(t, "read me")

	if err := e.cli.SendReceipt(ctx, e.roomID, msgID, event.ReceiptTypeRead, nil); err != nil {
		t.Fatal(err)
	}
	receipt := e.bridges.waitQueued(t, event.EphemeralEventReceipt)
	if receipt.Mautrix.EventSource&event.SourceEphemeral == 0 {
		t.Error("receipts must be marked ephemeral for bridgev2")
	}
	if err := e.cli.SendReceipt(ctx, e.roomID, "$nope", event.ReceiptTypeRead, nil); !errors.Is(err, mautrix.MNotFound) {
		t.Errorf("receipt for unknown event: %v", err)
	}

	if err := e.cli.SetRoomAccountData(ctx, e.roomID, event.AccountDataBeeperMute.Type, &event.BeeperMuteEventContent{MutedUntil: -1}); err != nil {
		t.Fatal(err)
	}
	e.bridges.waitQueued(t, event.AccountDataBeeperMute)

	next, err := e.cli.FullSyncRequest(ctx, mautrix.ReqSync{Since: first.NextBatch, Timeout: 1000})
	if err != nil {
		t.Fatal(err)
	}
	room := next.Rooms.Join[e.roomID]
	var sawReceipt, sawMute bool
	for _, evt := range room.Ephemeral.Events {
		sawReceipt = sawReceipt || evt.Type == event.EphemeralEventReceipt
	}
	for _, evt := range room.AccountData.Events {
		sawMute = sawMute || evt.Type.Type == event.AccountDataBeeperMute.Type
	}
	if !sawReceipt || !sawMute {
		t.Errorf("sync: receipt %v, mute %v", sawReceipt, sawMute)
	}
}

func TestHistoryFromNetwork(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	fetched := 0
	e.bridges.older = func(roomID id.RoomID) (bool, error) {
		if fetched > 0 {
			return false, nil
		}
		fetched++
		old := []*event.Event{
			{ID: "$old1", Sender: e.alice.GetMXID(), Type: event.EventMessage, Timestamp: 1000,
				Content: event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: "old 1"}}},
			{ID: "$old2", Sender: e.alice.GetMXID(), Type: event.EventMessage, Timestamp: 2000,
				Content: event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: "old 2"}}},
		}
		_, err := e.conn.BatchSend(ctx, roomID, &mautrix.ReqBeeperBatchSend{Events: old}, nil)
		return true, err
	}
	first, _ := e.cli.FullSyncRequest(ctx, mautrix.ReqSync{})
	prev := first.Rooms.Join[e.roomID].Timeline.PrevBatch

	page, err := e.cli.Messages(ctx, e.roomID, prev, "", mautrix.DirectionBackward, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := bodies(page.Chunk); len(got) != 2 || got[0] != "old 2" || got[1] != "old 1" {
		t.Errorf("older page = %v, want [old 2, old 1] (newest first)", got)
	}
	if len(page.State) == 0 {
		t.Error("page should carry the senders' member events")
	}
	if page.End == "" {
		t.Fatal("network said it may have more, so there should be an end token")
	}
	last, err := e.cli.Messages(ctx, e.roomID, page.End, "", mautrix.DirectionBackward, nil, 10)
	if err != nil || len(last.Chunk) != 0 || last.End != "" {
		t.Errorf("after the start of history: %d events, end %q, %v", len(last.Chunk), last.End, err)
	}
}

func TestPushRulesMediaAndLeave(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	rules, err := e.cli.GetPushRules(ctx)
	if err != nil || len(rules.Underride) == 0 {
		t.Fatalf("default push rules: %v", err)
	}
	err = e.cli.PutPushRule(ctx, "global", pushrules.RoomRule, string(e.roomID), &mautrix.ReqPutPushRule{Actions: []*pushrules.PushAction{}})
	if err != nil {
		t.Fatal(err)
	}
	if rules, _ = e.cli.GetPushRules(ctx); rules.Room.Map[string(e.roomID)] == nil {
		t.Error("room push rule (a mute) was not saved")
	}

	up, err := e.cli.UploadBytes(ctx, []byte("picture"), "image/png")
	if err != nil {
		t.Fatal(err)
	}
	data, err := e.cli.DownloadBytes(ctx, up.ContentURI)
	if err != nil || string(data) != "picture" {
		t.Errorf("download: %q %v", data, err)
	}

}

func TestLeave(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	first, _ := e.cli.FullSyncRequest(ctx, mautrix.ReqSync{})

	// A network that can't leave chats: nothing changes, and the client is told.
	e.bridges.refuse = errors.New("leaving chats is not supported")
	if _, err := e.cli.LeaveRoom(ctx, e.roomID); !errors.Is(err, mautrix.MForbidden) {
		t.Errorf("refused leave: %v", err)
	}
	if rooms, _ := e.cli.JoinedRooms(ctx); len(rooms.JoinedRooms) != 1 {
		t.Error("the room should stay joined when the network refuses")
	}

	// A network that can: the leave goes to the bridge first, then the room is left here.
	e.bridges.refuse = nil
	if _, err := e.cli.LeaveRoom(ctx, e.roomID); err != nil {
		t.Fatal(err)
	}
	leave := e.bridges.waitQueued(t, event.StateMember)
	if leave.GetStateKey() != owner.String() || leave.Content.AsMember().Membership != event.MembershipLeave {
		t.Errorf("bridge got %+v", leave)
	}
	if prev := leave.Unsigned.PrevContent; prev == nil || prev.AsMember().Membership != event.MembershipJoin {
		t.Error("bridgev2 needs the previous membership (join) to see this as leaving")
	}
	next, err := e.cli.FullSyncRequest(ctx, mautrix.ReqSync{Since: first.NextBatch, Timeout: 1000})
	if err != nil || next.Rooms.Leave[e.roomID] == nil {
		t.Errorf("left room should be in the leave section: %+v %v", next.Rooms, err)
	}
	if _, err = e.cli.LeaveRoom(ctx, e.roomID); err != nil {
		t.Errorf("leaving twice should be fine: %v", err)
	}
}

func TestNoEncryption(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	versions, err := e.cli.Versions(ctx)
	if err != nil || !versions.UnstableFeatures["com.tuimessenger.no_e2ee"] || !versions.UnstableFeatures["com.tuimessenger.viewing"] ||
		!versions.UnstableFeatures["com.tuimessenger.reconnect"] || !versions.UnstableFeatures["com.tuimessenger.networks"] {
		t.Errorf("versions should announce no_e2ee: %+v %v", versions, err)
	}
	var upload struct {
		Counts map[string]int `json:"one_time_key_counts"`
	}
	_, err = e.cli.MakeRequest(ctx, "POST", e.cli.BuildClientURL("v3", "keys", "upload"), map[string]any{}, &upload)
	if err != nil || upload.Counts["signed_curve25519"] == 0 {
		t.Errorf("keys/upload: %+v %v", upload, err)
	}
	resp, err := e.cli.FullSyncRequest(ctx, mautrix.ReqSync{})
	if err != nil || resp.DeviceOTKCount.SignedCurve25519 == 0 {
		t.Errorf("sync should report a full key count so clients don't keep uploading: %+v %v", resp.DeviceOTKCount, err)
	}
	if _, err = e.cli.GetKeyBackupLatestVersion(ctx); !errors.Is(err, mautrix.MNotFound) {
		t.Errorf("key backup: %v", err)
	}
}

func TestViewing(t *testing.T) {
	e := setup(t)
	e.bridges.slowFirstView = true // changes must still arrive in order
	ctx := context.Background()
	url := e.cli.BuildURL(mautrix.BaseURLPath{"_matrix", "client", "unstable", "com.tuimessenger", "viewing"})
	view := func(roomID id.RoomID) {
		t.Helper()
		if _, err := e.cli.MakeRequest(ctx, "PUT", url, map[string]any{"room_id": roomID}, nil); err != nil {
			t.Fatal(err)
		}
	}
	view(e.roomID)
	view(e.roomID) // a refresh, which bridgev2 expects
	view("")
	view("") // closing twice only clears once
	time.Sleep(200 * time.Millisecond)
	got := e.bridges.viewed()
	if len(got) != 3 || got[0] != e.roomID || got[1] != e.roomID || got[2] != "" {
		t.Errorf("bridges saw %v, want [room room \"\"]", got)
	}
}

func TestReconnect(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	url := e.cli.BuildURL(mautrix.BaseURLPath{"_matrix", "client", "unstable", "com.tuimessenger", "reconnect"})
	reconnect := func() []string {
		t.Helper()
		var resp struct {
			Reconnecting []string `json:"reconnecting"`
		}
		if _, err := e.cli.MakeRequest(ctx, "POST", url, struct{}{}, &resp); err != nil {
			t.Fatal(err)
		} else if resp.Reconnecting == nil {
			t.Fatal("reconnecting should be a list, even when empty")
		}
		return resp.Reconnecting
	}
	if got := reconnect(); len(got) != 0 {
		t.Errorf("all healthy: got %v", got)
	}
	e.bridges.lock.Lock()
	e.bridges.broken = []string{"whatsapp"}
	e.bridges.lock.Unlock()
	if got := reconnect(); len(got) != 1 || got[0] != "whatsapp" {
		t.Errorf("got %v, want [whatsapp]", got)
	}
}

func TestNetworkSettings(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	base := mautrix.BaseURLPath{"_matrix", "client", "unstable", "com.tuimessenger", "networks"}
	list := func() csapi.Network {
		t.Helper()
		var resp struct {
			Networks []csapi.Network `json:"networks"`
		}
		if _, err := e.cli.MakeRequest(ctx, "GET", e.cli.BuildURL(base), nil, &resp); err != nil {
			t.Fatal(err)
		} else if len(resp.Networks) != 1 {
			t.Fatalf("got %+v", resp.Networks)
		}
		return resp.Networks[0]
	}
	if n := list(); n.Name != "slack" || n.State != "CONNECTED" || n.Settings["include_channels"] {
		t.Errorf("got %+v", n)
	}
	set := func(network string, body map[string]bool) error {
		_, err := e.cli.MakeRequest(ctx, "PUT", e.cli.BuildURL(append(base, network, "settings")), body, nil)
		return err
	}
	if err := set("slack", map[string]bool{"include_channels": true}); err != nil {
		t.Fatal(err)
	}
	if n := list(); !n.Settings["include_channels"] {
		t.Errorf("setting didn't change: %+v", n)
	}
	if err := set("slack", map[string]bool{"nope": true}); !errors.Is(err, mautrix.MNotFound) {
		t.Errorf("unknown setting: want M_NOT_FOUND, got %v", err)
	}
}

func TestEnableNetwork(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	enable := func(network string) error {
		url := e.cli.BuildURL(mautrix.BaseURLPath{"_matrix", "client", "unstable", "com.tuimessenger", "networks", network, "enable"})
		_, err := e.cli.MakeRequest(ctx, "POST", url, struct{}{}, nil)
		return err
	}
	if err := enable("mattermost"); err != nil {
		t.Fatal(err)
	}
	if err := enable("nope"); !errors.Is(err, mautrix.MNotFound) {
		t.Errorf("unknown network: want M_NOT_FOUND, got %v", err)
	}
	e.bridges.lock.Lock()
	defer e.bridges.lock.Unlock()
	if len(e.bridges.enabled) != 1 || e.bridges.enabled[0] != "mattermost" {
		t.Errorf("enabled %v", e.bridges.enabled)
	}
}

func TestDirectMediaDownload(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	fileID := media.DirectID("whatsapp", []byte("photo-123"))
	e.bridges.remote = map[string]string{fileID: "jpeg bytes"}

	resp, err := e.cli.Download(ctx, id.ContentURI{Homeserver: server, FileID: fileID})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(data) != "jpeg bytes" || resp.Header.Get("Content-Type") != "image/jpeg" {
		t.Errorf("got %q as %q", data, resp.Header.Get("Content-Type"))
	}
	_, err = e.cli.DownloadBytes(ctx, id.ContentURI{Homeserver: server, FileID: media.DirectID("whatsapp", []byte("gone"))})
	if !errors.Is(err, mautrix.MNotFound) {
		t.Errorf("unknown direct media: %v", err)
	}
}

// waitRefused waits until the bridge has refused an event with this ID; forward runs in the background.
func (f *fakeBridges) waitRefused(t *testing.T, eventID id.EventID) {
	t.Helper()
	for range 100 {
		f.lock.Lock()
		for _, evt := range f.refused {
			if evt.ID == eventID {
				f.lock.Unlock()
				time.Sleep(20 * time.Millisecond) // forward saves its follow-ups right after Queue returns
				return
			}
		}
		f.lock.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the bridge never got %s", eventID)
}

// failureFor reports whether a "failed" delivery status was saved for eventID.
func failureFor(t *testing.T, st *store.Store, eventID id.EventID) bool {
	t.Helper()
	events, err := st.EventsSince(context.Background(), 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	for _, evt := range events {
		if evt.Type == event.BeeperMessageStatus && gjson.GetBytes(evt.Content.VeryRaw, `m\.relates_to.event_id`).Str == eventID.String() {
			return true
		}
	}
	return false
}

// A reaction the network rejects (Telegram: 😂 without Premium) is removed here too, so it no longer counts as
// yours; taking back such a reaction later is done, not "not sent".
func TestRejectedReactionRemoved(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	msg, err := e.cli.SendText(ctx, e.roomID, "hi")
	if err != nil {
		t.Fatal(err)
	}
	react := func(key string) id.EventID {
		t.Helper()
		resp, err := e.cli.SendReaction(ctx, e.roomID, msg.EventID, key)
		if err != nil {
			t.Fatal(err)
		}
		return resp.EventID
	}

	e.bridges.lock.Lock()
	e.bridges.refuse = errors.New("rpc error code 400: REACTION_INVALID")
	e.bridges.lock.Unlock()
	rejected := react("😂")
	e.bridges.waitRefused(t, rejected)
	if redacted, err := e.store.IsRedacted(ctx, rejected); err != nil || !redacted {
		t.Errorf("a rejected reaction should be removed here: redacted=%v %v", redacted, err)
	}
	if !failureFor(t, e.store, rejected) {
		t.Error("the failure should still be reported, so the client can say why")
	}

	// A dead reaction from an older version: the bridge doesn't know it, so taking it back is just done here.
	e.bridges.lock.Lock()
	e.bridges.refuse = nil
	e.bridges.lock.Unlock()
	old := react("👍")
	e.bridges.waitQueued(t, event.EventReaction)
	e.bridges.lock.Lock()
	e.bridges.refuse = fmt.Errorf("redaction %w", bridgev2.ErrTargetMessageNotFound)
	e.bridges.lock.Unlock()
	undo, err := e.cli.RedactEvent(ctx, e.roomID, old)
	if err != nil {
		t.Fatal(err)
	}
	e.bridges.waitRefused(t, undo.EventID)
	if failureFor(t, e.store, undo.EventID) {
		t.Error("taking back a reaction the network never had isn't a failure")
	}
	if redacted, _ := e.store.IsRedacted(ctx, old); !redacted {
		t.Error("the reaction should be gone here")
	}
}
