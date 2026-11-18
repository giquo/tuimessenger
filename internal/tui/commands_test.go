package tui

import "testing"

// A network either leaves chats on the network too (leaveNetworks) or only here (localLeaveNetworks), never both.
func TestLeaveNetworksDisjoint(t *testing.T) {
	for id := range localLeaveNetworks {
		if _, ok := leaveNetworks[id]; ok {
			t.Errorf("%s is in both leaveNetworks and localLeaveNetworks", id)
		}
	}
	if localLeaveNetworks["mattermost"] == "" {
		t.Error("the Mattermost bridge can't leave channels: /leave should say it only leaves here")
	}
}
