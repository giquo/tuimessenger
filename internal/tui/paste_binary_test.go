package tui

import (
	"bytes"
	"image"
	"image/png"
	"strings"
	"testing"

	"go.mau.fi/mauview"

	"github.com/whyyor/tuimessenger/internal/tui/config"
)

// A screenshot pasted on Wayland arrives as its PNG bytes; real text, even with a stray �, isn't mistaken for it.
func TestPastedBinary(t *testing.T) {
	var buf bytes.Buffer
	if err := png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 64, 64))); err != nil {
		t.Fatal(err)
	}
	jpegish := string([]byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 'J', 'F', 'I', 'F', 0x00, 0x01, 0x01, 0x00, 0x00, 0x48, 0x00, 0x48})
	for _, c := range []struct {
		name string
		text string
		want bool
	}{
		{"PNG bytes", buf.String(), true},
		{"PNG after a UTF-8 clean-up", strings.ToValidUTF8(buf.String(), "\uFFFD"), true},
		{"JPEG bytes", jpegish, true},
		{"mostly invalid bytes", strings.Repeat("\xfe\xff", 20), true},
		{"plain text", "see you at 8, booked a table near the lake", false},
		{"text with a stray �", "the file was called r\uFFFDsum\uFFFD.pdf, can you resend it?", false},
		{"emoji and accents", "café ☕ très bien 🎉 ok", false},
		{"empty", "", false},
		{"short garbage", "\xfe\xff", false},
	} {
		if got := pastedBinary(c.text); got != c.want {
			t.Errorf("%s: pastedBinary = %v, want %v", c.name, got, c.want)
		}
	}
}

// A text paste (e.g. a middle-click of the primary selection) still lands in the composer as text, whatever is on
// the clipboard.
func TestTextPasteStaysText(t *testing.T) {
	cfg := &config.Config{}
	cfg.LoadKeybindings()
	view := &RoomView{config: cfg, input: mauview.NewInputArea()}
	view.OnPasteEvent(fakePaste{text: "selected words"})
	if got := view.input.GetText(); got != "selected words" {
		t.Errorf("composer has %q", got)
	}
	if len(view.Attachments()) != 0 {
		t.Error("a text paste must not attach anything")
	}
}
