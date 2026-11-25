package termapp

import (
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/gdamore/tcell/v2"
)

// The Kitty keyboard protocol (https://sw.kovidgoyal.net/kitty/keyboard-protocol/) reports keys that legacy
// terminals send identically or not at all: Ctrl+m (legacy: Enter), Ctrl+; (legacy: nothing), Shift+Enter (legacy:
// Enter). tcell 2 can't parse those reports, so kittyTty turns them back into the bytes tuimessenger's keys already
// use; these are the bytes the Ghostty keybinds in the README send, so those keep working too.
const (
	// Push flag 1 (disambiguate escape codes) onto the terminal's stack; pop it again.
	kittyOn  = "\x1b[>1u"
	kittyOff = "\x1b[<u"
	// Stand-ins for keys legacy encoding can't tell apart: Ctrl+^ and Ctrl+_.
	ctrlMByte         = 0x1e
	ctrlSemicolonByte = 0x1f
)

// kittyTty is the terminal tcell reads from: it turns the protocol on while tuimessenger draws and translates key
// reports in what tcell reads.
type kittyTty struct {
	tcell.Tty
	push    bool
	on      atomic.Bool
	pending []byte
	out     []byte
}

// newKittyTty opens the terminal; nil if it can't, and tcell opens it itself. TUIMESSENGER_KITTY_KEYBOARD=0 leaves
// the protocol off (reports some terminals send anyway are still translated).
func newKittyTty() *kittyTty {
	tty, err := tcell.NewDevTty()
	if err != nil {
		return nil
	}
	return &kittyTty{Tty: tty, push: os.Getenv("TUIMESSENGER_KITTY_KEYBOARD") != "0"}
}

// enable turns the protocol on; call it after tcell entered the alternate screen (Init, Resume).
func (k *kittyTty) enable() {
	if k != nil && k.push && k.on.CompareAndSwap(false, true) {
		_, _ = k.Tty.Write([]byte(kittyOn))
	}
}

// disable turns it off; call it before tcell leaves the alternate screen (Suspend, Fini).
func (k *kittyTty) disable() {
	if k != nil && k.on.CompareAndSwap(true, false) {
		_, _ = k.Tty.Write([]byte(kittyOff))
	}
}

func (k *kittyTty) Start() error {
	k.pending, k.out = nil, nil
	return k.Tty.Start()
}

func (k *kittyTty) Read(p []byte) (int, error) {
	if len(k.out) == 0 {
		n, err := k.Tty.Read(p)
		buf := append(k.pending, p[:n]...)
		if n == 0 || err != nil {
			// Woken up to stop: hand over whatever was held back.
			k.out, k.pending = buf, nil
		} else {
			k.out, k.pending = translate(buf)
		}
		if len(k.out) == 0 {
			return 0, err
		}
	}
	n := copy(p, k.out)
	k.out = k.out[n:]
	return n, nil
}

// translate replaces Kitty key reports (CSI … u) with legacy bytes. A report cut off at the end is returned as
// pending, to be completed by the next read.
func translate(in []byte) (out, pending []byte) {
	out = make([]byte, 0, len(in))
	for i := 0; i < len(in); {
		if in[i] != 0x1b || i+1 >= len(in) || in[i+1] != '[' {
			out = append(out, in[i])
			i++
			continue
		}
		j := i + 2
		for j < len(in) && (in[j] >= '0' && in[j] <= '9' || in[j] == ';' || in[j] == ':') {
			j++
		}
		if j == len(in) && j > i+2 {
			return out, append([]byte(nil), in[i:]...)
		}
		if j < len(in) && in[j] == 'u' && j > i+2 {
			if b, ok := reportBytes(string(in[i+2 : j])); ok {
				out = append(out, b...)
				i = j + 1
				continue
			}
		}
		if j < len(in) && j > i+2 {
			if b, ok := functionalReportBytes(string(in[i+2:j]), in[j]); ok {
				out = append(out, b...)
				i = j + 1
				continue
			}
		}
		out = append(out, in[i])
		i++
	}
	return out, nil
}

// functionalReportBytes normalizes Kitty's CSI 1;modifiers KEY form to the
// legacy sequences in terminfo. Disambiguate-escape-codes mode uses this form
// even for an unmodified cursor key, for example CSI 1;1A for Up, while tcell
// expects CSI A.
func functionalReportBytes(params string, final byte) ([]byte, bool) {
	switch final {
	case 'A', 'B', 'C', 'D', 'H', 'F', '~':
	default:
		return nil, false
	}
	fields := strings.Split(params, ";")
	if len(fields) < 2 {
		return nil, false
	}
	first := strings.SplitN(fields[0], ":", 2)[0]
	if first == "" {
		return nil, false
	}
	if _, err := strconv.Atoi(first); err != nil {
		return nil, false
	}
	modField := strings.SplitN(fields[1], ":", 2)
	mods, err := strconv.Atoi(modField[0])
	if err != nil || mods < 1 {
		return nil, false
	}
	if len(modField) == 2 && modField[1] == "3" {
		return nil, true
	}
	kittyMods := mods - 1
	kittyMods &^= modCapsLock | modNumLock
	if kittyMods&(modSuper|modHyper|modMeta) != 0 {
		return nil, true
	}
	return legacySpecialKey(first, final, kittyMods), true
}

// reportBytes is what a report "key[:alternates];mods[:event][;text]" stands for in legacy encoding; nil drops it.
func reportBytes(params string) ([]byte, bool) {
	fields := strings.Split(params, ";")
	key, err := strconv.Atoi(strings.SplitN(fields[0], ":", 2)[0])
	if err != nil {
		return nil, false
	}
	mods := 1
	if len(fields) > 1 && fields[1] != "" {
		sub := strings.SplitN(fields[1], ":", 2)
		if mods, err = strconv.Atoi(sub[0]); err != nil {
			return nil, false
		}
		if len(sub) == 2 && sub[1] == "3" {
			return nil, true
		}
	}
	return legacyKey(rune(key), mods-1)
}

const (
	modShift = 1 << iota
	modAlt
	modCtrl
	modSuper
	modHyper
	modMeta
	modCapsLock
	modNumLock
)

// legacyKey is the legacy encoding of a key with Kitty modifier bits.
func legacyKey(key rune, mods int) ([]byte, bool) {
	mods &^= modCapsLock | modNumLock
	if mods&(modSuper|modHyper|modMeta) != 0 {
		return nil, true
	}
	shift, alt, ctrl := mods&modShift != 0, mods&modAlt != 0, mods&modCtrl != 0
	var b []byte
	switch {
	case key >= 57348 && key <= 57357:
		keys := [...]struct {
			number string
			final  byte
		}{
			{"2", '~'}, {"3", '~'}, // Insert, Delete.
			{"1", 'D'}, {"1", 'C'}, {"1", 'A'}, {"1", 'B'}, // Left, Right, Up, Down.
			{"5", '~'}, {"6", '~'}, {"1", 'H'}, {"1", 'F'}, // PageUp, PageDown, Home, End.
		}
		k := keys[key-57348]
		return legacySpecialKey(k.number, k.final, mods), true
	case key == 27:
		b = []byte{0x1b}
	case key == 13 || key == 57414:
		// Shift+Enter (and Ctrl+Enter) is a new line, sent as LF.
		if shift || ctrl {
			b = []byte{'\n'}
		} else {
			b = []byte{'\r'}
		}
	case key == 9:
		if shift {
			b = []byte("\x1b[Z")
		} else {
			b = []byte{'\t'}
		}
	case key == 127:
		if ctrl {
			b = []byte{0x08}
		} else {
			b = []byte{0x7f}
		}
	case key >= 57344:
		// Keypad, media and lone modifier keys: nothing tuimessenger uses.
		return nil, true
	case ctrl && key < utf8.RuneSelf:
		b = []byte{ctrlByte(key)}
	default:
		if shift && key >= 'a' && key <= 'z' {
			key -= 'a' - 'A'
		}
		b = utf8.AppendRune(nil, key)
	}
	if alt {
		b = append([]byte{0x1b}, b...)
	}
	return b, true
}

// legacySpecialKey encodes a cursor or navigation key in a form terminfo and
// tcell understand. With no modifiers, cursor keys omit the redundant 1;1
// parameter that Kitty uses.
func legacySpecialKey(number string, final byte, mods int) []byte {
	var sequence string
	switch {
	case mods == 0 && number == "1" && final != '~':
		sequence = string(final)
	case mods == 0:
		sequence = number + string(final)
	default:
		sequence = number + ";" + strconv.Itoa(mods+1) + string(final)
	}
	return append([]byte("\x1b["), sequence...)
}

// ctrlByte is Ctrl plus a key, as xterm sends it, except Ctrl+m and Ctrl+; get their stand-ins.
func ctrlByte(key rune) byte {
	switch {
	case key == 'm':
		return ctrlMByte
	case key == ';':
		return ctrlSemicolonByte
	case key >= 'a' && key <= 'z', key >= 'A' && key <= 'Z':
		return byte(key) & 0x1f
	}
	switch key {
	case ' ', '@', '2':
		return 0x00
	case '[', '3':
		return 0x1b
	case '\\', '4':
		return 0x1c
	case ']', '5':
		return 0x1d
	case '^', '6':
		return ctrlMByte
	case '_', '-', '/', '7':
		return ctrlSemicolonByte
	case '?', '8':
		return 0x7f
	}
	return byte(key)
}
