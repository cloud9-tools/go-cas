package script // import "github.com/chronos-tachyon/go-cas/cmd/casutil/internal/script"

import (
	"errors"
	"io"
	"strconv"
	"unicode"
	"unicode/utf8"
)

type TokenType uint8

const (
	ErrorToken TokenType = iota
	WordToken
	CommentToken
	NewlineToken
)

type Token struct {
	Type  TokenType
	Value string
	Error error
}

func Parse(r io.Reader) ([][]string, error) {
	var lines [][]string
	var line []string
	ch, _ := Lex(r)
	for tok := range ch {
		switch tok.Type {
		case ErrorToken:
			return nil, tok.Error
		case WordToken:
			line = append(line, tok.Value)
		case NewlineToken:
			if len(line) > 0 {
				lines = append(lines, line)
				line = nil
			}
		}
	}
	return lines, nil
}

type state uint8

const (
	normal state = iota
	inSQ
	inDQ
	inDQBS
	inDQOct
	inDQHex
	inBS
	inComment
	inWhitespace
	inNewline
)

func Lex(r io.Reader) (<-chan Token, chan<- struct{}) {
	reader := newRuneScanner(r)
	ch := make(chan Token)
	cancel := make(chan struct{})
	go func() {
		var partial, partial2 string
		var maxDigits int
		var tmp [4]byte
		var state = normal
		var r rune
		var n int
		var err error
		var eof bool
		for !eof {
			select {
			case <-cancel:
				close(ch)
				return
			default:
			}
			r, n, err = reader.ReadRune()
			if err == nil {
				utf8.EncodeRune(tmp[0:n], r)
			} else if err == io.EOF {
				eof = true
				r, n, err = '\n', 0, nil
			} else {
				ch <- Token{ErrorToken, "", err}
				close(ch)
				return
			}
			switch state {
			case normal:
				var end bool
				switch {
				case r == '\'':
					state = inSQ
				case r == '"':
					state = inDQ
				case r == '\\':
					state = inBS
				case r == '#':
					end = true
					state = inComment
				case isNewline(r):
					end = true
					state = inNewline
				case unicode.IsSpace(r):
					end = true
					state = inWhitespace
				case eof:
					end = true
				default:
					partial += string(tmp[0:n])
				}
				if end && partial != "" {
					ch <- Token{WordToken, partial, nil}
					partial = ""
				}
			case inSQ:
				if r == '\'' {
					state = normal
				} else {
					partial += string(tmp[0:n])
				}
			case inDQ:
				if r == '"' {
					state = normal
				} else if r == '\\' {
					state = inDQBS
				} else {
					partial += string(tmp[0:n])
				}
			case inDQBS:
				switch r {
				case 'a':
					partial += "\a"
				case 'b':
					partial += "\b"
				case 't':
					partial += "\t"
				case 'n':
					partial += "\n"
				case 'v':
					partial += "\v"
				case 'f':
					partial += "\f"
				case 'r':
					partial += "\r"
				case 'e':
					partial += "\x1B"
				case '0', '1':
					maxDigits = 3
					state = inDQOct
					partial2 += string(tmp[0:n])
				case '2', '3', '4', '5', '6', '7':
					maxDigits = 2
					state = inDQOct
					partial2 += string(tmp[0:n])
				case 'x':
					maxDigits = 2
					state = inDQHex
				case 'u':
					maxDigits = 4
					state = inDQHex
				case 'U':
					maxDigits = 8
					state = inDQHex
				default:
					partial += string(tmp[0:n])
					state = inDQ
				}
			case inDQOct:
				var end bool
				if isOctDigit(r) {
					partial2 += string(tmp[0:n])
					end = len(partial2) == maxDigits
				} else {
					reader.UnreadRune()
					end = true
				}
				if end {
					x, _ := strconv.ParseUint(partial2, 8, 32)
					r = rune(x)
					n = utf8.EncodeRune(tmp[:], r)
					partial += string(tmp[0:n])
					partial2 = ""
					maxDigits = 0
					state = inDQ
				}
			case inDQHex:
				var end bool
				if isHexDigit(r) {
					partial2 += string(tmp[0:n])
					end = len(partial2) == maxDigits
				} else {
					reader.UnreadRune()
					end = true
				}
				if end {
					x, _ := strconv.ParseUint(partial2, 16, 32)
					r = rune(x)
					n = utf8.EncodeRune(tmp[:], r)
					partial += string(tmp[0:n])
					partial2 = ""
					maxDigits = 0
					state = inDQ
				}
			case inBS:
				partial += string(tmp[0:n])
				state = normal
			case inComment:
				if eof || isNewline(r) {
					reader.UnreadRune()
					ch <- Token{CommentToken, partial, nil}
					partial = ""
					state = inNewline
				} else {
					partial += string(tmp[0:n])
				}
			case inWhitespace:
				if eof || !unicode.IsSpace(r) {
					reader.UnreadRune()
					state = normal
				}
			case inNewline:
				if !isNewline(r) {
					reader.UnreadRune()
					ch <- Token{NewlineToken, "\n", nil}
					state = normal
				}
			}
		}
		ch <- Token{NewlineToken, "\n", nil}
		close(ch)
	}()
	return ch, cancel
}

func isNewline(r rune) bool {
	switch r {
	case '\n', '\v', '\r', 0x85:
		return true
	default:
		return false
	}
}

func isOctDigit(r rune) bool {
	switch r {
	case '0', '1', '2', '3', '4', '5', '6', '7':
		return true
	default:
		return false
	}
}

func isHexDigit(r rune) bool {
	switch r {
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'a', 'b', 'c', 'd', 'e', 'f':
		return true
	default:
		return false
	}
}

type runeState uint8

const (
	noRune runeState = iota
	savedRune
	unreadRune
)

type runeScanner struct {
	Reader    io.Reader
	Buffer    []byte
	Rune      rune
	RuneLen   int
	RuneState runeState
	Error     error
}

func newRuneScanner(r io.Reader) io.RuneScanner {
	return &runeScanner{Reader: r}
}

func (scanner *runeScanner) ReadRune() (rune, int, error) {
	if scanner.RuneState == unreadRune {
		r, n := scanner.Rune, scanner.RuneLen
		scanner.Rune, scanner.RuneLen, scanner.RuneState = 0, 0, noRune
		return r, n, nil
	}
Redo:
	if utf8.FullRune(scanner.Buffer) {
		r, n := utf8.DecodeRune(scanner.Buffer)
		scanner.Rune, scanner.RuneLen, scanner.RuneState = r, n, savedRune
		scanner.Buffer = scanner.Buffer[n:]
		return r, n, nil
	}
	if len(scanner.Buffer) > 0 && scanner.Error != nil {
		r, n := utf8.DecodeRune(scanner.Buffer)
		scanner.Rune, scanner.RuneLen, scanner.RuneState = r, n, savedRune
		scanner.Buffer = nil
		return r, n, nil
	}
	if scanner.Error != nil {
		return 0, 0, scanner.Error
	}
	newbuf := make([]byte, 4096)
	n, err := scanner.Reader.Read(newbuf)
	newbuf = newbuf[:n]
	scanner.Buffer = append(scanner.Buffer, newbuf...)
	scanner.Error = err
	goto Redo
}

func (scanner *runeScanner) UnreadRune() error {
	if scanner.RuneState == savedRune {
		scanner.RuneState = unreadRune
		return nil
	}
	return errors.New("can only unread one rune")
}
