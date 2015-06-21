package script

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strconv"
	"unicode"
	"unicode/utf8"
)

var _ = strconv.ParseUint

type LexError uint8

const (
	ErrUnterminatedSQS LexError = iota
	ErrUnterminatedDQS
	ErrUnterminatedBS
)

func (err LexError) GoString() string {
	switch err {
	case ErrUnterminatedSQS:
		return "script.ErrUnterminatedSQS"
	case ErrUnterminatedDQS:
		return "script.ErrUnterminatedDQS"
	case ErrUnterminatedBS:
		return "script.ErrUnterminatedBS"
	default:
		return fmt.Sprintf("script.LexError(%d)", uint8(err))
	}
}

func (err LexError) String() string {
	return err.GoString()
}

func (err LexError) Error() string {
	switch err {
	case ErrUnterminatedSQS:
		return "unterminated single-quoted string"
	case ErrUnterminatedDQS:
		return "unterminated double-quoted string"
	case ErrUnterminatedBS:
		return "unterminated backslash"
	default:
		return err.String()
	}
}

type Token struct {
	Type  TokenType
	Value string
	Error error
}
type TokenType uint8

const (
	ErrorToken TokenType = iota
	WordToken
	CommentToken
	WhitespaceToken
	NewlineToken
)

func (tt TokenType) GoString() string {
	switch tt {
	case ErrorToken:
		return "script.ErrorToken"
	case WordToken:
		return "script.WordToken"
	case CommentToken:
		return "script.CommentToken"
	case WhitespaceToken:
		return "script.WhitespaceToken"
	case NewlineToken:
		return "script.NewlineToken"
	default:
		return fmt.Sprintf("script.TokenType(%d)", uint8(tt))
	}
}

func (tt TokenType) String() string {
	return tt.GoString()
}

func Parse(r io.Reader) ([][]string, error) {
	var lines [][]string
	var line []string
	raw, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	for tok := range NewLexer(raw).Go() {
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

type Lexer struct {
	In []byte
}

func NewLexer(in []byte) *Lexer {
	return &Lexer{in}
}

func (l *Lexer) Go() <-chan Token {
	ch := make(chan Token)
	go l.Lex(ch)
	return ch
}

func (l *Lexer) Lex(ch chan<- Token) {
	var word string
	defer close(ch)
	for {
		if len(l.In) == 0 {
			if len(word) != 0 {
				ch <- Token{WordToken, string(word), nil}
				word = ""
			}
			ch <- Token{NewlineToken, "\n", nil}
			return
		}

		r, n := utf8.DecodeRune(l.In)
		b := l.In[0:n]
		l.In = l.In[n:]

		if IsNewline(r) {
			l.While(IsNewline)
			if len(word) != 0 {
				ch <- Token{WordToken, string(word), nil}
				word = ""
			}
			ch <- Token{NewlineToken, "\n", nil}
			if len(l.In) == 0 {
				return
			}
			continue
		}

		if IsSpace(r) {
			l.While(IsSpace)
			if len(word) != 0 {
				ch <- Token{WordToken, string(word), nil}
				word = ""
			}
			ch <- Token{WhitespaceToken, " ", nil}
			continue
		}

		switch r {
		case '"':
			dqstring, err := l.LexDoubleQuotedString()
			word += dqstring
			if err != nil {
				ch <- Token{WordToken, string(word), nil}
				ch <- Token{ErrorToken, "", err}
				return
			}
			l.In = l.In[1:]

		case '\'':
			sqstring := l.Until(IsChar('\''))
			word += sqstring
			if len(l.In) == 0 {
				ch <- Token{WordToken, string(word), nil}
				ch <- Token{ErrorToken, "", ErrUnterminatedSQS}
				return
			}
			l.In = l.In[1:]

		case '\\':
			if len(l.In) == 0 {
				ch <- Token{WordToken, string(word), nil}
				ch <- Token{ErrorToken, "", ErrUnterminatedBS}
				return
			}
			_, n := utf8.DecodeRune(l.In)
			word += string(l.In[0:n])
			l.In = l.In[n:]

		case '#':
			comment := l.Until(IsNewline)
			if len(word) != 0 {
				ch <- Token{WordToken, string(word), nil}
				word = ""
			}
			ch <- Token{CommentToken, string(comment), nil}

		default:
			rest := l.Until(IsMeta)
			word += string(b)
			word += rest
		}
	}
}

func (l *Lexer) LexDoubleQuotedString() (string, error) {
	var accum string
	for {
		accum += l.Until(IsDQMeta)
		if len(l.In) == 0 {
			return accum, ErrUnterminatedDQS
		}
		if l.In[0] == '"' {
			return accum, nil
		}
		_, n := utf8.DecodeRune(l.In)
		l.In = l.In[n:]
		r, n := utf8.DecodeRune(l.In)
		switch r {
		case 'a', 'b', 't', 'n', 'v', 'f', 'r', 'e':
			l.In = l.In[n:]
			accum += Escapes[r]

		case '0', '1', '2', '3', '4', '5', '6', '7':
			x, err := l.LexOctalEscape()
			if err != nil {
				return accum, err
			}
			accum += EncodeRuneToString(x)

		case 'x':
			l.In = l.In[n:]
			x, err := l.LexHexEscape(2)
			if err != nil {
				return accum, err
			}
			accum += EncodeRuneToString(x)

		case 'u':
			l.In = l.In[n:]
			x, err := l.LexHexEscape(4)
			if err != nil {
				return accum, err
			}
			accum += EncodeRuneToString(x)

		case 'U':
			l.In = l.In[n:]
			x, err := l.LexHexEscape(8)
			if err != nil {
				return accum, err
			}
			accum += EncodeRuneToString(x)

		default:
			accum += string(l.In[0:n])
			l.In = l.In[n:]
		}
	}
}

func (l *Lexer) LexOctalEscape() (rune, error) {
	var digits string
	for len(digits) < 3 && len(l.In) != 0 {
		if !IsOctDigit(rune(l.In[0])) {
			break
		}
		digits += string(l.In[0:1])
		l.In = l.In[1:]
	}
	x, err := strconv.ParseUint(digits, 8, 32)
	return rune(x), err
}

func (l *Lexer) LexHexEscape(n int) (rune, error) {
	var digits string
	for len(digits) < n && len(l.In) != 0 {
		if !IsHexDigit(rune(l.In[0])) {
			break
		}
		digits += string(l.In[0:1])
		l.In = l.In[1:]
	}
	if digits == "" {
		return 0, ErrUnterminatedBS
	}
	x, err := strconv.ParseUint(digits, 16, 32)
	return rune(x), err
}

func (l *Lexer) Until(pred func(rune) bool) string {
	result := []byte(nil)
	for len(l.In) != 0 {
		r, n := utf8.DecodeRune(l.In)
		if pred(r) {
			break
		}
		result = append(result, l.In[0:n]...)
		l.In = l.In[n:]
	}
	return string(result)
}

func (l *Lexer) While(pred func(rune) bool) string {
	return l.Until(Not(pred))
}

func EncodeRuneToString(r rune) string {
	var tmp [4]byte
	n := utf8.EncodeRune(tmp[:], r)
	log.Printf("r=%U b=%#v", r, tmp[:n])
	return string(tmp[:n])
}

func Not(pred func(rune) bool) func(rune) bool {
	return func(r rune) bool {
		return !pred(r)
	}
}

func IsChar(ch rune) func(rune) bool {
	return func(r rune) bool {
		return r == ch
	}
}

func IsNewline(r rune) bool {
	switch r {
	case '\n', '\v', '\r', 0x85:
		return true
	default:
		return false
	}
}

func IsSpace(r rune) bool {
	return unicode.IsSpace(r) && !IsNewline(r)
}

func IsMeta(r rune) bool {
	switch r {
	case '"', '\'', '\\', '#':
		return true
	default:
		return IsNewline(r) || IsSpace(r)
	}
}

func IsDQMeta(r rune) bool {
	switch r {
	case '"', '\\':
		return true
	default:
		return false
	}
}

func IsOctDigit(r rune) bool {
	switch r {
	case '0', '1', '2', '3', '4', '5', '6', '7':
		return true
	default:
		return false
	}
}

func IsHexDigit(r rune) bool {
	switch r {
	case
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		'A', 'B', 'C', 'D', 'E', 'F',
		'a', 'b', 'c', 'd', 'e', 'f':
		return true
	default:
		return false
	}
}

var Escapes = map[rune]string{
	'a': "\a",   // 07h BEL
	'b': "\b",   // 08h BS
	't': "\t",   // 09h TAB
	'n': "\n",   // 0Ah LF
	'v': "\v",   // 0Bh VT
	'f': "\f",   // 0Ch FF
	'r': "\r",   // 0Dh CR
	'e': "\x1B", // 1Bh ESC
}
