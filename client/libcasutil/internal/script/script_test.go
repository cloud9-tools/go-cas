package script // import "github.com/chronos-tachyon/go-cas/client/libcasutil/internal/script"

import (
	"strings"
	"testing"
)

const CRLF = "\r\n"
const LF = "\n"

func TestLex(t *testing.T) {
	type testrow struct {
		Input  string
		Output []Token
	}

	for i, row := range []testrow{
		// Barewords {{{
		testrow{
			Input: ``,
			Output: []Token{
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `foobar`,
			Output: []Token{
				Token{WordToken, `foobar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `foobar` + LF,
			Output: []Token{
				Token{WordToken, `foobar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `foobar` + CRLF + CRLF,
			Output: []Token{
				Token{WordToken, `foobar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `foo\bar`,
			Output: []Token{
				Token{WordToken, `foobar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `foobar\` + LF,
			Output: []Token{
				Token{WordToken, `foobar` + LF, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `foo\'bar\'`,
			Output: []Token{
				Token{WordToken, `foo'bar'`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `foo\"bar\"`,
			Output: []Token{
				Token{WordToken, `foo"bar"`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `foo\#bar`,
			Output: []Token{
				Token{WordToken, `foo#bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		// }}}

		// Single-quoted strings {{{
		testrow{
			Input: `'foobar'`,
			Output: []Token{
				Token{WordToken, `foobar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `'foobar'` + CRLF,
			Output: []Token{
				Token{WordToken, `foobar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `'foobar` + CRLF + `'` + CRLF,
			Output: []Token{
				Token{WordToken, `foobar` + CRLF, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `foo'bar'`,
			Output: []Token{
				Token{WordToken, `foobar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `'foo\xbar'`,
			Output: []Token{
				Token{WordToken, `foo\xbar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `'foo\'bar`,
			Output: []Token{
				Token{WordToken, `foo\bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		// }}}

		// Double-quoted strings {{{
		testrow{
			Input: `"foobar"`,
			Output: []Token{
				Token{WordToken, `foobar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `foo"bar"`,
			Output: []Token{
				Token{WordToken, `foobar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\"bar"`,
			Output: []Token{
				Token{WordToken, `foo"bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\\bar"`,
			Output: []Token{
				Token{WordToken, `foo\bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\nbar"`,
			Output: []Token{
				Token{WordToken, `foo` + LF + `bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		// }}}

		// Comments {{{
		testrow{
			Input: `#bar`,
			Output: []Token{
				Token{CommentToken, `bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `#bar` + LF,
			Output: []Token{
				Token{CommentToken, `bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `foo#bar`,
			Output: []Token{
				Token{WordToken, `foo`, nil},
				Token{CommentToken, `bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		// }}}
	} {
		ch, _ := Lex(strings.NewReader(row.Input))
		tokens := consume(ch)
		if !equal(tokens, row.Output) {
			t.Errorf("[%2d] %q: expected %#v, got %#v", i, row.Input, row.Output, tokens)
		}
	}
}

func consume(ch <-chan Token) []Token {
	var result []Token
	for tok := range ch {
		result = append(result, tok)
	}
	return result
}

func equal(a, b []Token) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
