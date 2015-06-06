package script // import "github.com/chronos-tachyon/go-cas/libcasutil/internal/script"

import (
	"strings"
	"testing"
)

func TestLex(t *testing.T) {
	type testrow struct {
		Input  string
		Output []Token
	}

	for i, row := range []testrow{
		testrow{
			Input: "",
			Output: []Token{
				Token{NewlineToken, "\n", nil},
			},
		},

		testrow{
			Input: "foobar",
			Output: []Token{
				Token{WordToken, "foobar", nil},
				Token{NewlineToken, "\n", nil},
			},
		},
		testrow{
			Input: "foobar\n",
			Output: []Token{
				Token{WordToken, "foobar", nil},
				Token{NewlineToken, "\n", nil},
			},
		},
		testrow{
			Input: "foobar\r\n\r\n",
			Output: []Token{
				Token{WordToken, "foobar", nil},
				Token{NewlineToken, "\n", nil},
			},
		},

		testrow{
			Input: "'foobar\r\n'\r\n",
			Output: []Token{
				Token{WordToken, "foobar\r\n", nil},
				Token{NewlineToken, "\n", nil},
			},
		},
		testrow{
			Input: "foo'bar'\n",
			Output: []Token{
				Token{WordToken, "foobar", nil},
				Token{NewlineToken, "\n", nil},
			},
		},
		testrow{
			Input: "foo'b\\ar'\n",
			Output: []Token{
				Token{WordToken, "foob\\ar", nil},
				Token{NewlineToken, "\n", nil},
			},
		},
		testrow{
			Input: "foo'b\\\\ar'\n",
			Output: []Token{
				Token{WordToken, "foob\\\\ar", nil},
				Token{NewlineToken, "\n", nil},
			},
		},
		testrow{
			Input: "foo'bar\\'\n",
			Output: []Token{
				Token{WordToken, "foobar\\", nil},
				Token{NewlineToken, "\n", nil},
			},
		},

		testrow{
			Input: "foo\"bar\"\n",
			Output: []Token{
				Token{WordToken, "foobar", nil},
				Token{NewlineToken, "\n", nil},
			},
		},

		testrow{
			Input: "#bar",
			Output: []Token{
				Token{CommentToken, "bar", nil},
				Token{NewlineToken, "\n", nil},
			},
		},
		testrow{
			Input: "#bar\n",
			Output: []Token{
				Token{CommentToken, "bar", nil},
				Token{NewlineToken, "\n", nil},
			},
		},
		testrow{
			Input: "foo#bar",
			Output: []Token{
				Token{WordToken, "foo", nil},
				Token{CommentToken, "bar", nil},
				Token{NewlineToken, "\n", nil},
			},
		},

		testrow{
			Input: "foobar\\\n",
			Output: []Token{
				Token{WordToken, "foobar\n", nil},
				Token{NewlineToken, "\n", nil},
			},
		},
		testrow{
			Input: "foob\\ar\n",
			Output: []Token{
				Token{WordToken, "foobar", nil},
				Token{NewlineToken, "\n", nil},
			},
		},
		testrow{
			Input: "foo\\#bar",
			Output: []Token{
				Token{WordToken, "foo#bar", nil},
				Token{NewlineToken, "\n", nil},
			},
		},
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
