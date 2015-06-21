package script

import (
	"testing"
)

const CRLF = "\r\n"
const LF = "\n"
const NUL = "\000"
const BEL = "\a"

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
		testrow{
			Input: `foo` + CRLF + `bar` + CRLF,
			Output: []Token{
				Token{WordToken, `foo`, nil},
				Token{NewlineToken, LF, nil},
				Token{WordToken, `bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `foo bar` + CRLF,
			Output: []Token{
				Token{WordToken, `foo`, nil},
				Token{WhitespaceToken, " ", nil},
				Token{WordToken, `bar`, nil},
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
		testrow{
			Input: `"foo\abar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},

		// Octal escapes
		testrow{
			Input: `"foo\7,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\07,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\0007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + NUL + `7,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},

		// Hex \xHH escapes
		testrow{
			Input: `"foo\x7,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\x07,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\x007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + NUL + `7,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},

		// Hex \uHHHH escapes
		testrow{
			Input: `"foo\u7,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\u07,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\u007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\u0007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\u00007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + NUL + `7,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},

		// Hex \UHHHHHHHH escapes
		testrow{
			Input: `"foo\U7,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\U07,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\U007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\U0007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\U00007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\U000007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\U0000007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\U00000007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + BEL + `,bar`, nil},
				Token{NewlineToken, LF, nil},
			},
		},
		testrow{
			Input: `"foo\U000000007,bar"`,
			Output: []Token{
				Token{WordToken, `foo` + NUL + `7,bar`, nil},
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

		// Errors {{{
		testrow{
			Input: `foo'bar`,
			Output: []Token{
				Token{WordToken, `foobar`, nil},
				Token{ErrorToken, "", ErrUnterminatedSQS},
			},
		},
		testrow{
			Input: `foo"bar`,
			Output: []Token{
				Token{WordToken, `foobar`, nil},
				Token{ErrorToken, "", ErrUnterminatedDQS},
			},
		},
		testrow{
			Input: `foo\`,
			Output: []Token{
				Token{WordToken, `foo`, nil},
				Token{ErrorToken, "", ErrUnterminatedBS},
			},
		},
		testrow{
			Input: `"foo\x"`,
			Output: []Token{
				Token{WordToken, `foo`, nil},
				Token{ErrorToken, "", ErrUnterminatedBS},
			},
		},
		testrow{
			Input: `"foo\u"`,
			Output: []Token{
				Token{WordToken, `foo`, nil},
				Token{ErrorToken, "", ErrUnterminatedBS},
			},
		},
		testrow{
			Input: `"foo\U"`,
			Output: []Token{
				Token{WordToken, `foo`, nil},
				Token{ErrorToken, "", ErrUnterminatedBS},
			},
		},
		// }}}
	} {
		lex := NewLexer([]byte(row.Input))
		tokens := consume(lex.Go())
		if !equal(tokens, row.Output) {
			t.Errorf("[%2d] %#q: expected %#v, got %#v", i, row.Input, row.Output, tokens)
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

func TestLexError_String(t *testing.T) {
	type testrow struct {
		Value          LexError
		ExpectedString string
		ExpectedError  string
	}
	for _, row := range []testrow{
		testrow{ErrUnterminatedSQS,
			"script.ErrUnterminatedSQS",
			"unterminated single-quoted string"},
		testrow{ErrUnterminatedDQS,
			"script.ErrUnterminatedDQS",
			"unterminated double-quoted string"},
		testrow{ErrUnterminatedBS,
			"script.ErrUnterminatedBS",
			"unterminated backslash"},
		testrow{LexError(42),
			"script.LexError(42)",
			"script.LexError(42)"},
	} {
		str := row.Value.String()
		err := row.Value.Error()
		if str != row.ExpectedString {
			t.Errorf("%#v, got String() %q", row, str)
		}
		if err != row.ExpectedError {
			t.Errorf("%#v, got Error() %q", row, err)
		}
	}
}

func TestTokenType_String(t *testing.T) {
	type testrow struct {
		Value          TokenType
		ExpectedString string
	}
	for _, row := range []testrow{
		testrow{ErrorToken, "script.ErrorToken"},
		testrow{WordToken, "script.WordToken"},
		testrow{CommentToken, "script.CommentToken"},
		testrow{WhitespaceToken, "script.WhitespaceToken"},
		testrow{NewlineToken, "script.NewlineToken"},
		testrow{TokenType(42), "script.TokenType(42)"},
	} {
		str := row.Value.String()
		if str != row.ExpectedString {
			t.Errorf("%#v, got String() %q", row, str)
		}
	}
}
