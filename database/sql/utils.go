package sql

import (
	"bytes"
)

func escapeLikeCharacter(pattern []byte, escapeCharacter []byte, escape []byte) []byte {
	escapedCharacter := append(escape, escapeCharacter[0])
	return bytes.ReplaceAll(pattern, escapeCharacter, escapedCharacter)
}

// Escapes the wildcard characters allowed in a SQL LIKE `pattern`.
// `escapeCharacter` is the actual escape character to be used, it is a []byte but it's expected
// to be 1 byte long.
func EscapeLikeWildcardCharacters(pattern []byte, escapeCharacter []byte) []byte {
	s := pattern
	s = escapeLikeCharacter(s, escapeCharacter, escapeCharacter)
	s = escapeLikeCharacter(s, []byte("%"), escapeCharacter)
	s = escapeLikeCharacter(s, []byte("_"), escapeCharacter)
	return s
}

// Return a prefix pattern that can be used with LIKE.
// The pattern will match text/blob fields starting with `prefix`.
func PrefixPattern(prefix []byte, escapeCharacter []byte) []byte {
	return append(EscapeLikeWildcardCharacters(prefix, escapeCharacter), []byte("%")[0])
}
