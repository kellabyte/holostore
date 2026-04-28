package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
)

// Checksummed values should parse into stable, named fields.
func TestParseChecksummedValue(t *testing.T) {
	value := "scenario=baseline;key=antithesis_shared_k0;client=3;seq=9;checksum=0f0a"
	parsed, err := parseChecksummedValue(value)
	if err != nil {
		t.Fatalf("parseChecksummedValue returned error: %v", err)
	}
	if parsed.Scenario != "baseline" || parsed.Key != "antithesis_shared_k0" || parsed.Client != "3" || parsed.Seq != "9" || parsed.Checksum != "0f0a" {
		t.Fatalf("unexpected parse result: %#v", parsed)
	}
}

// Key-bound checksum validation must accept valid payloads.
func TestValidateChecksummedValue(t *testing.T) {
	prefix := "scenario=hot_key;key=antithesis_shared_k0;client=6;seq=11"
	digest := sha256.Sum256([]byte(prefix))
	value := fmt.Sprintf("%s;checksum=%s", prefix, hex.EncodeToString(digest[:]))
	if err := validateChecksummedValue(value, "antithesis_shared_k0"); err != nil {
		t.Fatalf("validateChecksummedValue returned error: %v", err)
	}
}

// Key binding errors should fail before linearizability checks run.
func TestValidateChecksummedValueRejectsWrongKey(t *testing.T) {
	prefix := "scenario=hot_key;key=antithesis_shared_k0;client=6;seq=11"
	digest := sha256.Sum256([]byte(prefix))
	value := fmt.Sprintf("%s;checksum=%s", prefix, hex.EncodeToString(digest[:]))
	if err := validateChecksummedValue(value, "antithesis_shared_k1"); err == nil {
		t.Fatal("expected key mismatch to fail")
	}
}
