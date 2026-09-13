package auth

import (
	"testing"
	"time"
)

func TestHashIsSaltedAndDeterministic(t *testing.T) {
	s1, _ := NewSalt()
	s2, _ := NewSalt()
	if string(s1) == string(s2) {
		t.Fatal("salts must differ")
	}
	if string(HashAPIKey(s1, "sk-abc")) != string(HashAPIKey(s1, "sk-abc")) {
		t.Error("same salt and key must hash identically")
	}
	if string(HashAPIKey(s1, "sk-abc")) == string(HashAPIKey(s2, "sk-abc")) {
		t.Error("different salts must produce different hashes")
	}
}

func TestAuthenticate(t *testing.T) {
	store := NewStore()
	salt, _ := NewSalt()
	store.Put(KeyRecord{ID: "k1", Subject: "alice", Salt: salt, Hash: HashAPIKey(salt, "sk-good"), Status: StatusActive})

	if p, err := store.Authenticate("sk-good", time.Now()); err != nil || p.SubjectID != "alice" || p.KeyID != "k1" {
		t.Errorf("valid key: p = %+v, err = %v", p, err)
	}
	if _, err := store.Authenticate("sk-bad", time.Now()); err != ErrInvalid {
		t.Errorf("invalid key err = %v", err)
	}
}
