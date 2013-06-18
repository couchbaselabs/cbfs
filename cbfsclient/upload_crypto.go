// +build !nocrypto

package main

import (
	"io"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	_ "crypto/ecdsa"
	_ "crypto/sha1"
	_ "crypto/sha256"
	_ "crypto/sha512"

	"code.google.com/p/go.crypto/openpgp"
)

var encryptKeys openpgp.EntityList

var uploadEncryptTo = uploadFlags.String("encryptTo", "",
	"PGP key to encrypt to.")

func intersectPref(a []uint8, b []uint8) (intersection []uint8) {
	if a == nil {
		return b
	}
	var j int
	for _, v := range a {
		for _, v2 := range b {
			if v == v2 {
				a[j] = v
				j++
				break
			}
		}
	}

	return a[:j]
}

func primaryIdentity(e *openpgp.Entity) *openpgp.Identity {
	var firstIdentity *openpgp.Identity
	for _, ident := range e.Identities {
		if firstIdentity == nil {
			firstIdentity = ident
		}
		if ident.SelfSignature.IsPrimaryId != nil &&
			*ident.SelfSignature.IsPrimaryId {
			return ident
		}
	}
	return firstIdentity
}

func gpgHome() string {
	if gpgh := os.Getenv("GNUPGHOME"); gpgh != "" {
		return gpgh
	}

	u, err := user.Current()
	if err != nil {
		log.Fatalf("Who am I?")
	}

	return filepath.Join(u.HomeDir, ".gnupg")
}

func initCrypto() {
	if *uploadEncryptTo == "" {
		return
	}

	// If encrypting, the hashes are basically not known.
	*uploadNoHash = true

	f, err := os.Open(filepath.Join(gpgHome(), "pubring.gpg"))
	if err != nil {
		log.Fatalf("Can't open keyring: %v", err)
	}
	defer f.Close()

	kl, err := openpgp.ReadKeyRing(f)
	if err != nil {
		log.Fatalf("Can't read keyring: %v", err)
	}

	keyids := strings.Split(*uploadEncryptTo, ",")
	var hprefs, sprefs []uint8

	for _, w := range keyids {
		for _, e := range kl {
			if e.PrimaryKey.KeyIdShortString() == w {
				pi := primaryIdentity(e)
				ss := pi.SelfSignature

				hprefs = intersectPref(hprefs, ss.PreferredHash)
				sprefs = intersectPref(sprefs, ss.PreferredSymmetric)
				encryptKeys = append(encryptKeys, e)
			}
		}
	}

	if len(encryptKeys) != len(keyids) {
		log.Fatalf("Couldn't find all keys")
	}
	if len(hprefs) == 0 {
		log.Fatalf("No common hashes for encryption keys")
	}
	if len(sprefs) == 0 {
		log.Fatalf("No common symmetric ciphers for encryption keys")
	}
}

func maybeCrypt(r io.ReadCloser) io.ReadCloser {
	if len(encryptKeys) == 0 {
		return r
	}

	pr, pw := io.Pipe()
	go func() {
		w, err := openpgp.Encrypt(pw, encryptKeys, nil, nil, nil)
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		_, err = io.Copy(w, r)
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.CloseWithError(w.Close())
	}()
	return pr
}
