package chainhash

import (
	"fmt"
	"sort"
	"testing"
)

// TestHashFuncs ensures the hash functions which perform hash(b) work as
// expected.
func TestHashSorter(t *testing.T) {
	var inputHashes []*Hash
	inputStrings := []string{"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
		"fb8e20fc2e4c3f248c60c39bd652f3c1347298bb977b8b4d5903b85055620603",
		"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
		"36bbe50ed96841d10443bcb670d6554f0a34b761be67ec9c4a8ad2c0c44ca42c",
		"bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721",
		"7d1a54127b222502f5b79b5fb0803061152a44f92b37e23c6527baf665d4da9a",
		"9c56cc51b374c3ba189210d5b6d4bf57790d351c96c47c02190ecf1e430635ab",
		"19cc02f26df43cc571bc9ed7b0c4d29224a3ec229529221725ef76d021c8326f",
		"72399361da6a7754fec986dca5b7cbaf1c810a28ded4abaf56b2106d06cb78b0"}

	for _, s := range inputStrings {
		h, _ := NewHashFromStr(s)
		inputHashes = append(inputHashes, h)
	}

	sortedHashes := []string{"19cc02f26df43cc571bc9ed7b0c4d29224a3ec229529221725ef76d021c8326f",
		"36bbe50ed96841d10443bcb670d6554f0a34b761be67ec9c4a8ad2c0c44ca42c",
		"72399361da6a7754fec986dca5b7cbaf1c810a28ded4abaf56b2106d06cb78b0",
		"7d1a54127b222502f5b79b5fb0803061152a44f92b37e23c6527baf665d4da9a",
		"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
		"9c56cc51b374c3ba189210d5b6d4bf57790d351c96c47c02190ecf1e430635ab",
		"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		"bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721",
		"ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"fb8e20fc2e4c3f248c60c39bd652f3c1347298bb977b8b4d5903b85055620603"}

	sort.Sort(HashSorter(inputHashes))

	// Ensure the hash function which returns a Hash returns the expected
	// result.
	for i := range inputHashes {
		h := fmt.Sprintf("%v", inputHashes[i])
		if h != sortedHashes[i] {
			t.Errorf("Want %v, got %v", sortedHashes[i], inputHashes[i])
			continue
		}
	}
}
