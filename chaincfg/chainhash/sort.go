package chainhash

//HashSorter implements sort.Interface to allow hashes to be sorted
// in lexicographical order
type HashSorter []*Hash

// Len returns the number of txs in the slice.  It is part of the
// sort.Interface implementation.
func (s HashSorter) Len() int {
	return len(s)
}

// Swap swaps the hashes at the passed indices.  It is part of the
// sort.Interface implementation.
func (s HashSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less returns whether the hash with index i should sort before the
// hash with index j.  It is part of the sort.Interface implementation.
func (s HashSorter) Less(i, j int) bool {
	return s[i].Compare(s[j]) < 0
}
