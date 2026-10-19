package store

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestKeysOrderNumerically(t *testing.T) {
	a := Key('M', "t", uint64(0), uint64(9))
	b := Key('M', "t", uint64(0), uint64(10))
	if bytes.Compare(a, b) >= 0 {
		t.Fatal("9 must sort before 10")
	}
}

func TestStringPartsDoNotCollide(t *testing.T) {
	a := Key('S', "ab", "c")
	b := Key('S', "a", "bc")
	if bytes.Equal(a, b) {
		t.Fatal("length-prefixed strings must not collide")
	}
}

func TestBatchPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b := s.NewBatch()
	b.Set([]byte("k"), []byte("v"))
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	v, ok, err := s2.Get([]byte("k"))
	if err != nil || !ok || string(v) != "v" {
		t.Fatalf("lost write: %q %v %v", v, ok, err)
	}
}

func TestScanPrefixInOrder(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	b := s.NewBatch()
	for _, off := range []uint64{3, 1, 2} {
		b.Set(Key('M', "t", uint64(0), off), []byte{byte(off)})
	}
	b.Set(Key('M', "t", uint64(1), uint64(0)), []byte{9}) // other shard, must not appear
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	var got []byte
	err := s.Scan(Prefix('M', "t", uint64(0)), func(k, v []byte) error {
		got = append(got, v[0])
		return nil
	})
	if err != nil || !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("got %v err %v", got, err)
	}
}

func TestScanFromStartsMidRange(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	b := s.NewBatch()
	for off := uint64(0); off < 5; off++ {
		b.Set(Key('M', "t", uint64(0), off), binary.BigEndian.AppendUint64(nil, off))
	}
	b.Commit()
	var n int
	s.ScanFrom(Key('M', "t", uint64(0), uint64(3)), Prefix('M', "t", uint64(0)), func(k, v []byte) error { n++; return nil })
	if n != 2 {
		t.Fatalf("want 2 got %d", n)
	}
}

func TestDeleteRemoves(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	b := s.NewBatch()
	b.Set([]byte("k"), []byte("v"))
	b.Commit()
	b = s.NewBatch()
	b.Delete([]byte("k"))
	b.Commit()
	if _, ok, _ := s.Get([]byte("k")); ok {
		t.Fatal("still present")
	}
}
