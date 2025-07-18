package kvservice

import (
	"testing"
)

func checkPutPrev(t *testing.T, ds *Datastore, k string, v string, prev string, hasPrev bool) {
	t.Helper()
	prevVal, ok := ds.Put(k, v)

	if hasPrev != ok || prev != prevVal {
		t.Errorf("prevVal=%s, ok=%v; want %s,%v", prevVal, ok, prev, hasPrev)
	}
}

func checkGet(t *testing.T, ds *Datastore, k string, v string, found bool) {
	t.Helper()
	value, ok := ds.Get(k)

	if value != v || found != ok {
		t.Errorf("gotV=%s, ok=%v; want %s,%v", value, ok, v, found)
	}
}

func checkCAS(t *testing.T, ds *Datastore, k string, comp string, v string, prev string, found bool) {
	t.Helper()

	gotPrev, gotFound := ds.CAS(k, comp, v)

	if gotPrev != prev || gotFound != found {
		t.Errorf("gotPrev=%s, gotFound=%v; want %s,%v", gotPrev, gotFound, prev, found)
	}
}

func TestGetPut(t *testing.T) {
	ds := NewDatastore()

	checkGet(t, ds, "foo", "", false)
	checkPutPrev(t, ds, "foo", "bar", "", false)
	checkGet(t, ds, "foo", "bar", true)
	checkPutPrev(t, ds, "foo", "baz", "bar", true)
	checkGet(t, ds, "foo", "baz", true)
	checkPutPrev(t, ds, "nix", "hard", "", false)
}

func TestCASBasic(t *testing.T) {
	ds := NewDatastore()
	ds.Put("foo", "bar")
	ds.Put("sun", "beam")

	// CAS replace existing value
	checkCAS(t, ds, "foo", "mex", "bro", "bar", true)
	checkCAS(t, ds, "foo", "bar", "bro", "bar", true)
	checkGet(t, ds, "foo", "bro", true)

	// CAS when key not found
	checkCAS(t, ds, "mf", "doom", "doomsday", "", false)
	checkGet(t, ds, "mf", "", false)

	ds.Put("mf", "fantastic")
	checkCAS(t, ds, "mf", "fantastic", "doom", "fantastic", true)
	checkGet(t, ds, "mf", "doom", true)
}

func TestCASConcurrent(t *testing.T) {
	ds := NewDatastore()
	ds.Put("Scipio", "Africanus")

	go func() {
		for range 2000 {
			ds.Put("Scipio", "Africanus")
		}
	}()

	go func() {
		for range 2000 {
			ds.Put("Scipio", "Aemilianus")
		}
	}()

	v, _ := ds.Get("Scipio")

	if v != "Africanus" && v != "Aemilianus" {
		t.Errorf("got v=%s, want Africanus or Aemilianus", v)
	}

}
