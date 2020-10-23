package jemalloc

import (
	"reflect"
	"testing"
	"unsafe"
)

func toBytes(ptr unsafe.Pointer, size int) []byte {
	p := &reflect.SliceHeader{}
	p.Data = uintptr(ptr)
	p.Len = size
	p.Cap = size
	return *(*[]byte)(unsafe.Pointer(p))
}

func TestMalloc(t *testing.T) {
	p1 := Malloc(100)
	if p1 == nil {
		t.Fatalf("malloc failed")
	}
	b1 := toBytes(p1, 100)
	for i := 0; i < 100; i++ {
		b1[i] = byte(i)
	}

	p2 := Realloc(p1, 200)
	if p2 == nil {
		t.Fatalf("realloc failed")
	}
	b2 := toBytes(p2, 200)
	for i := 0; i < 100; i++ {
		if b2[i] != byte(i) {
			t.Fatalf("realloc failed")
		}
	}
	Free(p2)
}
