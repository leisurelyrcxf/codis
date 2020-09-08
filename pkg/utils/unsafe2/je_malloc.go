// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

// +build cgo_jemalloc

package unsafe2

// #cgo         CPPFLAGS: -I../../../vendor/github.com/spinlock/jemalloc-go/jemalloc-4.4.0/include/
// #cgo  darwin  LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup -L../../../vendor/github.com/spinlock/jemalloc-go/jemalloc-4.4.0/lib -ljemalloc -static
// #cgo !darwin  LDFLAGS: -Wl,-unresolved-symbols=ignore-all -L../../../vendor/github.com/spinlock/jemalloc-go/jemalloc-4.4.0/lib -ljemalloc -static
// #include <jemalloc/jemalloc.h>
import "C"

import "unsafe"

func cgo_malloc(n int) unsafe.Pointer {
	return C.je_malloc(C.size_t(n))
}

func cgo_free(ptr unsafe.Pointer) {
	C.je_free(ptr)
}
