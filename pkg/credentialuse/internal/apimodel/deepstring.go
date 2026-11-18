package apimodel

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

var timeType = reflect.TypeOf(time.Time{})

// deepString renders v through pointers, interfaces, maps, slices and
// structs, so that two renderings differ whenever anything reachable from v
// differs: C-park's runtime comparison of an actor's locals before and after
// its condition. reflect.DeepEqual can't serve, as a local that is a pointer
// or a map is the same value before and after a write through it.
func deepString(v any) string {
	var b strings.Builder
	render(&b, reflect.ValueOf(v), map[uintptr]bool{})
	return b.String()
}

func render(b *strings.Builder, v reflect.Value, seen map[uintptr]bool) {
	if !v.IsValid() {
		b.WriteString("<nil>")
		return
	}
	if v.Type() == timeType && v.CanInterface() {
		fmt.Fprintf(b, "time(%d)", v.Interface().(time.Time).UnixNano())
		return
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			b.WriteString("nil")
			return
		}
		if seen[v.Pointer()] {
			b.WriteString("<cycle>")
			return
		}
		seen[v.Pointer()] = true
		b.WriteString("&")
		render(b, v.Elem(), seen)
		delete(seen, v.Pointer())
	case reflect.Interface:
		if v.IsNil() {
			b.WriteString("nil")
			return
		}
		render(b, v.Elem(), seen)
	case reflect.Map:
		if v.IsNil() {
			b.WriteString("map(nil)")
			return
		}
		type kv struct{ k, v string }
		var kvs []kv
		for it := v.MapRange(); it.Next(); {
			var kb, vb strings.Builder
			render(&kb, it.Key(), seen)
			render(&vb, it.Value(), seen)
			kvs = append(kvs, kv{kb.String(), vb.String()})
		}
		sort.Slice(kvs, func(i, j int) bool { return kvs[i].k < kvs[j].k })
		b.WriteString("map[")
		for _, e := range kvs {
			b.WriteString(e.k + ":" + e.v + " ")
		}
		b.WriteString("]")
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			b.WriteString("[](nil)")
			return
		}
		b.WriteString("[")
		for i := range v.Len() {
			render(b, v.Index(i), seen)
			b.WriteString(" ")
		}
		b.WriteString("]")
	case reflect.Struct:
		b.WriteString(v.Type().String() + "{")
		for i := range v.NumField() {
			render(b, v.Field(i), seen)
			b.WriteString(" ")
		}
		b.WriteString("}")
	case reflect.Bool:
		fmt.Fprint(b, v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fmt.Fprint(b, v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		fmt.Fprint(b, v.Uint())
	case reflect.Float32, reflect.Float64:
		fmt.Fprint(b, v.Float())
	case reflect.Complex64, reflect.Complex128:
		fmt.Fprint(b, v.Complex())
	case reflect.String:
		fmt.Fprintf(b, "%q", v.String())
	default:
		// Funcs, channels: their identity.
		fmt.Fprintf(b, "%s@%x", v.Kind(), v.Pointer())
	}
}
