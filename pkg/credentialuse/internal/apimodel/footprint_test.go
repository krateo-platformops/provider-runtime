package apimodel

import "testing"

func TestFootprintConflicts(t *testing.T) {
	k1, k2 := Object{ObjModelKey, "a"}, Object{ObjModelKey, "b"}
	clock := Object{Kind: ObjClock}
	fp := func(pairs ...any) Footprint {
		f := Footprint{}
		for i := 0; i < len(pairs); i += 2 {
			f.Add(pairs[i].(Object), pairs[i+1].(Mode))
		}
		return f
	}
	for name, tc := range map[string]struct {
		a, b Footprint
		want bool
	}{
		"two reads commute":                     {fp(k1, ModeRead), fp(k1, ModeRead), false},
		"a write and a read conflict":           {fp(k1, ModeWrite), fp(k1, ModeRead), true},
		"two writes conflict":                   {fp(k1, ModeWrite), fp(k1, ModeWrite), true},
		"different keys commute":                {fp(k1, ModeWrite), fp(k2, ModeWrite), false},
		"two ticks commute":                     {fp(clock, ModeTick), fp(clock, ModeTick), false},
		"a tick and a clock read conflict":      {fp(clock, ModeTick), fp(clock, ModeRead), true},
		"a tick and a deadline change conflict": {fp(clock, ModeTick), fp(clock, ModeWrite), true},
		"empty commutes":                        {Footprint{}, fp(k1, ModeWrite), false},
	} {
		if got := tc.a.Conflicts(tc.b); got != tc.want {
			t.Errorf("%s: %v.Conflicts(%v) = %v", name, tc.a, tc.b, got)
		}
		if got := tc.b.Conflicts(tc.a); got != tc.want {
			t.Errorf("%s, swapped: %v", name, got)
		}
	}
	u := fp(k1, ModeRead|ModeWrite, clock, ModeTick)
	if !fp(k1, ModeRead).Within(u) || fp(k2, ModeRead).Within(u) || fp(clock, ModeRead).Within(u) {
		t.Error("Within")
	}
}

// The recording context is a stack: the innermost scope decides, and off
// scopes nested in on scopes record nothing.
func TestRecordingScopes(t *testing.T) {
	s := newSched(Bounds{}, nil, nil, nil)
	k := Object{ObjModelKey, "a"}
	s.Read(k) // no scope: nothing
	body := Footprint{}
	s.RecordInto(body)
	s.Read(k)
	s.RecordOff()
	s.Write(Object{ObjModelKey, "instrumentation"})
	s.EndScope()
	s.Tick()
	s.EndScope()
	if s.RecordingScopes() != 0 {
		t.Fatalf("%d scopes open", s.RecordingScopes())
	}
	want := Footprint{k: ModeRead, Object{Kind: ObjClock}: ModeTick}
	if body.String() != want.String() {
		t.Errorf("recorded %v, want %v", body, want)
	}
}
