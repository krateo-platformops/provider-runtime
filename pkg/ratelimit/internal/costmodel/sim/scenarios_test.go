package sim

import "time"

// scenarios are the §10 CPU cases' configurations, by name, for a seed, for
// TestExplore.
func scenarios() map[string]func(seed uint64) Config {
	base := Class{CPU: 23 * time.Millisecond, Wait: 400 * time.Millisecond, Jitter: 0.1}
	return map[string]func(uint64) Config{
		"headline": func(seed uint64) Config { return measured(seed) },
		"costdoubles": func(seed uint64) Config {
			c := measured(seed)
			c.Duration = 150 * time.Second
			heavy := base
			heavy.CPU = 46 * time.Millisecond
			c.Queues[0].Class = classAfter(90*time.Second, base, heavy)
			return c
		},
		"cost46": func(seed uint64) Config {
			c := measured(seed)
			heavy := base
			heavy.CPU = 46 * time.Millisecond
			c.Queues[0].Class = fixedClass(heavy)
			return c
		},
		"limithalves": func(seed uint64) Config {
			c := measured(seed)
			c.Duration = 150 * time.Second
			c.LimitAt = limitAfter(90*time.Second, 0.25, 0.125)
			return c
		},
		"h1biased": func(seed uint64) Config {
			return Config{
				Seed: seed, Duration: 60 * time.Second, Procs: 2, NodeCPUs: 4, Limit: 0.5, Ceiling: 16,
				Background: burstyBackground(seed, 0.015, 0.060, 0.3),
				Queues: []Queue{{Name: "q", Class: fixedClass(Class{CPU: 3 * time.Millisecond, Wait: 800 * time.Millisecond, Jitter: 0.1}),
					Active: after(15 * time.Second)}},
				Model: defaultModel(),
			}
		},
		"h1bursts": func(seed uint64) Config {
			c := measured(seed)
			c.Background = burstyBackground(seed, 0.020, 0.100, 0.3)
			c.Queues[0].Active = after(15 * time.Second)
			return c
		},
		"dip":       func(seed uint64) Config { return blipNode(seed, 60*time.Second, 75*time.Second) },
		"nodeshare": func(seed uint64) Config { return nodeShare(seed) },
		"mixseen": func(seed uint64) Config {
			c, _ := mixShift(seed, true)
			return c
		},
		"limit125": func(seed uint64) Config {
			c := measured(seed)
			c.Limit = 0.125
			return c
		},
	}
}
