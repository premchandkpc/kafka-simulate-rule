package memory

import "sync"

// slabs is a pool of sized arenas, initialized once at startup.
var slabs = [3]*sync.Pool{
	{
		New: func() any { return &Arena{buf: make([]byte, ArenaSmall)} },
	},
	{
		New: func() any { return &Arena{buf: make([]byte, ArenaMedium)} },
	},
	{
		New: func() any { return &Arena{buf: make([]byte, ArenaLarge)} },
	},
}

// GetArena returns an arena sized for the estimated message body size.
func GetArena(estimatedSize int) *Arena {
	var a *Arena
	switch {
	case estimatedSize <= ArenaSmall:
		a = slabs[0].Get().(*Arena)
		a.pool = slabs[0]
	case estimatedSize <= ArenaMedium:
		a = slabs[1].Get().(*Arena)
		a.pool = slabs[1]
	default:
		a = slabs[2].Get().(*Arena)
		a.pool = slabs[2]
	}
	a.Reset()
	return a
}
