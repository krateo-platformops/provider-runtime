package credentialuse

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// batchRecords are the holders' records as one release batch reads them: each
// is parsed once, however many of the batch's objects it is asked about, by R0
// and by R2, and one that can't be parsed too (§6.2). Nothing of it outlives the
// batch, which is one goroutine's.
//
// What a record names is a function of its text alone, so the text is the key;
// a holder's version is looked up first, which is cheaper than its text. A
// record's IDs aren't kept, but one flag for each object of the batch: its
// thousands of names, in each of as many holders, would be kept for the batch.
type batchRecords struct {
	read            func(metav1.Object) Record
	ownerFinalizers []string
	place           map[ID]int
	byHolder        map[*metav1.PartialObjectMetadata]recordNames
	byText          map[string]recordNames
}

// recordNames is what a record names of a batch's objects: all of them when it
// can't be parsed, which names every object (§4).
type recordNames struct {
	all bool
	ids []bool // by place
}

// newBatchRecords is for a batch of ids.
func (p *protocol) newBatchRecords(ids []ID) *batchRecords {
	b := &batchRecords{
		read:            p.readRecord,
		ownerFinalizers: p.ownerFinalizers,
		place:           make(map[ID]int, len(ids)),
		byHolder:        map[*metav1.PartialObjectMetadata]recordNames{},
		byText:          map[string]recordNames{},
	}
	if b.read == nil {
		b.read = ReadRecord
	}
	for _, id := range ids {
		if _, again := b.place[id]; !again {
			b.place[id] = len(b.place)
		}
	}
	return b
}

func (b *batchRecords) of(h *metav1.PartialObjectMetadata) recordNames {
	if n, known := b.byHolder[h]; known {
		return n
	}
	text, present := h.Annotations[AnnotationRefs]
	n, known := b.byText[text]
	if !present {
		n = recordNames{} // no record names nothing; an empty one can't be parsed
	} else if !known {
		r := b.read(h)
		n.all = r.Unparseable
		for _, id := range r.IDs {
			if i, ours := b.place[id]; ours {
				if n.ids == nil {
					n.ids = make([]bool, len(b.place))
				}
				n.ids[i] = true
			}
		}
		b.byText[text] = n
	}
	b.byHolder[h] = n
	return n
}

// isUser is IsUser, of an object of the batch. Any other counts as named, as
// one that a record which can't be parsed may name does.
func (b *batchRecords) isUser(h *metav1.PartialObjectMetadata, id ID) bool {
	if notAUser(h, b.ownerFinalizers) {
		return false
	}
	n := b.of(h)
	i, ours := b.place[id]
	return n.all || !ours || n.ids != nil && n.ids[i]
}

// isDeletingUser is IsDeletingUser, of an object of the batch.
func (b *batchRecords) isDeletingUser(h *metav1.PartialObjectMetadata, id ID) bool {
	return h.DeletionTimestamp != nil && b.isUser(h, id)
}
