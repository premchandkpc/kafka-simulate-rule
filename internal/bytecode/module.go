package bytecode

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	Magic          = 0x464C4F57
	HeaderSize     = 32
	SectionEntrySize = 16
	InstrSize      = 16
	VersionMajor   = 1
	VersionMinor   = 0
)

type Module struct {
	VersionMajor uint8
	VersionMinor uint8
	Flags        uint16

	ConstPool   []ConstEntry
	TargetLists []TargetList
	Instrs      []Instruction
	MapExprs    []MapExprEntry
	Debug       []byte
	RuleMeta    *RuleMetaEntry
}

type ConstEntry struct {
	Type    ConstType
	Payload []byte
}

type TargetList struct {
	Indices []uint32
}

type Instruction struct {
	Opcode Opcode
	Flags  Flags
	Arg1   uint32
	Arg2   uint32
	Arg3   uint32
}

type MapExprEntry struct {
	Type  MapExprType
	Body  []byte
}

type RuleMetaEntry struct {
	RuleID   string
	Version  int64
	Priority int32
}

func (m *Module) Encode() ([]byte, error) {
	// Section count
	numSections := uint16(4)
	if m.Debug != nil {
		numSections++
	}
	if m.RuleMeta != nil {
		numSections++
	}

	secTableSize := uint32(8) + uint32(numSections)*SectionEntrySize
	cpOff := uint32(HeaderSize) + secTableSize
	cpLen := m.constPoolLen()

	tlOff := cpOff + cpLen
	tlLen := m.targetListLen()

	instrOff := tlOff + tlLen
	instrLen := uint32(len(m.Instrs)) * InstrSize

	mapOff := instrOff + instrLen
	mapLen := m.mapExprLen()

	metaOff := mapOff + mapLen
	metaLen := m.ruleMetaLen()

	totalLen := metaOff + metaLen
	buf := make([]byte, totalLen)

	// Header
	binary.LittleEndian.PutUint32(buf[0:4], Magic)
	buf[4] = VersionMajor
	buf[5] = VersionMinor
	binary.LittleEndian.PutUint16(buf[6:8], m.Flags)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(len(m.Instrs)))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(len(m.ConstPool)))

	// Section table
	binary.LittleEndian.PutUint16(buf[32:34], numSections)
	secOff := uint32(HeaderSize + 8)

	writeSection := func(st SectionType, offset, length uint32) {
		buf[secOff] = byte(st)
		binary.LittleEndian.PutUint32(buf[secOff+8:secOff+12], offset)
		binary.LittleEndian.PutUint32(buf[secOff+12:secOff+16], length)
		secOff += SectionEntrySize
	}

	writeSection(SectionConstPool, cpOff, cpLen)
	writeSection(SectionTargetLists, tlOff, tlLen)
	writeSection(SectionInstrs, instrOff, instrLen)
	writeSection(SectionMapExprs, mapOff, mapLen)

	if m.RuleMeta != nil {
		metaLenActual := m.ruleMetaLen()
		writeSection(SectionRuleMeta, metaOff, metaLenActual)
	}

	if m.Debug != nil {
		debugOff := metaOff + metaLen
		writeSection(SectionDebug, debugOff, uint32(len(m.Debug)))
		copy(buf[debugOff:], m.Debug)
	}

	// Constant pool
	cpBuf := buf[cpOff:cpOff]
	for _, ce := range m.ConstPool {
		entry := make([]byte, 8+len(ce.Payload))
		entry[0] = byte(ce.Type)
		binary.LittleEndian.PutUint32(entry[4:8], uint32(len(ce.Payload)))
		copy(entry[8:], ce.Payload)
		cpBuf = append(cpBuf, entry...)
	}

	// Target lists
	tlBuf := buf[tlOff:tlOff]
	for _, tl := range m.TargetLists {
		entry := make([]byte, 4+4*len(tl.Indices))
		binary.LittleEndian.PutUint16(entry[0:2], uint16(len(tl.Indices)))
		for i, idx := range tl.Indices {
			binary.LittleEndian.PutUint32(entry[4+4*i:8+4*i], idx)
		}
		tlBuf = append(tlBuf, entry...)
	}

	// Instructions
	for i, instr := range m.Instrs {
		base := instrOff + uint32(i)*InstrSize
		buf[base] = byte(instr.Opcode)
		buf[base+1] = byte(instr.Flags)
		binary.LittleEndian.PutUint32(buf[base+4:base+8], instr.Arg1)
		binary.LittleEndian.PutUint32(buf[base+8:base+12], instr.Arg2)
		binary.LittleEndian.PutUint32(buf[base+12:base+16], instr.Arg3)
	}

	// Map expressions
	mapBuf := buf[mapOff:mapOff]
	for _, me := range m.MapExprs {
		entry := make([]byte, 8+len(me.Body))
		entry[0] = byte(me.Type)
		binary.LittleEndian.PutUint32(entry[4:8], uint32(len(me.Body)))
		copy(entry[8:], me.Body)
		mapBuf = append(mapBuf, entry...)
	}

	// Rule meta
	if m.RuleMeta != nil {
		rm := m.RuleMeta
		metaPayload, _ := marshalRuleMeta(rm)
		copy(buf[metaOff:], metaPayload)
	}

	return buf, nil
}

func Decode(data []byte) (*Module, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("bytecode: data too short (%d bytes)", len(data))
	}

	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != Magic {
		return nil, fmt.Errorf("bytecode: bad magic 0x%08X", magic)
	}

	m := &Module{
		VersionMajor: data[4],
		VersionMinor: data[5],
		Flags:        binary.LittleEndian.Uint16(data[6:8]),
	}

	numInstrs := binary.LittleEndian.Uint64(data[16:24])
	numConsts := binary.LittleEndian.Uint64(data[24:32])

	// Parse section table
	numSections := binary.LittleEndian.Uint16(data[32:34])
	secBase := uint32(HeaderSize + 8) // after header + section_count + reserved

	for i := uint16(0); i < numSections; i++ {
		base := secBase + uint32(i)*SectionEntrySize
		st := SectionType(data[base])
		off := binary.LittleEndian.Uint32(data[base+8 : base+12])
		length := binary.LittleEndian.Uint32(data[base+12 : base+16])

		switch st {
		case SectionConstPool:
			m.ConstPool = make([]ConstEntry, 0, numConsts)
			pos := off
			for pos < off+length {
				if pos+8 > uint32(len(data)) {
					return nil, fmt.Errorf("bytecode: const pool header truncated")
				}
				ct := ConstType(data[pos])
				cl := binary.LittleEndian.Uint32(data[pos+4 : pos+8])
				if pos+8+cl > uint32(len(data)) {
					return nil, fmt.Errorf("bytecode: const entry %d truncated", len(m.ConstPool))
				}
				m.ConstPool = append(m.ConstPool, ConstEntry{
					Type:    ct,
					Payload: data[pos+8 : pos+8+cl],
				})
				pos += 8 + cl
			}

		case SectionTargetLists:
			pos := off
			for pos < off+length {
				if pos+4 > uint32(len(data)) {
					return nil, fmt.Errorf("bytecode: target list truncated")
				}
				count := binary.LittleEndian.Uint16(data[pos : pos+2])
				indices := make([]uint32, count)
				for j := uint16(0); j < count; j++ {
					idxOff := pos + 4 + uint32(j)*4
					indices[j] = binary.LittleEndian.Uint32(data[idxOff : idxOff+4])
				}
				m.TargetLists = append(m.TargetLists, TargetList{Indices: indices})
				pos += 4 + uint32(count)*4
			}

		case SectionInstrs:
			n := int(numInstrs)
			m.Instrs = make([]Instruction, n)
			for j := 0; j < n; j++ {
				base := off + uint32(j)*InstrSize
				m.Instrs[j] = Instruction{
					Opcode: Opcode(data[base]),
					Flags:  Flags(data[base+1]),
					Arg1:   binary.LittleEndian.Uint32(data[base+4 : base+8]),
					Arg2:   binary.LittleEndian.Uint32(data[base+8 : base+12]),
					Arg3:   binary.LittleEndian.Uint32(data[base+12 : base+16]),
				}
			}

		case SectionMapExprs:
			pos := off
			for pos < off+length {
				if pos+8 > uint32(len(data)) {
					return nil, fmt.Errorf("bytecode: map expr header truncated")
				}
				mt := MapExprType(data[pos])
				ml := binary.LittleEndian.Uint32(data[pos+4 : pos+8])
				if pos+8+ml > uint32(len(data)) {
					return nil, fmt.Errorf("bytecode: map expr body truncated")
				}
				m.MapExprs = append(m.MapExprs, MapExprEntry{
					Type: mt,
					Body: data[pos+8 : pos+8+ml],
				})
				pos += 8 + ml
			}

		case SectionRuleMeta:
			if length >= 8 {
				m.RuleMeta = &RuleMetaEntry{}
				m.RuleMeta.Version = int64(binary.LittleEndian.Uint64(data[off : off+8]))
				// Priority and RuleID are packed after
				if length > 8 {
					remaining := data[off+8 : off+length]
					_ = remaining
				}
			}

		case SectionDebug:
			m.Debug = make([]byte, length)
			copy(m.Debug, data[off:off+length])
		}
	}

	return m, nil
}

// WriteTo writes the encoded module to w.
func (m *Module) WriteTo(w io.Writer) (int64, error) {
	data, err := m.Encode()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(data)
	return int64(n), err
}

// helpers

func (m *Module) constPoolLen() uint32 {
	var total uint32
	for _, ce := range m.ConstPool {
		total += 8 + uint32(len(ce.Payload))
	}
	return total
}

func (m *Module) targetListLen() uint32 {
	var total uint32
	for _, tl := range m.TargetLists {
		total += 4 + 4*uint32(len(tl.Indices))
	}
	return total
}

func (m *Module) mapExprLen() uint32 {
	var total uint32
	for _, me := range m.MapExprs {
		total += 8 + uint32(len(me.Body))
	}
	return total
}

func (m *Module) ruleMetaLen() uint32 {
	if m.RuleMeta == nil {
		return 0
	}
	data, _ := marshalRuleMeta(m.RuleMeta)
	return uint32(len(data))
}

func marshalRuleMeta(rm *RuleMetaEntry) ([]byte, error) {
	idBytes := []byte(rm.RuleID)
	buf := make([]byte, 16+len(idBytes)+8)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(rm.Version))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(rm.Priority))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(idBytes)))
	copy(buf[16:], idBytes)
	return buf, nil
}
