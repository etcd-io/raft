package server

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"

	log "github.com/sirupsen/logrus"

	pb "go.etcd.io/raft/v3/raftpb"
)

type raftMsgs []pb.Message

func (msgs raftMsgs) Len() int {
	return len([]pb.Message(msgs))
}

// msgcnt   4 bytes
// len|recoder
// ...
// len|recoder
// crc      4 bytes
func (msgs raftMsgs) Encode(w io.Writer) error {
	crc := crc32.NewIEEE()
	mw := io.MultiWriter(w, crc)
	cnt := uint32(msgs.Len())
	b := make([]byte, 4)

	// write header
	binary.BigEndian.PutUint32(b, cnt)
	if _, err := w.Write(b); err != nil {
		return err
	}

	// write body
	for i := 0; i < msgs.Len(); i++ {
		buf := Alloc(4 + msgs[i].Size())
		binary.BigEndian.PutUint32(buf, uint32(msgs[i].Size()))
		_, err := msgs[i].MarshalTo(buf[4:])
		if err != nil {
			Free(buf)
			return err
		}
		if _, err = mw.Write(buf); err != nil {
			Free(buf)
			return err
		}
		Free(buf)
	}

	// write checksum
	binary.BigEndian.PutUint32(b, crc.Sum32())
	_, err := w.Write(b)
	return err
}

func (msgs raftMsgs) Decode(r io.Reader) (raftMsgs, error) {
	w := crc32.NewIEEE()
	tr := io.TeeReader(r, w)

	b := make([]byte, 4)
	// read msgcnt
	if _, err := io.ReadFull(r, b); err != nil {
		log.Errorf("read head[msgcnt] error: %v", err)
		return nil, err
	}
	cnt := binary.BigEndian.Uint32(b)
	msgs = make([]pb.Message, 0, cnt)
	for i := 0; i < int(cnt); i++ {
		// read recorder len
		if _, err := io.ReadFull(tr, b); err != nil {
			log.Errorf("read the %d's recorder len error: %v", i, err)
			return nil, err
		}
		msglen := binary.BigEndian.Uint32(b)
		data := Alloc(int(msglen))
		// read recorder
		if _, err := io.ReadFull(tr, data); err != nil {
			log.Errorf("read the %d's recorder error: %v", i, err)
			Free(data)
			return nil, err
		}
		var msg pb.Message
		if err := msg.Unmarshal(data); err != nil {
			Free(data)
			return nil, err
		}
		Free(data)
		msgs = append(msgs, msg)
	}
	// read checksum
	if _, err := io.ReadFull(r, b); err != nil {
		log.Errorf("read checksum error: %v", err)
		return nil, err
	}
	if binary.BigEndian.Uint32(b) != w.Sum32() {
		log.Error("checksum not match")
		return nil, ErrInvalidData
	}

	return msgs, nil
}

func normalEntryEncode(id uint64, data []byte) []byte {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, id)
	if err != nil {
		panic(err)
	}
	_, err = buf.Write(data)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func normalEntryDecode(data []byte) (uint64, []byte) {
	id := binary.BigEndian.Uint64(data[0:8])
	return id, data[8:]
}
