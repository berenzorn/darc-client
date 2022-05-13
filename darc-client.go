package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/itchio/lzma"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

const BUFFER = 512000
const NAMESIZE = 64

type Block struct {
	Timestamp int64  // 8 bytes
	ID        int64  // 8 bytes
	Name      string // :gap
	Body      []byte // gap:
}

type Routine struct {
	Name string
	End  chan bool
}

type Sender struct {
	Routine
}
type Worker struct {
	Routine
}
type Controller struct {
	Routine
}
type Connector struct {
	Routine
}

func check(msg string, err error) {
	if err != nil {
		log.Fatalf(msg, err)
	}
}

func serialize(block Block) (array []byte, err error) {
	stamp := make([]byte, 8)
	ID := make([]byte, 8)
	name := make([]byte, NAMESIZE-len(block.Name), NAMESIZE)
	name = append(name, []byte(block.Name)...)
	binary.BigEndian.PutUint64(stamp, uint64(block.Timestamp))
	binary.BigEndian.PutUint64(ID, uint64(block.ID))
	array = append(array, block.Body...)
	array = append(array, stamp...)
	array = append(array, ID...)
	array = append(array, name...)
	return array, nil
}

func deserialize(array []byte) (b Block, err error) {
	if len(array) < (NAMESIZE + 16) {
		return Block{}, errors.New("wrong block format")
	}
	gap := len(array) - (NAMESIZE + 16)
	b.Body = array[:gap]
	b.Timestamp = int64(binary.BigEndian.Uint64(array[gap : gap+8]))
	b.ID = int64(binary.BigEndian.Uint64(array[gap+8 : gap+16]))
	b.Name = string(bytes.Trim(array[gap+16:], "\x00"))
	return b, nil
}

func (r *Routine) Controller(conn net.Conn, bc chan Block, rw Worker, rs Sender, reconn chan bool) {
	for {
		select {
		case <-r.End:
			rw.End <- true
			rs.End <- true
			reconn <- true
			return
		default:
			mdlen := make([]byte, 20)
			buff := make([]byte, BUFFER+(NAMESIZE+16))
			_, err := conn.Write([]byte("\xFF"))
			check("Conn send FF", err)
			n, err := conn.Read(mdlen)
			_, err = conn.Write([]byte("\xAC"))
			n, err = conn.Read(buff)
			if err != nil {
				if err == io.EOF {
					rw.End <- true
					rs.End <- true
					reconn <- true
					return
				} else {
					check("CONTROLLER: Err not EOF", err)
				}
			}
			buff = buff[:n]
			md := mdlen[:16]
			length := int32(binary.BigEndian.Uint32(mdlen[16:]))
			if length != int32(n) {
				break
			} else {
				md16 := md5.Sum(buff)
				mdbuff := md16[:]
				if strings.Compare(string(md), string(mdbuff)) != 0 {
					break
				}
			}
			block, err := deserialize(buff)
			if err != nil {
				fmt.Println(err)
			}
			bc <- block
		}
	}
}

func (r *Routine) Worker(bc chan Block, out chan Block) {
	for {
		select {
		case <-r.End:
			return
		case block := <-bc:
			var b bytes.Buffer
			w := lzma.NewWriterSizeLevel(&b, -1, 9)
			_, err := w.Write(block.Body)
			check("Lzma writer problem", err)
			_ = w.Close()
			block.Body = nil
			block.Body = b.Bytes()
			out <- block
		}
	}
}

func (r *Routine) Sender(conn net.Conn, out chan Block, rc Controller) {
	for {
		select {
		case <-r.End:
			return
		case send := <-out:
			array, _ := serialize(send)
			md16 := md5.Sum(array)
			md := md16[:]
			mdbuf := make([]byte, 0, 20)
			length := make([]byte, 4)
			binary.BigEndian.PutUint32(length, uint32(len(array)))
			mdbuf = append(mdbuf, md...)
			mdbuf = append(mdbuf, length...)
			_, err := conn.Write(mdbuf)
			_, err = conn.Write(array)
			for {
				syn := make([]byte, 1)
				_, _ = conn.Read(syn)
				switch syn[0] {
				case '\xAC':
					goto F
				case '\xFF':
					_, err = conn.Write(array)
				}
			}
		F:
			if err != nil {
				if err == io.EOF {
					rc.End <- true
					return
				} else {
					check("SENDER: Err not EOF", err)
				}
			}
		}
	}
}

func (r *Routine) Connector(from net.Conn, to net.Conn, bc chan Block, out chan Block, reconn chan bool) {
	var err error
	for {
		from, err = net.Dial("tcp", ":27011")
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		} else {
			to, err = net.Dial("tcp", ":27012")
			break
		}
	}
	println("MAIN: Two conns set")

	rs := Sender{Routine: Routine{Name: "Sender", End: make(chan bool, 1)}}
	rw := Worker{Routine: Routine{Name: "Worker", End: make(chan bool, 1)}}
	rc := Controller{Routine: Routine{Name: "Controller", End: make(chan bool, 1)}}
	go rs.Sender(to, out, rc)
	go rw.Worker(bc, out)
	go rc.Controller(from, bc, rw, rs, reconn)
}

func main() {
	bc := make(chan Block, 8)
	out := make(chan Block, 8)
	recon := make(chan bool)

	var from net.Conn
	var to net.Conn
	cn := Connector{Routine: Routine{Name: "Connector", End: make(chan bool, 1)}}
	go cn.Connector(from, to, bc, out, recon)
	println("MAIN: GOs run")

	for {
		<-recon
		println("MAIN: reconnect")
		go cn.Connector(from, to, bc, out, recon)
	}

}
