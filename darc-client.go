package main

import (
	"bytes"
	"encoding/binary"
	"github.com/itchio/lzma"
	"io"
	"log"
	"net"
	"time"
)

const BUFFER = 1e6

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

func findGap(array []byte) (int, int) {
	at := 16
	end := 20
	slice := array[at:end]
	for {
		if string(slice) != "\xFE\xFE\xFE\xFE" {
			at++
			end++
			slice = array[at:end]
		} else {
			break
		}
	}
	return at, end
}

func serialize(block Block) (array []byte) {
	stamp := make([]byte, 8)
	value := make([]byte, 8) // ID
	name := []byte(block.Name)
	gap := []byte{'\xFE', '\xFE', '\xFE', '\xFE'}
	binary.BigEndian.PutUint64(stamp, uint64(block.Timestamp))
	binary.BigEndian.PutUint64(value, uint64(block.ID))
	array = append(array, stamp...)
	array = append(array, value...)
	array = append(array, name...)
	array = append(array, gap...)
	array = append(array, block.Body...)
	return array
}

func deserialize(array []byte) (b Block) {
	if len(array) == 0 {
		return Block{}
	}
	gap, end := findGap(array)
	b.Timestamp = int64(binary.BigEndian.Uint64(array[:8]))
	b.ID = int64(binary.BigEndian.Uint64(array[:16]))
	b.Name = string(array[16:gap])
	b.Body = array[end:]
	return b
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
			buff := make([]byte, BUFFER)
			_, err := conn.Write([]byte("\xFF"))
			check("Conn send FF", err)
			n, err := conn.Read(buff)
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
			block := deserialize(buff)
			bc <- block
		}
	}
}

func (r *Routine) Worker(bc chan Block, out chan Block) {
	var b bytes.Buffer
	for {
		select {
		case <-r.End:
			return
		case block := <-bc:
			w := lzma.NewWriterSizeLevel(&b, int64(len(block.Body)), 9)
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
			_, err := conn.Write(serialize(send))
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
	bc := make(chan Block, 3)
	out := make(chan Block, 3)
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
