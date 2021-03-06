package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"time"
)

var ErrCrcMismatch = errors.New("CRC-Mismatch")

type reqType int16

const (
	REQ_PRODUCE reqType = iota
	REQ_FETCH
	REQ_MULTIFETCH
	REQ_MULTIPRODUCE
	REQ_OFFSETS
)

var OFF_OLDEST int64 = -2
var OFF_NEWEST int64 = -1

type reqHeader struct {
	Length   int32
	Request  int16
	TopicLen int16
}

type reqFooter struct {
	Partition int32
	Offset    int64
	Size      int32
}

type request struct {
	reqHeader
	reqFooter
	topic []byte
}

func makeRequest(rType reqType, topic string, partition int32, offset int64, num int32) request {
	req := request{}
	req.Length = int32(20 + len(topic))
	req.Request = int16(rType)
	req.TopicLen = int16(len(topic))
	req.topic = []byte(topic)
	req.Partition = partition
	req.Offset = offset
	req.Size = num

	return req
}

func (r request) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, &(r.reqHeader)); err != nil {
		return err
	}

	if _, err := w.Write(r.topic); err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, &(r.reqFooter))
}

type message struct {
	Length int32
	Magic  byte
	Att    byte
	Crc    int32
}

type response struct {
	Length    int32
	ErrorCode int16
}

type ConsumerOptions struct {
	ReadBuffer *bytes.Buffer
	RetryDelay time.Duration
}

func DefaultConsumerOptions() ConsumerOptions {
	return ConsumerOptions{bytes.NewBuffer(make([]byte, 1024*1024)), 100 * time.Millisecond}
}

type Consumer struct {
	conn net.Conn
	co   ConsumerOptions

	topic     string
	partition int32
	offset    int64
}

func OpenConsumer(addr, topic string, partition int32, offset int64, options ConsumerOptions) (*Consumer, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second*2)
	if err != nil {
		return nil, err
	}

	c := &Consumer{conn: conn, topic: topic, partition: partition, offset: offset, co: options}
	if offset == OFF_NEWEST || offset == OFF_OLDEST {
		offsets, err := c.Offsets(offset, 1)
		if err != nil {
			c.Close()
			return nil, err
		}

		c.Seek(offsets[0])
	}

	return c, nil
}

func (r *Consumer) Close() error {
	return r.conn.Close()
}

const MAX_BUFFER = 1024 * 1024

func (r *Consumer) fill() error {
	req := makeRequest(REQ_FETCH, r.topic, r.partition, r.offset, MAX_BUFFER)
	if err := req.Write(r.conn); err != nil {
		log.Println(err)
		return err
	}

	resp := response{}
	if err := binary.Read(r.conn, binary.BigEndian, &resp); err != nil {
		log.Println(err)
		return err
	} else if resp.ErrorCode != 0 {
		r.co.ReadBuffer.Reset()
		return errors.New(fmt.Sprintf("Kafka error: %d", resp.ErrorCode))
	}

	r.co.ReadBuffer.Reset()
	_, err := io.CopyN(r.co.ReadBuffer, r.conn, int64(resp.Length-2))
	if err != nil {
		log.Println(err)
	}
	return err
}

func (r *Consumer) Seek(offset int64) {
	r.offset = offset
	r.co.ReadBuffer.Reset()
}

func (r *Consumer) DecodeMessage() (int, error) {
    return 0,nil
}

func (r *Consumer) Fetch()(data []string,err error){
    for r.co.ReadBuffer.Len() == 0 {
		if err = r.fill(); err != nil {
			return
		}

		if r.co.ReadBuffer.Len() > 0 {
			break
		}else{
            //if the fetch return empty ,sleep a while and do again
	        //TODO 截断二进制指数退避算法 in CSMA/CD（载波监听多路接入冲突检测）协议
            time.Sleep(2000 * time.Millisecond)
        }
	}

    msg := message{}
    buf := make([]byte,1024*1000)
    for r.co.ReadBuffer.Len() > 0 {
        err = binary.Read(r.co.ReadBuffer, binary.BigEndian, &msg)
        if  err != nil{
            break
        }
        if msg.Length-6 < 0 || msg.Length-6 >int32(len(buf)){
            log.Fatal(msg,msg.Length,msg.Magic,msg.Att,msg.Crc)
            break
        }

        n,err := r.co.ReadBuffer.Read(buf[:msg.Length-6])
        if err != nil{
            //如果err=EOF,n就是0
            break
        }
        if int32(n) != msg.Length-6 {
            //读到的长度和期望的数据长度不一致，说明buf已经用完了，需要去kafka取
            break
        }
        if crc32.ChecksumIEEE(buf[:n]) != uint32(msg.Crc) {
            err = ErrCrcMismatch
            break
        }
        r.offset += 4 + int64(msg.Length)
        data = append(data,string(buf[:n]))
    }
    return
}

func (r *Consumer) Read(buf []byte) (int, error) {
	for r.co.ReadBuffer.Len() == 0 {
		if err := r.fill(); err != nil {
			log.Println(err)
			return 0, err
		}

		if r.co.ReadBuffer.Len() > 0 {
            log.Println("fetch from kafka:",r.co.ReadBuffer.Len())
			break
		}
        log.Println("empty fetch from kafka")
        //if the fetch return empty ,sleep a while and do again
		time.Sleep(100 * time.Millisecond)
	}

    //length/int32 magic/byte [option att/byte] crc/int32 data/length-5 or length - 6...
	var (
        err error
	)

    msg := message{}
    for r.co.ReadBuffer.Len() > 0 {
        log.Println(r.co.ReadBuffer.Len())
        if binary.Read(r.co.ReadBuffer, binary.BigEndian, &msg) != nil{
            log.Println("read message length error:",err)
        }

        n,err := r.co.ReadBuffer.Read(buf[:msg.Length-6])
        if err != nil{
            log.Println(err)
        }
        if crc32.ChecksumIEEE(buf[:msg.Length-6]) != uint32(msg.Crc) {
            return 10 + n, ErrCrcMismatch
        }
        log.Println(buf[:msg.Length-6])
    }
    return 0,err
}

func (r Consumer) GetOffset() int64 {
	return r.offset
}

func (r *Consumer) Offsets(base int64, num int32) ([]int64, error) {
	req := makeRequest(REQ_OFFSETS, r.topic, r.partition, base, num)
	if err := req.Write(r.conn); err != nil {
		return nil, err
	}

	resp := response{}
	if err := binary.Read(r.conn, binary.BigEndian, &resp); err != nil {
		return nil, err
	} else if resp.ErrorCode != 0 {
		return nil, errors.New(fmt.Sprintf("Kafka error: %d", resp.ErrorCode))
	}

	buf := new(bytes.Buffer)
	if _, err := io.CopyN(buf, r.conn, int64(resp.Length-2)); err != nil {
		return nil, err
	}

	numOffsets := int32(0)
	if err := binary.Read(buf, binary.BigEndian, &numOffsets); err != nil {
		return nil, err
	}

	offsets := make([]int64, numOffsets)
	for i := int32(0); i < numOffsets; i++ {
		offset := int64(0)
		if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
			return nil, err
		}

		offsets[i] = offset
	}

	return offsets, nil
}
