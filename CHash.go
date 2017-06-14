package CHash

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"strconv"
)



type Consistent struct {
	Nodes     map[uint32]string
	numReps   int
	Ring      []uint32
	sync.RWMutex
}

func NewConsistent(numReps int) *Consistent {
	return &Consistent{
		Nodes:     make(map[uint32]string),
		numReps:   numReps,
		Ring:      []uint32{},
	}
}

func (c *Consistent) Add(node string) bool {
	c.Lock()
	defer c.Unlock()

	c.Nodes[c.hashStr(node)] = node
	for i := 0; i < c.numReps; i++ {
		tmpnode:=node+strconv.Itoa(i)
		c.Nodes[c.hashStr(tmpnode)] = node
	}
	c.sortHashRing()
	return true
}

func (c *Consistent) sortHashRing() {
	c.Ring = []uint32{}
	for k := range c.Nodes {
		c.Ring = append(c.Ring, k)
	}
	sort.Slice(c.Ring, func(i, j int) bool { return c.Ring[i] < c.Ring[j] })
}


func (c *Consistent) hashStr(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) Get(key string) string {
	c.RLock()
	defer c.RUnlock()

	hash := c.hashStr(key)
	i := c.search(hash)
	return c.Nodes[c.Ring[i]]
}

func (c *Consistent) search(hash uint32) int {
	a:=0
	for i,tmphash:=range c.Ring{
		if tmphash>=hash{
			fmt.Println(tmphash,hash)
			a=i-1
			break
		}
	}
	return a
}

func (c *Consistent) Remove(node string) {
	c.Lock()
	defer c.Unlock()
	delete(c.Nodes, c.hashStr(node))
	for i := 0; i < c.numReps; i++ {
		tmpnode:=node+strconv.Itoa(i)
		delete(c.Nodes, c.hashStr(tmpnode))
	}
	c.sortHashRing()
}

//func main() {
//
//	cHashRing := NewConsistent(10)
//
//	for i := 0; i < 6; i++ {
//		si := fmt.Sprintf("%d", i)
//		cHashRing.Add("172.18.1."+si)
//	}
//	for i := 0; i < 2; i++ {
//		si := fmt.Sprintf("%d", i)
//		fmt.Println(cHashRing.Get(si))
//	}
//	for _, i := range cHashRing.Ring {
//		fmt.Println("Hash:", i, " IP:", cHashRing.Nodes[i])
//	}
//	cHashRing.Remove("172.18.1.3")
//	for i := 0; i < 2; i++ {
//		si := fmt.Sprintf("%d", i)
//		fmt.Println(cHashRing.Get(si))
//	}
//	for _, i := range cHashRing.Ring {
//		fmt.Println("Hash:", i, " IP:", cHashRing.Nodes[i])
//	}
//
//
//}