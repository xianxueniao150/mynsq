package nsqd

import (
	"fmt"
	"testing"
)

func TestBufferPool(t *testing.T) {
	buf := bufferPoolGet()
	buf.WriteString("aaaa")
	fmt.Println(buf.String())  //aaaa

	buf2 := bufferPoolGet()
	buf2.WriteString("bbbbbb")
	bufferPoolPut(buf)
	fmt.Println(buf2.String()) //bbbbbb
	fmt.Println(buf.String()) //

	fmt.Println("--------------------------------------")
	buf3 := bufferPoolGet()
	fmt.Printf("buf3.Len():%d	buf3.Cap():%d\n",buf3.Len(),buf3.Cap()) //buf3.Len():0	buf3.Cap():64
	buf4 := bufferPoolGet()
	fmt.Printf("buf4.Len():%d	buf4.Cap():%d\n",buf4.Len(),buf4.Cap()) //buf4.Len():0	buf4.Cap():0
	buf3.WriteString("333")
	buf4.WriteString("444")
	fmt.Println(buf3.String())  //333
	fmt.Println(buf4.String()) 	//444

}
