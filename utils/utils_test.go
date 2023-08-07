package utils

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkAnts(b *testing.B) {
	for i := 0; i < 20; i++ {
		n := i
		Submit(func() {
			fmt.Println(n)
		})
	}
	time.Sleep(time.Second * 10)
}

func BenchmarkSequence(b *testing.B) {
	var seq Sequence
	for i := 0; i < 20; i++ {
		n := i
		seq.Submit(func() {
			fmt.Println(n)
		})
	}
	time.Sleep(time.Second * 10)
}

func BenchmarkProcess(b *testing.B) {
	SubmitProcess(func() {
		time.Sleep(time.Second * 10)
		fmt.Println("Sleep Over")
		//Exit(0)
	})
	time.Sleep(time.Second)
	//ExitWait()
	Exit(0)
}
