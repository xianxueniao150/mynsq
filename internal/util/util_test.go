package util

import (
	"fmt"
	"testing"

	"github.com/bowen/mynsq/internal/test"
)

func BenchmarkUniqRands5of5(b *testing.B) {
	for i := 0; i < b.N; i++ {
		UniqRands(5, 5)
	}
}
func BenchmarkUniqRands20of20(b *testing.B) {
	for i := 0; i < b.N; i++ {
		UniqRands(20, 20)
	}
}

func BenchmarkUniqRands20of50(b *testing.B) {
	for i := 0; i < b.N; i++ {
		UniqRands(20, 50)
	}
}

func TestUniqRands(t *testing.T) {
	var x []int
	x = UniqRands(3, 10)
	fmt.Println(x)
	test.Equal(t, 3, len(x))

	x = UniqRands(10, 5)
	fmt.Println(x)
	test.Equal(t, 5, len(x))

	x = UniqRands(10, 20)
	fmt.Println(x)
	test.Equal(t, 10, len(x))
}