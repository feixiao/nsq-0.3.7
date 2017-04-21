package util

import (
	"math/rand"
)

// 生成l个不相同的随机数，随机数范围在[0,n)
func UniqRands(l int, n int) []int {
	set := make(map[int]struct{}) 	// 判断随机数是否已经存在
	nums := make([]int, 0, l)	// 返回结果，存储随机值
	for {
		num := rand.Intn(n)
		// 如果重复，就不添加，重新生成新的
		if _, ok := set[num]; !ok {
			set[num] = struct{}{}
			nums = append(nums, num)
		}
		if len(nums) == l {
			goto exit
		}
	}
exit:
	return nums
}
