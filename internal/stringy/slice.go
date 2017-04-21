package stringy

// 如果string类型的a在切片中不存在，那么就添加到切片
func Add(s []string, a string) []string {
	for _, existing := range s {
		if a == existing {
			return s
		}
	}
	return append(s, a)
}

// 求两个切片的并集
func Union(s []string, a []string) []string {
	for _, entry := range a {
		found := false
		for _, existing := range s {
			if entry == existing {
				found = true
				break
			}
		}
		if !found {
			s = append(s, entry)
		}
	}
	return s
}

// 返回切片s中的元素(去掉重复部分)
func Uniq(s []string) (r []string) {
	for _, entry := range s {
		found := false
		for _, existing := range r {
			if existing == entry {
				found = true
				break
			}
		}
		if !found {
			r = append(r, entry)
		}
	}
	return
}
