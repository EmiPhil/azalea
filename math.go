package azalea

// intMin returns the min of the given integers
func intMin(integers ...int) int {
	var a int
	if len(integers) > 0 {
		a = integers[0]
		for _, b := range integers[1:] {
			if b < a {
				a = b
			}
		}
	}
	return a
}
