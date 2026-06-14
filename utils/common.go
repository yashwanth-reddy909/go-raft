package utils

func RemoveDuplicates[T comparable](s []T) []T {
	set := make(map[T]struct{})
	for _, element := range s {
		set[element] = struct{}{}
	}

	list := make([]T, 0, len(set))
	for key := range set {
		list = append(list, key)
	}

	return list
}

func RemoveElement[T comparable](s []T, elem T) []T {
	list := make([]T, 0, len(s))
	for _, v := range s {
		if v != elem {
			list = append(list, v)
		}
	}

	return list
}
