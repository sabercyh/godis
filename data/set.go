package data

type Set struct {
	Dict   *Dict
	length int
}

func SetCreate() *Set {
	return &Set{
		Dict:   DictCreate(),
		length: 0,
	}
}

func (set *Set) Length() int {
	return set.length
}
func (set *Set) SAdd(member *Gobj) error {
	if err := set.Dict.SetNx(member, &Gobj{}); err != nil {
		return err
	}
	set.length++
	return nil
}

func (set *Set) SDel(member *Gobj) error {
	if err := set.Dict.Delete(member); err != nil {
		return err
	}
	set.length--
	return nil
}

func (set *Set) Pop() string {
	member := set.Dict.RandomGetAndDelete()
	if member == "" {
		return ""
	}
	set.length--
	return member
}

func (set *Set) SInter(set1 *Set) []string {
	inter := []string{}
	s := set.Dict.IterateDict()
	s1 := set1.Dict.IterateDict()
	var member, member1 string
	for i := range s {
		member = s[i][0].StrVal()
		for j := range s1 {
			member1 = s1[j][0].StrVal()
			if member == member1 {
				inter = append(inter, member)
				break
			}
		}
	}
	return inter
}

func (set *Set) SDiff(set1 *Set) []string {
	diff := []string{}
	s := set.Dict.IterateDict()
	s1 := set1.Dict.IterateDict()
	var member, member1 string
	for i := range s {
		temp := false
		member = s[i][0].StrVal()
		for j := range s1 {
			member1 = s1[j][0].StrVal()
			if member == member1 {
				temp = true
				break
			}
		}
		if !temp {
			diff = append(diff, member)
		}
	}
	return diff
}

func (set *Set) SUnion(set1 *Set) []string {
	union := []string{}
	s := set.Dict.IterateDict()
	s1 := set1.Dict.IterateDict()
	var member string
	for i := range s {
		member = s[i][0].StrVal()
		if !contain(member, union) {
			union = append(union, member)
		}
	}
	for i := range s1 {
		member = s1[i][0].StrVal()
		if !contain(member, union) {
			union = append(union, member)
		}
	}
	return union
}

func contain(member string, union []string) bool {
	for i := range union {
		if union[i] == member {
			return true
		}
	}
	return false
}
