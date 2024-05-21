package data

type Set struct {
	Dict *Dict
	Len  int
}

func SetCreate(dictType DictType) *Set {
	return &Set{
		Dict: DictCreate(dictType),
		Len:  0,
	}
}

func (set *Set) SAdd(member *Gobj) error {
	if err := set.Dict.SetNx(member, &Gobj{}); err != nil {
		return err
	}
	set.Len++
	return nil
}
