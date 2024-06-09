package utils

import "fmt"

type Address struct {
	IP   string
	Port int64
}

func (a *Address) ToString() string {
	return fmt.Sprintf("%s:%d", a.IP, a.Port)
}

func (a *Address) Equal(o *Address) bool {
	return a.IP == o.IP && a.Port == o.Port
}
