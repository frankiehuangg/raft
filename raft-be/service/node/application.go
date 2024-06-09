package node

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type application struct {
	storage map[string]string
}

type Application interface {
	ping() string
	get(key string) string
	set(key string, value string) bool
	strlen(key string) string
	del(key string) bool
	append(key string, value string) bool

	Execute(command string) (string, error)
}

func NewApplication() Application {
	return application{
		storage: make(map[string]string),
	}
}

func (a application) ping() string {
	return "PONG"
}

func (a application) get(key string) string {
	return a.storage[key]
}

func (a application) set(key string, value string) bool {
	a.storage[key] = value
	return true
}

func (a application) strlen(key string) string {
	return strconv.Itoa(len(a.storage[key]))
}

func (a application) del(key string) bool {
	if _, ok := a.storage[key]; ok {
		delete(a.storage, key)
		return true
	}

	return false
}

func (a application) append(key string, value string) bool {
	if _, ok := a.storage[key]; ok {
		a.storage[key] = a.storage[key] + value
		return true
	} else {
		a.set(key, value)
		return true
	}

	return false
}

func (a application) Execute(command string) (string, error) {
	commandArgs := strings.Split(command, " ")

	switch len(commandArgs) {
	case 1:
		if commandArgs[0] == "ping" {
			return "PONG", nil
		} else if commandArgs[0] == "icespice" {
			return "gyatt", nil
		} else {
			return "", errors.New(fmt.Sprintf("command \"%s\" not found", command))
		}
	case 2:
		if commandArgs[0] == "get" {
			return a.get(commandArgs[1]), nil
		} else if commandArgs[0] == "strlen" {
			return a.strlen(commandArgs[1]), nil
		} else if commandArgs[0] == "del" {
			if a.del(commandArgs[1]) {
				return "OK", nil
			} else {
				return "", errors.New("key not found")
			}
		} else {
			return "", errors.New(fmt.Sprintf("command \"%s\" not found", command))
		}
	case 3:
		if commandArgs[0] == "set" {
			a.set(commandArgs[1], commandArgs[2])
			return "OK", nil
		} else if commandArgs[0] == "append" {
			if a.append(commandArgs[1], commandArgs[2]) {
				return "OK", nil
			} else {
				return "", errors.New("key not found")
			}
		} else {
			return "", errors.New(fmt.Sprintf("command \"%s\" not found", command))
		}
	default:
		return "", errors.New(fmt.Sprintf("command \"%s\" not found", command))
	}
}
