package common

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const Version = "(unreleased)"
const URL = "https://github.com/cloud9-tools/go-cas"

func ShowVersion() {
	prog := filepath.Base(os.Args[0])
	fmt.Println(prog + " " + Version)
	fmt.Println("Copyright ©2015 Donald King")
	fmt.Println("<" + URL + ">")
	os.Exit(0)
}

type VersionFlag struct{}

func (_ VersionFlag) IsBoolFlag() bool { return true }
func (_ VersionFlag) String() string   { return "false" }
func (_ VersionFlag) Get() interface{} { return nil }
func (_ VersionFlag) Set(str string) error {
	value, err := strconv.ParseBool(str)
	if err == nil && value {
		ShowVersion()
	}
	return err
}
