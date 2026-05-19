package rafttest

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/datadriven"
)

// scanArgs is a copy-paste from datadriven. TODO
func scanArgs(args []datadriven.CmdArg, key string, dests ...interface{}) error {
	var arg datadriven.CmdArg
	for i := range args {
		if args[i].Key == key {
			arg = args[i]
			break
		}
	}
	if arg.Key == "" {
		return fmt.Errorf("missing argument: %s", key)
	}
	if len(dests) != len(arg.Vals) {
		return fmt.Errorf("%s: got %d destinations, but %d values", arg.Key, len(dests), len(arg.Vals))
	}

	for i := range dests {
		if err := scanErr(arg, i, dests[i]); err != nil {
			return fmt.Errorf("%s: failed to scan argument %d: %v", arg.Key, i, err)
		}
	}
	return nil
}

// scanErr is like Scan but returns an error rather than taking a testing.T to fatal.
func scanErr(arg datadriven.CmdArg, i int, dest interface{}) error {
	if i < 0 || i >= len(arg.Vals) {
		return fmt.Errorf("cannot scan index %d of key %s", i, arg.Key)
	}
	val := arg.Vals[i]
	switch dest := dest.(type) {
	case *string:
		*dest = val
	case *int:
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return err
		}
		*dest = int(n) // assume 64bit ints
	case *int64:
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return err
		}
		*dest = n
	case *uint64:
		n, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return err
		}
		*dest = n
	case *bool:
		b, err := strconv.ParseBool(val)
		if err != nil {
			return err
		}
		*dest = b
	default:
		return fmt.Errorf("unsupported type %T for destination #%d (might be easy to add it)", dest, i+1)
	}
	return nil
}
