package internal

import (
	"errors"
	"io"
)

var ErrShortRead = errors.New("short read")

func ReadExactly(r io.Reader, out []byte) error {
	for len(out) > 0 {
		n, err := r.Read(out)
		out = out[n:]
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if len(out) > 0 {
		return ErrShortRead
	}
	return nil
}

func ReadExactlyAt(r io.ReaderAt, out []byte, offset int64) error {
	for len(out) > 0 {
		n, err := r.ReadAt(out, offset)
		out = out[n:]
		offset += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if len(out) > 0 {
		return ErrShortRead
	}
	return nil
}

func WriteExactly(w io.Writer, in []byte) error {
	for len(in) > 0 {
		n, err := w.Write(in)
		in = in[n:]
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteExactlyAt(w io.WriterAt, in []byte, offset int64) error {
	for len(in) > 0 {
		n, err := w.WriteAt(in, offset)
		in = in[n:]
		offset += int64(n)
		if err != nil {
			return err
		}
	}
	return nil
}
