package libdiskserver

import (
	"encoding/hex"
	"os"
	"regexp"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/fs"
	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-multierror"
)

func (s *Server) Walk(in *proto.WalkRequest, stream proto.CAS_WalkServer) (err error) {
	var re *regexp.Regexp
	if in.Regexp != "" {
		re, err = regexp.Compile(in.Regexp)
		if err != nil {
			return grpc.Errorf(codes.InvalidArgument, "%v", err)
		}
	}
	internal.Debugf("-- begin Walk: blocks=%t grep=%q", in.WantBlocks, in.Regexp)
	defer func() {
		internal.Debugf("-- end Walk: err=%v", err)
	}()

	var errors []error
	toperr := s.FS.Walk(func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			internal.Debugf("item: path=%q err=%v", path, err)
			errors = append(errors, err)
			return nil
		}
		if fi == nil ||
			!fi.Mode().IsRegular() ||
			!strings.HasSuffix(path, ".index") ||
			strings.Count(path, "/") != int(s.Depth) {
			return nil
		}
		x := strings.TrimSuffix(path, ".index")
		x = strings.Replace(x, "/", "", -1)
		y, err := hex.DecodeString(x)
		if err != nil {
			return nil
		}
		internal.Debugf("item: path=%q fi=%v", path, fi)
		var fakeAddr cas.Addr
		copy(fakeAddr[:len(y)], y)
		internal.Debugf("fakeAddr=%v", fakeAddr)
		h, err := s.Open(fakeAddr, fs.ReadOnly)
		if err != nil {
			errors = append(errors, err)
			return nil
		}
		defer h.Close()
		index, err := h.LoadIndex()
		if err != nil {
			errors = append(errors, err)
			return nil
		}
		for slot, used := range index.Used {
			internal.Debugf("found slot=%d blknum=%d", slot, used.Offset)
			out := &proto.WalkReply{Addr: used.Addr.String()}
			if in.WantBlocks || re != nil {
				var block cas.Block
				if err := h.LoadBlock(&block, used.Offset); err != nil {
					internal.Debugf("I/O loadBlock err=%v", err)
					errors = append(errors, err)
					return nil
				}
				if !re.Match(block[:]) {
					internal.Debug("no match")
					return nil
				}
				internal.Debug("match")
				if in.WantBlocks {
					out.Block = block[:]
				}
			}
			err := stream.Send(out)
			if err != nil {
				return err
			}
		}
		return nil
	})
	errors = append(errors, toperr)
	return multierror.New(errors)
}