package log

import (
	"fmt"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
)

type LockFreeWriteFile struct {
	file *os.File
}
type WriteFile struct {
	LockFreeWriteFile
	mutex sync.Mutex
}

func newFILE(filename string) *os.File {
	file, _ := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	return file
}

func (f *LockFreeWriteFile) Write(p []byte) (int, error) {
	return f.file.Write(p)
}
func (f *WriteFile) Write(p []byte) (int, error) {
	f.mutex.Lock()
	f.LockFreeWriteFile.Write(p)
	f.mutex.Unlock()
	return len(p), nil
}

func BenchmarkSyncWriteLog(b *testing.B) {
	var syncFileWrite WriteFile
	syncFileWrite.file = newFILE("SyncWriteLogTest.log")
	//InitLog("")
	zerolog.ErrorHandler = func(err error) {
		fmt.Printf("zerolog err %v", err)
	}
	log.Logger = log.Output(&syncFileWrite)
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	var group sync.WaitGroup
	const testCNT = 10
	group.Add(testCNT)

	err2 := errors.New("Testxxxxxxxxxxxxxaa sfsafs")

	for idx := 0; idx < testCNT; idx++ {
		go func() {
			defer group.Done()
			for i := 0; i < b.N; i++ {
				log.Error().Stack().Err(err2).Str("key", "ssssssss").Int32("Number", 100).Msg("SyncWriteTest")
			}
		}()
	}
	// for i := 0; i < b.N; i++ {
	// 	log.Info().Str("key", "ssssssss").Int32("Number", 100).Msg("SyncWriteTest")
	// }
	group.Wait()
	log.Error().Msg("This is end.....")
}

func BenchmarkAsyncWriteLogSTD(b *testing.B) {
	var syncLockfreeWriteLog LockFreeWriteFile
	syncLockfreeWriteLog.file = newFILE("ASyncWriteLogTest.log")
	nonBlockingWrite := diode.NewWriter(&syncLockfreeWriteLog, 32*1024, 0, func(missed int) {
		//fmt.Printf("Logger Dropped %d messages", missed)
	})
	log.Logger = log.Output(&nonBlockingWrite)
	var group sync.WaitGroup
	const testCNT = 10
	group.Add(testCNT)
	for idx := 0; idx < 10; idx++ {
		go func() {
			defer group.Done()
			for i := 0; i < b.N; i++ {
				log.Info().Str("key", "ssssssss").Int32("Number", 100).Msg("AsyncWriteTestSTD")
			}
		}()
	}
	group.Wait()
}
func BenchmarkRealLog(b *testing.B) {
	DisableStdout()
	var group sync.WaitGroup
	const testCNT = 10
	group.Add(testCNT)
	for idx := 0; idx < 10; idx++ {
		go func() {
			defer group.Done()
			for i := 0; i < b.N; i++ {
				log.Info().Str("key", "ssss2222ssss").Int32("Number", 100).Msg("testlog")
			}
		}()
	}
	group.Wait()
}
func init() {
	InitLog("baseTest")
}
func TestFileSplit(t *testing.T) {
	//log.Info().Str("abc", "cccc").Msg("Some fun")

	s := fmt.Sprintf(`^.*[[?:(%s)|?:(git\.ixianlai\.com)]](.*)`, "live-gamelogic")
	s = `^(?:.*live-gamelogic)|(?:.*git\.ixianlai\.com)(.*$)`
	//t.Log(s)
	SplitFileReg := regexp.MustCompile(s)

	t.Log(SplitFileReg.ReplaceAllString(`/f/Work/live-gamelogic/pkg/util/exit.go`, "$1"))
	t.Log(SplitFileReg.ReplaceAllString(`C:/Users/wanghl/go/pkg/mod/gobase@v0.0.1/log/log.go:41`, "$1"))
	t.Log(SplitFileReg.ReplaceAllString(`/C:/Users/wanghl/go/pkg/mod/gobase@v0.0.0-20230403032714-4a34cacf95c2/log/log.go:41`, "$1"))

	time.Sleep(3 * time.Second)
}
