package majordomo

import (
    "log"
    "os"
)

var StdLogger, ErrLogger *log.Logger

func init() {
    StdLogger = log.New(os.Stdout, "[Majordomo Info] ", log.Ldate | log.Ltime | log.Lshortfile)
    ErrLogger = log.New(os.Stderr, "[Majordomo Error] ", log.Ldate | log.Ltime | log.Lshortfile)
}
