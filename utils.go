package majordomo

import (
    "container/list"
    "fmt"
    "reflect"
)

type ZList struct {
    list.List
}

func NewList() *ZList {
    return new(ZList)
}

func (self *ZList) Delete(value interface{}) {
    for elem := self.Front(); elem != nil; elem = elem.Next() {
        if reflect.DeepEqual(elem.Value, value) {
            self.Remove(elem)
            break
        }
    }
}

func (self *ZList) Pop() *list.Element {
    if self.Len() == 0 {
        return nil
    }
    elem := self.Front()
    self.Remove(elem)
    return elem
}

func dump(msg [][]byte) (out string) {
    for _, part := range msg {
        isText := true
        out += fmt.Sprintf("[%03d] ", len(part))
        for _, char := range part {
            if char < 32 || char > 127 {
                isText = false
                break
            }
        }
        if isText {
            out += fmt.Sprintf("%s\n", part)
        } else {
            out += fmt.Sprintf("%X\n", part)
        }
    }
    return
}
