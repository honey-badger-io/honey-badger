package term

import (
	"fmt"

	"github.com/pkg/term"
)

var commandsHistory []string
var historyPos = -1

func ReadCmd() string {
	cmdText := ""

	for {
		ascii, keyCode, _ := getChar()

		if keyCode == 38 { // arrow up
			if historyPos == -1 {
				continue
			}

			// Clear current buffer
			for i := 1; i < len(cmdText); i++ {
				fmt.Printf("\b ")
				fmt.Printf("\b")
			}

			cmdText = commandsHistory[historyPos]
			fmt.Printf("%s", cmdText)
			historyPos--
		}

		if keyCode == 40 { // arrow down
			if historyPos >= len(commandsHistory)-1 {
				continue
			}

			// Clear current buffer
			for i := 1; i < len(cmdText); i++ {
				fmt.Printf("\b ")
				fmt.Printf("\b")
			}

			cmdText = commandsHistory[historyPos+1]
			fmt.Printf("%s", cmdText)
			historyPos++
		}

		if ascii == 3 {
			fmt.Printf("Interrupt\n")
			return "quit"
		}

		if ascii == 13 { // enter key
			// Push command to history and advance position
			commandsHistory = append(commandsHistory, cmdText)
			historyPos++

			fmt.Printf("\n")
			return cmdText
		}

		if ascii == 127 { //backspace
			if cmdText == "" {
				continue
			}

			// Clear last character from tty
			fmt.Printf("\b ")
			fmt.Printf("\b")

			// Remove last character from command text
			cmdText = cmdText[:len(cmdText)-1]
			continue
		}

		// Buffer command text
		keyText := string(rune(ascii))
		cmdText = cmdText + keyText

		fmt.Printf("%s", keyText)
	}
}

// https://github.com/paulrademacher/climenu/blob/master/getchar.go
// Returns either an ascii code, or (if input is an arrow) a Javascript key code.
func getChar() (ascii int, keyCode int, err error) {
	t, _ := term.Open("/dev/tty")
	term.RawMode(t)
	bytes := make([]byte, 3)

	var numRead int
	numRead, err = t.Read(bytes)
	if err != nil {
		return
	}
	if numRead == 3 && bytes[0] == 27 && bytes[1] == 91 {
		// Three-character control sequence, beginning with "ESC-[".

		// Since there are no ASCII codes for arrow keys, we use
		// Javascript key codes.
		if bytes[2] == 65 {
			// Up
			keyCode = 38
		} else if bytes[2] == 66 {
			// Down
			keyCode = 40
		} else if bytes[2] == 67 {
			// Right
			keyCode = 39
		} else if bytes[2] == 68 {
			// Left
			keyCode = 37
		}
	} else if numRead == 1 {
		ascii = int(bytes[0])
	} else {
		// Two characters read??
	}
	t.Restore()
	t.Close()
	return
}
