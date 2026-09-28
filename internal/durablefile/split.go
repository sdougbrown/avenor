package durablefile

// SplitLines splits data on '\n', recording each line's byte end offset (just
// after the newline, or len(data) for a final line with no trailing newline).
func SplitLines(data []byte) []LineSpan {
	var lines []LineSpan
	start := int64(0)
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, LineSpan{Start: start, End: int64(i) + 1})
			start = int64(i) + 1
		}
	}
	if start < int64(len(data)) {
		lines = append(lines, LineSpan{Start: start, End: int64(len(data))})
	}
	return lines
}
