package workflow

import "testing"

func TestSameSelectionMessaging(t *testing.T) {
	a, b, off := true, true, false
	if !sameSelection(&ExecutionSelection{Messaging: &a}, &ExecutionSelection{Messaging: &b}) {
		t.Fatal("equal choices conflict")
	}
	if sameSelection(&ExecutionSelection{Messaging: &a}, &ExecutionSelection{Messaging: &off}) || sameSelection(&ExecutionSelection{Messaging: &a}, &ExecutionSelection{}) {
		t.Fatal("different choices match")
	}
}
