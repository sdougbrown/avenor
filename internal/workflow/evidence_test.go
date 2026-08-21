package workflow

import "testing"

func TestValidateStoredPath(t *testing.T) {
	tests := []struct {
		name string
		wantErr bool
	}{
		{name: "out.txt", wantErr: false},
		{name: "logs/out.txt", wantErr: false},
		{name: "a/b/c.txt", wantErr: false},
		{name: "", wantErr: true},
		{name: ".", wantErr: true},
		{name: "..", wantErr: true},
		{name: "../x", wantErr: true},
		{name: "a/../b", wantErr: true},
		{name: "/abs", wantErr: true},
		{name: "a\\b", wantErr: true},
		{name: "a\x00b", wantErr: true},
		{name: "a//b", wantErr: true},
		{name: "a/./b", wantErr: true},
		{name: "a/..", wantErr: true},
	}
	for _, tt := range tests {
		err := validateStoredPath(tt.name)
		if (err != nil) != tt.wantErr {
			t.Fatalf("validateStoredPath(%q): expected error=%v, got %v", tt.name, tt.wantErr, err)
		}
	}
}