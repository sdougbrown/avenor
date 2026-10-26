package askstore

import "github.com/sdougbrown/avenor/internal/messaging"

// AdmitRequestFrom maps a SendRequest onto the store's admission request. It
// is the one mapping every in-process caller shares, so a field the send
// contract gains cannot be dropped by one caller only. Legacy admits IDs
// without an encoded deadline, which is the local HTTP router's
// compatibility path; the shared client leaves it false.
func AdmitRequestFrom(req messaging.SendRequest, legacy bool) AdmitRequest {
	return AdmitRequest{
		Sender:     req.From,
		Target:     req.To.RunID,
		MessageID:  req.MessageID,
		Generation: req.Generation,
		Body:       req.Body,
		Legacy:     legacy,
		NoReply:    req.NoReply,
	}
}
