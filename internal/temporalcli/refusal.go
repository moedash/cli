package temporalcli

// refusalError is a server refusal read into plain language, so the message
// says what to do next instead of repeating the server's wording.
type refusalError struct {
	plain  string
	detail string
	cause  error
}

func (e *refusalError) Error() string {
	if e.detail == "" {
		return e.plain
	}
	return e.plain + " The server said: " + e.detail
}

func (e *refusalError) Unwrap() error { return e.cause }
