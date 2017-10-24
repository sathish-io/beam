package errors

// Any returns any non-nil error from the supplied list, or nil if they are all nil
func Any(e ...error) error {
	for _, x := range e {
		if x != nil {
			return x
		}
	}
	return nil
}
