package escrowmint

type ConsumeResult struct {
	Applied     bool   `json:"applied"`
	Remaining   int64  `json:"remaining"`
	OperationID string `json:"operation_id"`
}

type Reservation struct {
	ReservationID string `json:"reservation_id"`
	Resource      string `json:"resource"`
	Amount        int64  `json:"amount"`
	ExpiresAtMS   int64  `json:"expires_at_ms"`
	Status        string `json:"status"`
}

type ResourceState struct {
	Resource  string
	Available int64
	Reserved  int64
	Version   int64
}
