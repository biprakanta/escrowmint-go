package escrowmint

type ConsumeResult struct {
	Applied     bool   `json:"applied"`
	Remaining   int64  `json:"remaining"`
	OperationID string `json:"operation_id"`
}

type TopUpResult struct {
	Added       int64  `json:"added"`
	Available   int64  `json:"available"`
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

type ChunkLease struct {
	LeaseID     string `json:"lease_id"`
	Resource    string `json:"resource"`
	OwnerID     string `json:"owner_id"`
	Granted     int64  `json:"granted"`
	Remaining   int64  `json:"remaining"`
	ExpiresAtMS int64  `json:"expires_at_ms"`
	Status      string `json:"status"`
}

type ChunkConsumeResult struct {
	Applied     bool   `json:"applied"`
	LeaseID     string `json:"lease_id"`
	Remaining   int64  `json:"remaining"`
	ExpiresAtMS int64  `json:"expires_at_ms"`
}
