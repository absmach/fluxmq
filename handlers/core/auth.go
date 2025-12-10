package core

// Authenticator validates client credentials.
type Authenticator interface {
	Authenticate(clientID, username, password string) (bool, error)
}

// Authorizer checks topic-level permissions.
type Authorizer interface {
	CanPublish(clientID, topic string) bool
	CanSubscribe(clientID, topic string) bool
}

// AuthEngine handles authentication and authorization checks.
type AuthEngine struct {
	auth  Authenticator
	authz Authorizer
}

// NewAuthEngine creates a new auth engine with the given authenticator and authorizer.
func NewAuthEngine(auth Authenticator, authz Authorizer) *AuthEngine {
	return &AuthEngine{
		auth:  auth,
		authz: authz,
	}
}

// CanPublish checks if a client is authorized to publish to a topic.
// Returns true if authorized or if no authorizer is configured.
func (e *AuthEngine) CanPublish(clientID, topic string) bool {
	if e.authz == nil {
		return true
	}
	return e.authz.CanPublish(clientID, topic)
}

// CanSubscribe checks if a client is authorized to subscribe to a topic filter.
// Returns true if authorized or if no authorizer is configured.
func (e *AuthEngine) CanSubscribe(clientID, filter string) bool {
	if e.authz == nil {
		return true
	}
	return e.authz.CanSubscribe(clientID, filter)
}

// Authenticate validates client credentials.
// Returns true if authenticated or if no authenticator is configured.
func (e *AuthEngine) Authenticate(clientID, username, password string) (bool, error) {
	if e.auth == nil {
		return true, nil
	}
	return e.auth.Authenticate(clientID, username, password)
}
