package proxy

import "github.com/CodisLabs/codis/pkg/utils/log"

func (ps *SessionStore) addSession(s *Session) bool {
	ps.smm.Lock()
	defer ps.smm.Unlock()
	if ps.sessions != nil {
		ps.sessions[s] = struct{}{}
		return true
	}
	return false
}

func (ps *SessionStore) removeSession(s *Session) bool {
	ps.smm.Lock()
	defer ps.smm.Unlock()
	if ps.sessions != nil {
		delete(ps.sessions, s)
		return true
	}
	return false
}

func (ps *SessionStore) closeAllSessionReader() {
	ps.smm.Lock()
	defer ps.smm.Unlock()
	for session := range ps.sessions {
		if err := session.CloseReaderWithError(ErrClosedProxy); err != nil {
			log.Errorf("[%p] close session reader failed: %v", session, err)
		}
	}
	ps.sessions = nil
}
