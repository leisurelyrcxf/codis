package proxy

import "github.com/CodisLabs/codis/pkg/utils/log"

func (ps *ProxySessions) addSession(s *Session) bool {
	ps.smm.Lock()
	defer ps.smm.Unlock()
	if ps.sessionMap != nil {
		ps.sessionMap[s] = struct{}{}
		return true
	}
	return false
}

func (ps *ProxySessions) removeSession(s *Session) bool {
	ps.smm.Lock()
	defer ps.smm.Unlock()
	if ps.sessionMap != nil {
		delete(ps.sessionMap, s)
		return true
	}
	return false
}

func (ps *ProxySessions) closeAllSessionReader() {
	ps.smm.Lock()
	defer ps.smm.Unlock()
	for k := range ps.sessionMap {
		if err := k.CloseReaderWithError(nil); err != nil {
			log.Errorf("[%p] session reader close on error", k)
		}
	}
	ps.sessionMap = nil
}
