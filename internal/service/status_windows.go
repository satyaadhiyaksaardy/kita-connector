package service

import (
	"errors"

	"golang.org/x/sys/windows"
)

// statusOf asks the service manager with query rights only. kardianos's
// Status also asks for start/stop rights, which a normal user lacks, so a
// running service read as "unknown" outside an administrator terminal (M3).
func statusOf(name string) string {
	m, err := windows.OpenSCManager(nil, nil, windows.SC_MANAGER_CONNECT)
	if err != nil {
		return "unknown"
	}
	defer windows.CloseServiceHandle(m)
	h, err := windows.OpenService(m, windows.StringToUTF16Ptr(name), windows.SERVICE_QUERY_STATUS)
	if errors.Is(err, windows.ERROR_SERVICE_DOES_NOT_EXIST) {
		return "not installed"
	} else if err != nil {
		return "unknown"
	}
	defer windows.CloseServiceHandle(h)
	var st windows.SERVICE_STATUS
	if windows.QueryServiceStatus(h, &st) != nil {
		return "unknown"
	}
	switch st.CurrentState {
	case windows.SERVICE_RUNNING, windows.SERVICE_START_PENDING:
		return "running"
	case windows.SERVICE_STOPPED, windows.SERVICE_STOP_PENDING:
		return "stopped"
	}
	return "unknown"
}
