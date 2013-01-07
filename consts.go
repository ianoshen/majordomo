package majordomo

import "time"

const (
    MDPC_CLIENT = "MDPC01"
    MDPW_WORKER = "MDPW01"
    MDPW_READY = "\001"
    MDPW_REQUEST = "\002"
    MDPW_REPLY = "\003"
    MDPW_HEARTBEAT = "\004"
    MDPW_DISCONNECT = "\005"

    RETRIES = 3
    HEARTBEAT_INTERVAL = 2000 * time.Millisecond
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * RETRIES

    WORKER_RECONNECT_INTERVAL = 2000 * time.Millisecond
    CLIENT_TIMEOUT = 2000 * time.Millisecond

    INTERNAL_SERVICE_PREFIX = "mmi."
)

