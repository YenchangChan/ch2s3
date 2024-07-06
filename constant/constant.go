package constant

const (
	OP_TYPE_BACKUP  = "backup"
	OP_TYPE_RESTORE = "restore"

	STATE_ROWS              = "rows"
	STATE_UNCOMPRESSED_SIZE = "buncsize"
	STATE_COMPRESSED_SIZE   = "bczise"
	STATE_REMOTE_SIZE       = "rsize"

	BACKUP_SUCCESS = 0
	BACKUP_FAILURE = 1
)
