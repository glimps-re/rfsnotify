# Changelog

## [v0.2.0]

### Fixed

- deadlock on Windows when a removal or a directory creation is notified while other events are pending: watches are no longer added or removed by the goroutine reading fsnotify events
- `Close` waits for in-flight watch updates before closing fsnotify, returns its error, and closes `Events` and `Errors` before returning, even when nobody reads them anymore
- `AddRecursive` skips sub-directories removed while walking instead of failing
- `RemoveRecursive` works on deleted directories, it relies on the watch list instead of walking the file system
- race on closed state between `Close` and `Add`/`AddRecursive`

### Changed

- errors while updating watches on events are sent to `Errors` instead of being logged, expected ones (watch already removed, entry already deleted) are ignored
- fsnotify notifications are queued without limit while the consumer does not read `Events`, instead of blocking fsnotify
- `AddRecursive` uses `filepath.WalkDir`, faster on large trees
- `ErrClosed` is exported

## [v0.1.0]

- initial fork of [farmergreg/rfsnotify](https://github.com/farmergreg/rfsnotify)
