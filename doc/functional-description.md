# historyprovider-rs
_historyprovider-rs_ (hp-rs) whose goal is to gather logs from the shv tree and provide a `getLog` interface.

To do that _hp-rs_ does these things:
- retrieves logs from other sources
- exposes a `getLog` interface for every site
- maintains a log directory for every log source
- exposes a `_shvjournal` node, which serves as a controlling node and also contain every file that _hp-rs_ has under its
  possession

## Startup
At startup, _hp-rs_:
- Retrieves sites definition via `getSites`
- Recursively iterates through sites and looks for `_meta` nodes containing `HP3`
- An `HP3` node can be either `device` (which is the default) or `HP3`, this information is in the `type` key

_hp-rs_ downloads logs at the first `HP`/`HP3` node it encounters in a subree. The `HP3` type signifies that
the node is a _hp-rs_ and its logs includes all logs from its subtree. This creates a cascade so that logs gradually flow
from the `device`s (_shvgate_) to the top _hp-rs_.

Otherwise, the whole site are traversed, and site nodes are created and detected via `HP`/`HP3`. The `HP` key can also
contain the `pushLog` (Bool) key, which signifies whether this is a pushlog site. Logs are not synced directly from
pushlog sites.

_hp-rs_ also checks for the presence of the `HP3` key in the root node of the sites, i.e. the node corresponding to
where this _hp-rs_ instance runs. If it's not present, _hp-rs_ should exit with an error. This is to prevent running
_hp-rs_ instances that can't be "seen" from parent _hp-rs_ instances.

## How does _hp-rs_ sync logs?
There three ways _hp-rs_ can obtain logs:

0) Capturing events.
1) Downloading files via "file synchronization".
2) Downloading logs via `getLog` and saving them to files.
3) Receiving logs via pushLog.

Syncing is done on a per-source basis. A source can have a nested subdirectory tree. If the local database for a
specific leaf subdirectory is empty, _hp-rs_ only downloads history up until a specific point (controlled by the
configuration). Afterwards, only new logs are ever downloaded, _hp-rs_ never downloads logs, that are older than it
already has.

### Capturing events
This is the most straightforward way of getting logs and isn't really "downloading". _hp-rs_ subscribes all the sites for
`chng` events and saves them to a file called dirtylog. The dirtylog serves as a temporary storage of logs, until actual
logs are retrieved from other sources. When new logs are retrieved, entries that are older than the newest synced entry
are removed. This process is called "dirty log trimming".

### File synchronization
The standard way for retrieving logs is via file synchronization. _hp-rs_ retrieves a file list from a node and checks the
difference between the local database and the remote database. All newer files are synced to the local database. The
filelist can be provided by either the "device" (_shvgate_) or "HP3" and the node that contains the filelist is called
`shvjournal` or `_shvjournal`. The location of the filelist node (and the file nodes) can be found through the sites
definition.

_hp-rs_ is supposed to retrieve files via chunks. The default maximum chunk size is 128 kB chunks. The max chunk size is
configurable via sites.

An _hp-rs_ node of type "device" can have additional configuration:
- readLogChunkLimit: Int - Maximum size of data _hp-rs_ will retrieve for one `read` call.
- syncPath: Signifies the node where logs are located on the site.

#### Procedure
1) _hp-rs_ takes a path from which it'll sync (that could be a site path, or a child _hp-rs_).
2) _hp-rs_ calls `lsfiles` on the node specified by `syncPath` (e.g. `.app/history`, `_shvjournal`).
3) From the response, _hp-rs_ recursively travels through subdirectories to find all files present on the source.
4) _hp-rs_ finds the most recent file in the local repository for the source and subdirectory. If there are no local
files for the source and subdirectory, start downloading files not older than the configured age.
5) _hp-rs_ downloads all newer files from the source.
6) If the latest local file is incomplete, it'll download the rest of it.
7) Files are not downloaded wholly at once, they are downloaded via smaller chunks so as not to overload the source.
8) Log files are always only appended, so you can be sure that files with the same size also have the some contents .
9) The filenames are timestamps, but do not depend on the order of files from `lsfiles`, sort it beforehand.
10) Limit the memory usage of the synchronization process, if you run out, write the current files, and continue.
11) There two API versions for the `read` method: one takes a map, and one takes an array:
```
{
    offset: Int;
    size: Int;
}

[Int, Int] // offset, size
```
The API can be detected by the presence of the `sha1` method. If it exists, it's the newer "List" API. Otherwise, it's
the map API. For a single source, only detect the method once.

Note: the Map API is only supported because of SHV2, and is deprecated.
12) The source can return less data than requested, this is not an error, just make another request.
13) If the source returns MORE data than requested, it is an error, reject the response.
14) If any of the files are rejected for any reason, skip all queued file downloads from the current source, and try
again in the next sync cycle, here are some of the reasons for failure:
- A download failed to finish (`read` resulted in an error).
- The result had an unexpected (e.g. a String, instead of a Blob).
- Got a different offset than requested, or got more data than requested.
- The local file is larger than the remote file.
15) If a file has an empty size, you don't have to use read, just create it manually on the filesystem. This is to
prevent a bug from an old _shvgate_ where you couldn't download zero-sized files via `read`.
16) The timeout for the `read` call should be higher than other requests (about a minute or so). Some devices take
longer to respond. This is mostly solved by downloading through chunks, but a higher timeout can't hurt.
17) The remote filenames can't be trusted to be in the correct format (i.e. a timestamp), so always validate them.
18) After the synchronization is done, write the rest of the files to the filesystem.
19) Find the newest entry in the local repository. Remove all entries older than this entry from the dirtylog. Produce a
warning if the dirtylog is too large after this process (large == 20M).
20) The synchronization process can take quite a long time, it is possible that the timer for periodical syncing tries
to sync a site that's already being synced. Make sure that the isn't being synced twice at the same time.

### Downloading via `getLog`
Devices that don't support directly retrieving files, _hp-rs_ falls back to retrieving logs via `getLog`. _hp-rs_ calls the
method, figures out a filename and saves that to the disk. The filenames are more or less guessed, so they won't
correspond to the filenames on the device.

Note: this method is deprecated, and only used for legacy devices.

### Receiving logs via pushLog
Some devices can't connect to the broker fulltime, so instead of _hp-rs_ downloading logs, the devices "push" the logs
to _hp-rs_. The process is done by calling the `pushLog` method on the site node. Pushlog sites have a `pushLog: true`
key in their site `_meta` node.

## When does _hp-rs_ sync logs?
### Manually through the `syncLog` method
The user can invoke the `syncLog` method to manually sync sites. More info the SHV tree section.

### Periodically over time
_hp-rs_ iterates over all sites and syncs them periodically one by one. The interval between each site is configurable
(default one minute).

### On the `mntchng` signal
Whenever a site gets a `mntchng` signal, it might mean that it has been offline for some time, and our data is stale,
because we couldn't get events for our dirty log. Therefore, the site gets synced.

## Sanitizer
_hp-rs_ periodically calculates the size of all logs combined. If the size exceeds the maximum log size, the oldest file
is deleted until the total log size is under the maximum log size. The maximum log size is configurable. The interval
between sanitizing is also configurable.

Note: always keep at least one file in each difrectory.

## Value cache
_hp-rs_ has to subscribe to the whole tree for changes, so as a side effect, it exports a value cache. It caches all
calls to get on all paths.

## SHV tree
### `_valuecache:get`
| Parameter | Result   | Access |
|-----------|----------|--------|
| String    | RpcValue | dev    |

Returns the result of the `get` method called on the path specified by the param. If the value is currently not
available in the cache, _hp-rs_ will call the method directly, and return the result (and caching it).

### `_valuecache:getCache`
| Parameter | Result   | Access |
|-----------|----------|--------|
| Null      | Map      | dev    |

Returns the current contents of the cache.

### `_shvjournal:syncLog`
| Parameter | Result   | Access |
|-----------|----------|--------|
| String    | string[] | dev    |

Triggers the synchronization manually. The mandatory string parameter serves as a prefix for which sites should be
synchronized. This method is meant to be used for debugging or to speed up fixes in synchronization, and is not meant to
be used regularly.

### `_shvjournal:syncInfo`
| Parameter | Result   | Access |
|-----------|----------|--------|
| String    | Map      | dev    |

Shows info about about the latest attempt at synchronization for each site. The format is a Map where the keys are log
sources, otherwise the format is unspecified. It should include information about:
- filenames that were attempted to be synchronized, and their remote/local sizes,
- successfully downloaded file chunks
- failed downloads

The information should also contain timestamps. The optional string parameter serves as a prefix for which sites should
be included in the response.

### `_shvjournal:logSizeLimit`
| Parameter | Result   | Access |
|-----------|----------|--------|
| Null      | Int      | dev    |

The configured maximum log size.

### `_shvjournal:totalLogSize`
| Parameter | Result   | Access |
|-----------|----------|--------|
| Null      | Int      | dev    |

The current log size.

### `_shvjournal:logUsage`
| Parameter | Result   | Access |
|-----------|----------|--------|
| Null      | Decimal  | dev    |

The percentage of space occupied (i.e. totalLogSize/logSizeLimit).

### `_shvjournal/**:*`
File node structure according to: https://github.com/silicon-heaven/shv-doc/blob/master/src/rpcmethods/file.md

### `<site_path>:logSize`
| Parameter | Result   | Access |
|-----------|----------|--------|
| Null      | Int      | dev    |
Total log size of all files for this site.

### `<site_path>:getLog`
| Parameter | Result   | Access |
|-----------|----------|--------|
| Map       | List     | rd     |

Retrieves logs for the site. The parameter is in this format:
```typescript
{
    recordCountLimit: Int; // The maximum records in the result.
    withSnapshot: Bool; // Whether the result should include a snapshot.
    since: DateTime|"last"; // The datetime from which the logs should begin, or "last".
    until: DateTime; // The datetime until which the logs should continue.
    pathPattern: String; // Filter for paths.
    pathPatternType: Int; // "regex" for regular expressions, anything other means wildcard
}
```
The default parameters are:
```typescript
{
    recordCountLimit: implementation defined by the client (usually it's 10000)
    withSnapshot: false
    since: the beginning of time
    until: the end of time
    pathPattern: "";
    pathPatternType: "";
}
```
The result is a List, where each element is one log entry. A log entry is a List with a fixed size of 7. The elements
are defined as:

0) timestamp: DateTime
1) path: String
2) value: RpcValue
3) shortTime: Int (-1 for no short time)
4) domain: String (empty string for no domain)
5) valueFlags: ValueFlags(UInt) (bit field: 0 - Snapshot, 1 - Spontaneous, 2 - Provisional)
6) userId: String

### `<site_path>:onlineStatus`
| Parameter | Result                    | Access |
|-----------|---------------------------|--------|
| Null      | i[Unknown,Offline,Online] | rd     |

Shows the online status of the site.

### `<site_path>:alarmTable`
| Parameter | Result   | Access |
|-----------|----------|--------|
| Null      | List     | rd     |

Returns a List of currently active alarms for the site. An alarm is a Map with these fields:
```typescript
{
    description: String;
    isActive: true;
    label: String;
    path: String;
    severity: Int;
    severityName: String;
    stale: Bool;
    timestamp: DateTime;
}
```

### `<site_path>:alarmLog`
| Parameter | Result   | Access |
|-----------|----------|--------|
| Map       | List     | rd     |

Returns a log of alarm appearances and disappearances.
Param:
```typescript
{
    since: DateTime,
    until: DateTime,
}
```
The result contains a snapshot of which alarms were present at `since`. It also contains `events` which show the history
of which alarms began, and which ended. The `isActive` field is used to show if the alarm began or ended.
```typescript
{
    snapshot: HpAlarm[],
    events: HpAlarm[],
}
```
Description for `HpAlarm` is in the `alarmTable` method definition.

### `<site_path>:pushLog`
| Parameter | Result   | Access |
|-----------|----------|--------|
| Map       | Map      | wr     |

This method allows clients to supply logs to _hp-rs_ instead of _hp-rs_ downloading it from them. The param is the log
to be pushed. See the result of `<site_path>:getLog` for the format.

The result of `pushLog` is a Map in this format:
```typescript
{
    since: DateTime; // The timestamp of the first log entry received.
    until: DateTime; // The timestamp of the last log entry received.
    msg: "success"; // A string containing "success" literally.
}
```
This log is saved to the disk as a new file for every `pushLog` call.

Note: It isn't clear why the `since` and `until` fields are needed, or if the clients use it.

Note: The clients depend on `msg` being `"success"`, otherwise they keep trying again.

Note: The clients don't necessarily push valid logs, so some measures are implemented to counter the invalid ones:

1) Any pushlog entry that's older the newest one is rejected.
2) Any entry with a timestamp and path combination that already exists on the local disk is rejected. E.g., the new
pushlog contains an entry with the path devices/a/b and timestamp 2025-01-01T00-00-00Z, but our local cache already has
an entry with this exact path and timestamp (the value is ignored). _hp-rs_ will reject this entry.

If _hp-rs_ rejects an entry, it'll log the incident, and move on to the next entry. It won't notify the caller in any
way (the result will still say `msg: "success"`.

### `<site_path>:pushLogDebugLog`
| Parameter | Result   | Access |
|-----------|----------|--------|
| Null      | List     | dev    |

Shows info about the latest `pushLog` call on this site. The format is unspecified, but should include information
about:
- The start of a pushlog operation
- Rejecting entries
- The total number of rejected entries
