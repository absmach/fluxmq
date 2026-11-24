// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// On-disk layout.
//
// Every queue lives in one directory below queueLogsDir, and every consumer
// group of a queue in one directory below consumerGroupsDir, each named by
// encodePathComponent. A logical name therefore maps to exactly one path
// element: it cannot climb out of the store, alias another queue, or nest
// inside one, so deleting a queue or group removes only what it owns.
//
// Earlier builds joined the raw names into the path below legacyQueuesDir and
// legacyConsumersDir. Those trees are moved into the encoded layout when the
// store opens. The new layout uses different directory names rather than
// reinterpreting the old ones in place, because a legacy directory that was
// literally named "a%2Fb" is indistinguishable from the encoding of "a/b".
const (
	queueLogsDir       = "queue-logs"
	legacyQueuesDir    = "queues"
	segmentsDirName    = "segments"
	consumerGroupsDir  = "consumer-groups"
	legacyConsumersDir = "consumers"
	consumerOpLogName  = "ops.log"
)

// ErrInvalidName reports a queue or consumer group name that cannot be stored.
var ErrInvalidName = errors.New("invalid storage name")

func validateStorageName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: name is empty", ErrInvalidName)
	}
	return nil
}

func queueDirIn(baseDir, queueName string) string {
	return filepath.Join(baseDir, queueLogsDir, encodePathComponent(queueName))
}

// decodePathComponent inverts encodePathComponent. Only the canonical encoding
// of some name is accepted, so two directories can never decode to one name.
func decodePathComponent(component string) (string, error) {
	if !strings.Contains(component, "%") {
		return component, nil
	}

	var decoded strings.Builder
	decoded.Grow(len(component))
	for i := 0; i < len(component); i++ {
		if component[i] != '%' {
			decoded.WriteByte(component[i])
			continue
		}
		if i+2 >= len(component) {
			return "", fmt.Errorf("%w: truncated escape in %q", ErrInvalidName, component)
		}
		value, err := strconv.ParseUint(component[i+1:i+3], 16, 8)
		if err != nil {
			return "", fmt.Errorf("%w: bad escape in %q", ErrInvalidName, component)
		}
		decoded.WriteByte(byte(value))
		i += 2
	}

	name := decoded.String()
	if encodePathComponent(name) != component {
		return "", fmt.Errorf("%w: %q is not a canonical encoding", ErrInvalidName, component)
	}
	return name, nil
}

// decodedDirNames lists the subdirectories of dir and decodes their names.
// Entries that are not canonical encodings were not written by this layout and
// are skipped.
func decodedDirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name, err := decodePathComponent(entry.Name())
		if err != nil {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)
	return names, nil
}

// discoverQueueNames returns the queues stored under baseDir.
func discoverQueueNames(baseDir string) ([]string, error) {
	return decodedDirNames(filepath.Join(baseDir, queueLogsDir))
}

// migrateLegacyQueues moves every queue found in the legacy layout into its
// encoded directory. Each move is a single rename, so an interrupted migration
// leaves every queue in exactly one of the two trees and the next open
// continues where it stopped.
func migrateLegacyQueues(baseDir string) error {
	legacyRoot := filepath.Join(baseDir, legacyQueuesDir)
	names, err := discoverLegacyQueueNames(legacyRoot)
	if err != nil || len(names) == 0 {
		return err
	}

	targetRoot := filepath.Join(baseDir, queueLogsDir)
	if err := MkdirAllSynced(targetRoot, 0o755); err != nil {
		return err
	}

	// Deepest first: a queue nested inside another must leave before its
	// parent's directory moves and takes it along.
	slices.SortFunc(names, func(a, b string) int {
		return strings.Count(b, "/") - strings.Count(a, "/")
	})
	for _, name := range names {
		source := filepath.Join(legacyRoot, filepath.FromSlash(name))
		target := queueDirIn(baseDir, name)
		if err := renameNoReplace(source, target); err != nil {
			return fmt.Errorf("migrate queue %q: %w", name, err)
		}
	}
	if err := SyncDir(targetRoot); err != nil {
		return err
	}

	removeEmptyDirs(legacyRoot)
	return nil
}

// discoverLegacyQueueNames finds queue roots in the legacy layout by their
// segment files. Legacy queue names could contain '/', so roots can be nested.
func discoverLegacyQueueNames(legacyRoot string) ([]string, error) {
	if _, err := os.Stat(legacyRoot); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	names := make(map[string]struct{})
	err := filepath.WalkDir(legacyRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != SegmentExtension {
			return nil
		}
		if _, err := ParseSegmentName(entry.Name()); err != nil {
			return nil
		}

		segmentsDir := filepath.Dir(path)
		if filepath.Base(segmentsDir) != segmentsDirName {
			return nil
		}
		rel, err := filepath.Rel(legacyRoot, filepath.Dir(segmentsDir))
		if err != nil {
			return err
		}
		if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return nil
		}
		names[filepath.ToSlash(rel)] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	return result, nil
}

// migrateLegacyGroups moves the consumer group directories of one queue from
// the legacy raw-named layout into encoded directories. Legacy group IDs could
// contain '/', so a group's directory, recognised by its operation log, may be
// nested inside another's.
func migrateLegacyGroups(queueDir string) error {
	legacyRoot := filepath.Join(queueDir, legacyConsumersDir)
	groupIDs, err := discoverLegacyGroupIDs(legacyRoot)
	if err != nil || len(groupIDs) == 0 {
		return err
	}

	targetRoot := filepath.Join(queueDir, consumerGroupsDir)
	if err := MkdirAllSynced(targetRoot, 0o755); err != nil {
		return err
	}
	slices.SortFunc(groupIDs, func(a, b string) int {
		return strings.Count(b, "/") - strings.Count(a, "/")
	})
	for _, groupID := range groupIDs {
		source := filepath.Join(legacyRoot, filepath.FromSlash(groupID))
		target := filepath.Join(targetRoot, encodePathComponent(groupID))
		if err := renameNoReplace(source, target); err != nil {
			return fmt.Errorf("migrate consumer group %q: %w", groupID, err)
		}
	}
	if err := SyncDir(targetRoot); err != nil {
		return err
	}

	removeEmptyDirs(legacyRoot)
	return nil
}

func discoverLegacyGroupIDs(legacyRoot string) ([]string, error) {
	var groupIDs []string
	err := filepath.WalkDir(legacyRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) && path == legacyRoot {
				return filepath.SkipDir
			}
			return walkErr
		}
		if entry.IsDir() || entry.Name() != consumerOpLogName {
			return nil
		}
		rel, err := filepath.Rel(legacyRoot, filepath.Dir(path))
		if err != nil {
			return err
		}
		if rel != "." {
			groupIDs = append(groupIDs, filepath.ToSlash(rel))
		}
		return nil
	})
	return groupIDs, err
}

// renameNoReplace refuses to merge a migrated directory into one that already
// exists, which would mean two copies of the same queue or group.
func renameNoReplace(source, target string) error {
	if _, err := os.Lstat(target); err == nil {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, target)
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Rename(source, target)
}

// removeEmptyDirs removes dir and every directory below it that is left empty
// once its empty children are gone. Anything still holding a file stays.
func removeEmptyDirs(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			removeEmptyDirs(filepath.Join(dir, entry.Name()))
		}
	}
	os.Remove(dir) //nolint:errcheck // fails while the directory still has content, which is intended
}
