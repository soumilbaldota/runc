package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/opencontainers/runc/libcontainer"
)

const (
	SYS_SUPERFORK = 470
)
const BTRFS_IOC_SNAP_CREATE_V2 = 0x50009417

type BtrfsIoctlVolArgsV2 struct {
	Fd      int64
	Transid uint64
	Flags   uint64
	Unused  [4]uint64
	Name    [256]byte
}
type ContainerConfig struct {
	ContainerID     [64]byte
	CgroupPath      [256]byte
	NewRootfsPath   [4096]byte
	PidNsPath       [256]byte
	MntNsPath       [256]byte
	IpcNsPath       [256]byte
	UtsNsPath       [256]byte
	NetNsPath       [256]byte
	UserNsPath      [256]byte
	CgroupNsPath    [256]byte
	OwnerUID        uint32
	OwnerGID        uint32
	ShareNamespaces uint8
	_               [3]byte
}

// ========== Performance Tracking ==========

type perfTimer struct {
	name     string
	start    time.Time
	recorder *perfRecorder
}

type perfRecorder struct {
	runID    string
	sourceID string
	newID    string
	start    time.Time
	seq      int64
}

type perfEvent struct {
	Type       string `json:"type"`
	RunID      string `json:"run_id"`
	SourceID   string `json:"source_id,omitempty"`
	NewID      string `json:"new_id,omitempty"`
	Step       string `json:"step,omitempty"`
	Checkpoint string `json:"checkpoint,omitempty"`
	Status     string `json:"status,omitempty"`
	Error      string `json:"error,omitempty"`
	Seq        int64  `json:"seq"`
	TsUnixNs   int64  `json:"ts_unix_ns"`
	OffsetNs   int64  `json:"offset_ns"`
	DurationNs int64  `json:"duration_ns,omitempty"`
}

func newPerfRecorder(sourceID, newID string) *perfRecorder {
	start := time.Now()
	r := &perfRecorder{
		runID:    fmt.Sprintf("%s->%s-%d", sourceID, newID, start.UnixNano()),
		sourceID: sourceID,
		newID:    newID,
		start:    start,
	}
	r.emit(perfEvent{
		Type:     "flow_start",
		Status:   "started",
		TsUnixNs: start.UnixNano(),
		OffsetNs: 0,
	})
	return r
}

func (r *perfRecorder) emit(ev perfEvent) {
	if r == nil {
		return
	}
	r.seq++
	ev.RunID = r.runID
	ev.SourceID = r.sourceID
	ev.NewID = r.newID
	ev.Seq = r.seq
	if ev.TsUnixNs == 0 {
		ev.TsUnixNs = time.Now().UnixNano()
	}
	if ev.OffsetNs == 0 && ev.TsUnixNs > r.start.UnixNano() {
		ev.OffsetNs = ev.TsUnixNs - r.start.UnixNano()
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		logrus.Errorf("superfork perf: failed to marshal event: %v", err)
		return
	}
	logrus.Infof("[PERFJSON] %s", payload)
}

func (r *perfRecorder) beginStep(name string, t time.Time) {
	r.emit(perfEvent{
		Type:     "step_start",
		Step:     name,
		TsUnixNs: t.UnixNano(),
		OffsetNs: t.Sub(r.start).Nanoseconds(),
	})
}

func (r *perfRecorder) endStep(name string, start, end time.Time) {
	r.emit(perfEvent{
		Type:       "step_end",
		Step:       name,
		TsUnixNs:   end.UnixNano(),
		OffsetNs:   end.Sub(r.start).Nanoseconds(),
		DurationNs: end.Sub(start).Nanoseconds(),
	})
}

func (r *perfRecorder) checkpoint(name string) {
	now := time.Now()
	r.emit(perfEvent{
		Type:       "checkpoint",
		Checkpoint: name,
		TsUnixNs:   now.UnixNano(),
		OffsetNs:   now.Sub(r.start).Nanoseconds(),
	})
}

func (r *perfRecorder) finish(err error) {
	now := time.Now()
	status := "ok"
	errMsg := ""
	if err != nil {
		status = "error"
		errMsg = err.Error()
	}
	r.emit(perfEvent{
		Type:       "flow_end",
		Status:     status,
		Error:      errMsg,
		TsUnixNs:   now.UnixNano(),
		OffsetNs:   now.Sub(r.start).Nanoseconds(),
		DurationNs: now.Sub(r.start).Nanoseconds(),
	})
}

func startTimer(name string, recorder *perfRecorder) *perfTimer {
	start := time.Now()
	if recorder != nil {
		recorder.beginStep(name, start)
	}
	return &perfTimer{name: name, start: start, recorder: recorder}
}

func (t *perfTimer) Stop() {
	end := time.Now()
	elapsed := end.Sub(t.start)
	logrus.Infof("[PERF] %s: %v", t.name, elapsed)
	if t.recorder != nil {
		t.recorder.endStep(t.name, t.start, end)
	}
}

func (t *perfTimer) Checkpoint(stage string) {
	elapsed := time.Since(t.start)
	logrus.Infof("[PERF] %s.%s: %v", t.name, stage, elapsed)
	if t.recorder != nil {
		t.recorder.checkpoint(stage)
	}
}

// ========== End Performance Tracking ==========

var superforkCommand = cli.Command{
	Name:  "superfork",
	Usage: "clone a running container into a new container",
	ArgsUsage: `<source-container-id> <new-container-id>

Where "<source-container-id>" is the name for the source running container, and
"<new-container-id>" is the name for the newly cloned container.

EXAMPLE:
   # Clone container "redis-1" to "redis-2"
   runc superfork redis-1 redis-2`,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "bundle, b",
			Value: "",
			Usage: `path to the root of the bundle directory for the new container`,
		},
		cli.StringFlag{
			Name:  "pid-file",
			Value: "",
			Usage: "specify the file to write the new container's init process id to",
		},
		cli.BoolFlag{
			Name:  "detach, d",
			Usage: "detach from the container's process",
		},
		cli.BoolFlag{
			Name:  "no-pivot",
			Usage: "do not use pivot root to jail process inside rootfs",
		},
		cli.BoolFlag{
			Name:  "no-new-keyring",
			Usage: "do not create a new session keyring for the container",
		},
	},
	Action: func(context *cli.Context) error {
		if err := checkArgs(context, 2, exactArgs); err != nil {
			return err
		}
		return doSuperFork(context)
	},
}

func superforkCreatePidFile(path string, pid int) error {
	var (
		tmpDir  = filepath.Dir(path)
		tmpName = filepath.Join(tmpDir, "."+filepath.Base(path))
	)
	f, err := os.OpenFile(tmpName, os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_SYNC, 0o666)
	if err != nil {
		return err
	}
	_, err = f.WriteString(strconv.Itoa(pid))
	f.Close()
	if err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func cgroupPathForKernel(absolutePath string) string {
	prefixes := []string{
		"/sys/fs/cgroup/",
		"/sys/fs/cgroup",
	}

	for _, prefix := range prefixes {
		if strings.HasPrefix(absolutePath, prefix) {
			relativePath := strings.TrimPrefix(absolutePath, prefix)
			if relativePath != "" && !strings.HasPrefix(relativePath, "/") {
				relativePath = "/" + relativePath
			}
			logrus.Infof("superfork: converted path '%s' -> '%s'", absolutePath, relativePath)
			return relativePath
		}
	}

	return absolutePath
}

func doSuperFork(context *cli.Context) (retErr error) {
	sourceID := context.Args().Get(0)
	newID := context.Args().Get(1)
	recorder := newPerfRecorder(sourceID, newID)
	defer func() {
		recorder.finish(retErr)
	}()

	timer := startTimer("total", recorder)
	defer timer.Stop()

	root := context.GlobalString("root")
	if root == "" {
		return errors.New("root not set")
	}

	t1 := startTimer("load_source", recorder)
	sourceContainer, err := libcontainer.Load(root, sourceID)
	if err != nil {
		return fmt.Errorf("failed to load source container: %w", err)
	}
	t1.Stop()

	status, err := sourceContainer.Status()
	if err != nil {
		return fmt.Errorf("failed to get source status: %w", err)
	}

	if status != libcontainer.Running {
		return fmt.Errorf("source container must be running (current: %v)", status)
	}

	sourceConfig := sourceContainer.Config()
	srcRootfs := sourceConfig.Rootfs
	if srcRootfs == "" {
		return fmt.Errorf("source container has no rootfs")
	}

	srcBundle := filepath.Dir(srcRootfs)

	bundle := context.String("bundle")
	if bundle == "" {
		bundleParent := filepath.Dir(srcBundle)
		bundle = filepath.Join(bundleParent, filepath.Base(srcBundle)+"-"+newID)
	}

	dstRootfs := filepath.Join(bundle, "rootfs")

	logrus.Infof("superfork: source bundle: %s", srcBundle)
	logrus.Infof("superfork: new bundle: %s", bundle)
	logrus.Infof("superfork: source rootfs: %s", srcRootfs)
	logrus.Infof("superfork: new rootfs: %s", dstRootfs)

	tSnapshot := startTimer("create_snapshot", recorder)
	if err := createBtrfsSnapshot(srcBundle, bundle); err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}
	tSnapshot.Stop()
	timer.Checkpoint("snapshot_created")

	// Freeze source
	t2 := startTimer("freeze_source", recorder)
	if err := sourceContainer.Pause(); err != nil {
		return fmt.Errorf("failed to freeze source: %w", err)
	}
	t2.Stop()
	timer.Checkpoint("source_frozen")

	sourceThawed := false
	thawSource := func() {
		if sourceThawed {
			return
		}
		t := startTimer("thaw_source", recorder)
		defer t.Stop()

		logrus.Info("superfork: thawing source container")
		currentStatus, err := sourceContainer.Status()
		if err != nil {
			logrus.Errorf("failed to get source status for thaw: %v", err)
			return
		}
		if currentStatus == libcontainer.Paused {
			if err := sourceContainer.Resume(); err != nil {
				logrus.Errorf("failed to thaw source: %v", err)
			} else {
				sourceThawed = true
			}
		} else {
			logrus.Infof("superfork: source container already in state: %v", currentStatus)
			sourceThawed = true
		}
	}
	defer thawSource()

	// Get source PIDs
	tGetPIDs := startTimer("get_source_pids", recorder)
	pids, err := sourceContainer.Processes()
	tGetPIDs.Stop()
	if err != nil {
		return err
	}
	if len(pids) == 0 {
		return fmt.Errorf("no processes in source container")
	}
	timer.Checkpoint("got_pids")

	// Prepare new container spec
	t3 := startTimer("prepare_new_container", recorder)
	_, spec, err := prepareNewContainer(context, sourceID, newID, bundle)
	if err != nil {
		return err
	}
	t3.Stop()

	// Create new container
	t4 := startTimer("create_container", recorder)
	origDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getwd before create: %w", err)
	}
	if err := os.Chdir(bundle); err != nil {
		return fmt.Errorf("chdir to new bundle %s: %w", bundle, err)
	}
	newContainer, err := createContainer(context, newID, spec)
	if chdirErr := os.Chdir(origDir); chdirErr != nil {
		return fmt.Errorf("restore cwd after create: %w", chdirErr)
	}
	if err != nil {
		return err
	}
	t4.Stop()

	// Apply cgroup setup
	t5 := startTimer("apply_cgroup", recorder)
	if err := newContainer.Apply(-1); err != nil {
		newContainer.Destroy()
		return err
	}
	t5.Stop()
	timer.Checkpoint("cgroup_applied")

	// Get cgroup path
	tResolveCgroup := startTimer("resolve_cgroup_path", recorder)
	cgroupPath, err := getContainerCgroupPath(newContainer)
	if err != nil {
		tResolveCgroup.Stop()
		newContainer.Destroy()
		return fmt.Errorf("failed to get new container cgroup path: %w", err)
	}

	if _, err := os.Stat(cgroupPath); err != nil {
		tResolveCgroup.Stop()
		newContainer.Destroy()
		return fmt.Errorf("new container cgroup doesn't exist at %s: %w", cgroupPath, err)
	}

	kernelCgroupPath := cgroupPathForKernel(cgroupPath)

	if kernelCgroupPath == "" || kernelCgroupPath == "/" {
		tResolveCgroup.Stop()
		newContainer.Destroy()
		return fmt.Errorf("invalid kernel cgroup path: %s", kernelCgroupPath)
	}
	tResolveCgroup.Stop()

	logrus.Infof("superfork: using cgroup path: %s (kernel: %s)", cgroupPath, kernelCgroupPath)
	logrus.Infof("superfork: new rootfs path: %s", dstRootfs)

	// Call superfork syscall with new rootfs path
	t6 := startTimer("superfork_syscall", recorder)
	newInitPID, err := superforkSyscall(pids, newID, kernelCgroupPath, dstRootfs)
	t6.Stop()
	timer.Checkpoint("syscall_done")

	if err != nil {
		newContainer.Destroy()
		return fmt.Errorf("syscall failed: %w", err)
	}

	logrus.Infof("superfork: new init PID: %d", newInitPID)

	// Update state
	t7 := startTimer("update_state", recorder)
	if err := updateContainerState(context, newContainer, int(newInitPID), root, newID); err != nil {
		logrus.Errorf("failed to update container state: %v", err)
	}
	t7.Stop()

	if pidFile := context.String("pid-file"); pidFile != "" {
		tPidFile := startTimer("write_pid_file", recorder)
		if err := superforkCreatePidFile(pidFile, int(newInitPID)); err != nil {
			logrus.Errorf("failed to write pid file: %v", err)
		}
		tPidFile.Stop()
	}

	thawSource()
	timer.Checkpoint("source_thawed")

	if context.Bool("detach") {
		fmt.Printf("Container %s successfully forked to %s (init PID: %d)\n",
			sourceID, newID, newInitPID)
		return nil
	}

	fmt.Printf("\nContainer %s successfully forked to %s\n", sourceID, newID)
	fmt.Printf("New container PID: %d\n", newInitPID)
	fmt.Printf("Cgroup: %s\n", cgroupPath)
	fmt.Printf("Rootfs: %s\n", dstRootfs)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	return nil
}

func createBtrfsSnapshot(src, dst string) error {
	parent := filepath.Dir(dst)
	name := filepath.Base(dst)

	logrus.Infof("creating btrfs snapshot: %s -> %s", src, dst)

	// Open source subvolume
	srcFd, err := syscall.Open(src, syscall.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open src: %w", err)
	}
	defer syscall.Close(srcFd)

	// Open destination parent directory
	parentFd, err := syscall.Open(parent, syscall.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open parent: %w", err)
	}
	defer syscall.Close(parentFd)

	var args BtrfsIoctlVolArgsV2
	args.Fd = int64(srcFd)
	copy(args.Name[:], name)

	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(parentFd),
		uintptr(BTRFS_IOC_SNAP_CREATE_V2),
		uintptr(unsafe.Pointer(&args)),
	)
	if errno != 0 {
		return fmt.Errorf("btrfs snapshot failed: %v", errno)
	}

	logrus.Infof("snapshot created: %s", dst)
	return nil
}

func superforkSyscall(pids []int, containerID, cgroupPath, rootfsPath string) (int32, error) {
	pidArr := make([]int32, len(pids))
	for i, p := range pids {
		pidArr[i] = int32(p)
	}

	cfg := ContainerConfig{}
	copy(cfg.ContainerID[:], containerID)
	copy(cfg.CgroupPath[:], cgroupPath)
	copy(cfg.NewRootfsPath[:], rootfsPath) // NEW

	// Explicitly request namespace isolation; kernel will fall back if sockets are sanitized
	cfg.ShareNamespaces = 0

	var newInitPID int32

	_, _, errno := syscall.Syscall6(
		SYS_SUPERFORK,
		uintptr(unsafe.Pointer(&pidArr[0])),
		uintptr(len(pidArr)),
		uintptr(unsafe.Pointer(&cfg)),
		uintptr(unsafe.Pointer(&newInitPID)),
		0, 0,
	)

	if errno != 0 {
		return 0, fmt.Errorf("superfork syscall failed: %v", errno)
	}

	return newInitPID, nil
}

func updateContainerState(context *cli.Context, container *libcontainer.Container, initPID int, root, id string) error {
	state, err := container.State()
	if err != nil {
		return fmt.Errorf("failed to get container state: %w", err)
	}

	state.InitProcessPid = initPID
	state.InitProcessStartTime = uint64(getProcessStartTime(initPID))
	stateDir := filepath.Join(root, id)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return fmt.Errorf("failed to create state directory: %w", err)
	}

	stateFile := filepath.Join(stateDir, "state.json")
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(stateFile, data, 0o644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	logrus.Infof("superfork: saved container state to %s", stateFile)
	return nil
}

func getProcessStartTime(pid int) int64 {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}

	var fields []string
	var inComm bool
	var field string

	for _, b := range data {
		switch b {
		case '(':
			inComm = true
			field = ""
		case ')':
			inComm = false
			fields = append(fields, field)
			field = ""
		case ' ', '\t':
			if !inComm && field != "" {
				fields = append(fields, field)
				field = ""
			} else if !inComm {
				continue
			} else {
				field += string(b)
			}
		default:
			field += string(b)
		}
	}
	if field != "" {
		fields = append(fields, field)
	}

	if len(fields) > 21 {
		startTime, _ := strconv.ParseInt(fields[21], 10, 64)
		return startTime
	}

	return 0
}

func prepareNewContainer(context *cli.Context, sourceID, newID, bundle string) (
	string,
	*specs.Spec,
	error,
) {
	spec, err := setupSpecFile(filepath.Join(bundle, "config.json"))
	if err != nil {
		return "", nil, fmt.Errorf("load spec: %w", err)
	}

	if spec.Hostname == sourceID {
		spec.Hostname = newID
	}

	return bundle, spec, nil
}

func setupSpecFile(path string) (*specs.Spec, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("JSON specification file %s not found", path)
		}
		return nil, err
	}
	defer f.Close()

	var spec *specs.Spec
	if err := json.NewDecoder(f).Decode(&spec); err != nil {
		return nil, err
	}
	return spec, nil
}

func getContainerCgroupPath(container *libcontainer.Container) (string, error) {
	state, err := container.State()
	if err != nil {
		return "", err
	}

	if state.CgroupPaths != nil && len(state.CgroupPaths) > 0 {
		for _, path := range state.CgroupPaths {
			if path != "" {
				return path, nil
			}
		}
	}

	config := container.Config()
	if config.Cgroups != nil && config.Cgroups.Path != "" {
		cgroupPath := filepath.Join("/sys/fs/cgroup", config.Cgroups.Path)
		if _, err := os.Stat(cgroupPath); err == nil {
			return cgroupPath, nil
		}

		cgroupPath = filepath.Join("/sys/fs/cgroup/cpu", config.Cgroups.Path)
		if _, err := os.Stat(cgroupPath); err == nil {
			return cgroupPath, nil
		}

		cgroupPath = filepath.Join("/sys/fs/cgroup/systemd", config.Cgroups.Path)
		if _, err := os.Stat(cgroupPath); err == nil {
			return cgroupPath, nil
		}
	}

	return "", fmt.Errorf("could not determine cgroup path")
}
