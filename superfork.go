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
	"unsafe"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/opencontainers/runc/libcontainer"
)

const (
	SYS_SUPERFORK = 470
)

type ContainerConfig struct {
	ContainerID     [64]byte
	CgroupPath      [256]byte
	PidNsPath       [256]byte
	MntNsPath       [256]byte
	IpcNsPath       [256]byte
	UtsNsPath       [256]byte
	NetNsPath       [256]byte
	UserNsPath      [256]byte
	CgroupNsPath    [256]byte
	OwnerUID        uint32
	OwnerGID        uint32
	ShareNamespaces uint8 // unused for now.
	_               [3]byte
}

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

// cgroupPathForKernel converts an absolute cgroup path to a kernel-relative path
func cgroupPathForKernel(absolutePath string) string {
	// Strip common prefixes
	prefixes := []string{
		"/sys/fs/cgroup/",
		"/sys/fs/cgroup",
	}

	for _, prefix := range prefixes {
		if strings.HasPrefix(absolutePath, prefix) {
			relativePath := strings.TrimPrefix(absolutePath, prefix)
			// Ensure we have a leading slash for the relative path
			if relativePath != "" && !strings.HasPrefix(relativePath, "/") {
				relativePath = "/" + relativePath
			}
			logrus.Infof("superfork: converted path '%s' -> '%s'", absolutePath, relativePath)
			return relativePath
		}
	}

	// If no prefix matched, return as-is (might already be relative)
	return absolutePath
}

func doSuperFork(context *cli.Context) error {
	sourceID := context.Args().Get(0)
	newID := context.Args().Get(1)
	root := context.GlobalString("root")
	if root == "" {
		return errors.New("root not set")
	}

	// Load and freeze source
	sourceContainer, err := libcontainer.Load(root, sourceID)
	if err != nil {
		return fmt.Errorf("failed to load source container: %w", err)
	}

	status, err := sourceContainer.Status()
	if err != nil {
		return fmt.Errorf("failed to get source status: %w", err)
	}

	if status != libcontainer.Running {
		return fmt.Errorf("source container must be running (current: %v)", status)
	}

	if err := sourceContainer.Pause(); err != nil {
		return fmt.Errorf("failed to freeze source: %w", err)
	}

	sourceThawed := false
	thawSource := func() {
		if sourceThawed {
			return
		}
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
	pids, err := sourceContainer.Processes()
	if err != nil {
		return err
	}
	if len(pids) == 0 {
		return fmt.Errorf("no processes in source container")
	}

	// Prepare new container
	_, spec, err := prepareNewContainer(context, sourceID, newID)
	if err != nil {
		return err
	}

	// Create new container
	newContainer, err := createContainer(context, newID, spec)
	if err != nil {
		return err
	}

	// Apply cgroup setup
	if err := newContainer.Apply(-1); err != nil {
		newContainer.Destroy()
		return err
	}

	// Get NEW container's cgroup path
	cgroupPath, err := getContainerCgroupPath(newContainer)
	if err != nil {
		newContainer.Destroy()
		return fmt.Errorf("failed to get new container cgroup path: %w", err)
	}

	// Verify cgroup exists
	if _, err := os.Stat(cgroupPath); err != nil {
		newContainer.Destroy()
		return fmt.Errorf("new container cgroup doesn't exist at %s: %w", cgroupPath, err)
	}

	// Convert to kernel-relative path
	kernelCgroupPath := cgroupPathForKernel(cgroupPath)

	// CRITICAL: Verify we have a valid path
	if kernelCgroupPath == "" || kernelCgroupPath == "/" {
		newContainer.Destroy()
		return fmt.Errorf("invalid kernel cgroup path: %s", kernelCgroupPath)
	}

	logrus.Infof("superfork: using cgroup path: %s (kernel: %s)", cgroupPath, kernelCgroupPath)

	// Call superfork syscall
	newInitPID, err := superforkSyscall(pids, newID, kernelCgroupPath)
	if err != nil {
		newContainer.Destroy()
		return fmt.Errorf("syscall failed: %w", err)
	}

	logrus.Infof("superfork: new init PID: %d", newInitPID)

	if err := updateContainerState(context, newContainer, int(newInitPID), root, newID); err != nil {
		logrus.Errorf("failed to update container state: %v", err)
	}

	if pidFile := context.String("pid-file"); pidFile != "" {
		if err := superforkCreatePidFile(pidFile, int(newInitPID)); err != nil {
			logrus.Errorf("failed to write pid file: %v", err)
		}
	}

	thawSource()

	if context.Bool("detach") {
		fmt.Printf("Container %s successfully forked to %s (init PID: %d)\n",
			sourceID, newID, newInitPID)
		return nil
	}

	fmt.Printf("\nContainer %s successfully forked to %s\n", sourceID, newID)
	fmt.Printf("New container PID: %d\n", newInitPID)
	fmt.Printf("Cgroup: %s\n", cgroupPath)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	return nil
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

func prepareNewContainer(context *cli.Context, sourceID, newID string) (
	string,
	*specs.Spec,
	error,
) {

	bundle := context.String("bundle")
	if bundle == "" {
		var err error
		bundle, err = os.Getwd()
		if err != nil {
			return "", nil, err
		}
	}

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
		// Try cgroup v2
		cgroupPath := filepath.Join("/sys/fs/cgroup", config.Cgroups.Path)
		if _, err := os.Stat(cgroupPath); err == nil {
			return cgroupPath, nil
		}

		// Try cgroup v1 (cpu controller)
		cgroupPath = filepath.Join("/sys/fs/cgroup/cpu", config.Cgroups.Path)
		if _, err := os.Stat(cgroupPath); err == nil {
			return cgroupPath, nil
		}

		// Try cgroup v1 (systemd)
		cgroupPath = filepath.Join("/sys/fs/cgroup/systemd", config.Cgroups.Path)
		if _, err := os.Stat(cgroupPath); err == nil {
			return cgroupPath, nil
		}
	}

	return "", fmt.Errorf("could not determine cgroup path")
}

func superforkSyscall(pids []int, containerID, cgroupPath string) (int32, error) {
	pidArr := make([]int32, len(pids))
	for i, p := range pids {
		pidArr[i] = int32(p)
	}

	cfg := ContainerConfig{}
	copy(cfg.ContainerID[:], containerID)
	copy(cfg.CgroupPath[:], cgroupPath)

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
