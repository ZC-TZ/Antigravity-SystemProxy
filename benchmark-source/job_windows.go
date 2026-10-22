package main

import (
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"syscall"
	"unicode/utf16"
	"unsafe"
)

type jobLimits struct {
	ProcessTime, JobTime                                       int64
	Flags                                                      uint32
	MinimumWS, MaximumWS                                       uintptr
	ActiveLimit                                                uint32
	Affinity                                                   uintptr
	Priority, Scheduling                                       uint32
	IO                                                         [6]uint64
	ProcessMemory, JobMemory, PeakProcessMemory, PeakJobMemory uintptr
}

// Assign before ResumeThread: otherwise a fast launcher could spawn children
// outside the job. Closing the job then covers even unpublished startup PIDs.
func launchInJob(cmd *exec.Cmd) (job, process syscall.Handle, pid int, err error) {
	j, _, callErr := kernel.NewProc("CreateJobObjectW").Call(0, 0)
	if j == 0 {
		return 0, 0, 0, callErr
	}
	job = syscall.Handle(j)
	defer func() {
		if err != nil {
			syscall.CloseHandle(job)
			job = 0
		}
	}()
	limits := jobLimits{Flags: 0x2000} // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	ok, _, callErr := kernel.NewProc("SetInformationJobObject").Call(j, 9, uintptr(unsafe.Pointer(&limits)), unsafe.Sizeof(limits))
	if ok == 0 {
		return job, 0, 0, callErr
	}
	app, err := syscall.UTF16PtrFromString(cmd.Path)
	if err != nil {
		return job, 0, 0, err
	}
	args := make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		args[i] = syscall.EscapeArg(arg)
	}
	line, err := syscall.UTF16PtrFromString(strings.Join(args, " "))
	if err != nil {
		return job, 0, 0, err
	}
	env := append([]string(nil), cmd.Env...)
	sort.Slice(env, func(i, j int) bool { return strings.ToUpper(env[i]) < strings.ToUpper(env[j]) })
	for _, pair := range env {
		if strings.ContainsRune(pair, 0) {
			return job, 0, 0, errors.New("invalid benchmark environment")
		}
	}
	block := utf16.Encode([]rune(strings.Join(env, "\x00") + "\x00\x00"))
	si := syscall.StartupInfo{Cb: uint32(unsafe.Sizeof(syscall.StartupInfo{})), Flags: 1, ShowWindow: 0}
	var pi syscall.ProcessInformation
	err = syscall.CreateProcess(app, line, nil, nil, false, 0x08000404, &block[0], nil, &si, &pi)
	if err != nil {
		return job, 0, 0, err
	}
	defer syscall.CloseHandle(pi.Thread)
	ok, _, callErr = kernel.NewProc("AssignProcessToJobObject").Call(j, uintptr(pi.Process))
	if ok == 0 {
		_ = syscall.TerminateProcess(pi.Process, 1)
		syscall.WaitForSingleObject(pi.Process, 5000)
		syscall.CloseHandle(pi.Process)
		return job, 0, 0, fmt.Errorf("isolated benchmark job assignment: %w", callErr)
	}
	res, _, callErr := kernel.NewProc("ResumeThread").Call(uintptr(pi.Thread))
	if res == 0xffffffff {
		_ = syscall.TerminateProcess(pi.Process, 1)
		syscall.CloseHandle(pi.Process)
		return job, 0, 0, callErr
	}
	return job, pi.Process, int(pi.ProcessId), nil
}
