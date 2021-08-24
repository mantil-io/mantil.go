package shell

import (
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
)

type ExecOptions struct {
	Args           []string
	Env            []string
	WorkDir        string
	SucessStatuses []int
	Logger         func(format string, v ...interface{})
	ErrorsMap      map[string]error
}

func Exec(opt ExecOptions) error {
	r := runner{
		dir:       opt.WorkDir,
		verbose:   true,
		output:    opt.Logger,
		errorsMap: opt.ErrorsMap,
	}
	if opt.Logger == nil {
		var std = log.New(os.Stderr, log.Prefix(), 0)
		r.output = func(format string, v ...interface{}) {
			std.Printf(format, v...)
		}
	}
	if opt.Env != nil {
		r.env = addCurrentPath(opt.Env)
	}
	return r.runCmd(opt.Args, opt.SucessStatuses...)
}

func addCurrentPath(env []string) []string {
	for _, s := range env {
		if strings.HasPrefix(s, "PATH") {
			return env
		}
	}
	val, ok := os.LookupEnv("PATH")
	if !ok {
		return env
	}
	return append(env, "PATH="+val)
}

type runner struct {
	verbose   bool
	dir       string
	env       []string
	output    func(format string, v ...interface{})
	errorsMap map[string]error
	err       error
}

func (r *runner) runCmd(args []string, successStatuses ...int) error {
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = r.dir
	if r.env != nil {
		cmd.Env = r.env
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	printCmd := func() {
		r.output(">> %s", strings.Join(args, " "))
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	if r.verbose {
		printCmd()
		if err := r.printToConsole(stdout); err != nil {
			return err
		}
		if err := r.printToConsole(stderr); err != nil {
			return err
		}
	}
	err = cmd.Wait()
	exitCode := exitCode(err)
	//r.output("  command done exit code: %s", exitCode)
	for _, ss := range successStatuses {
		if exitCode == ss {
			exitCode = 0
			err = nil
		}
	}
	if exitCode != 0 {
		if !r.verbose {
			printCmd()
		}
		r.output("FAILED with exit status %d", exitCode)
	}
	if r.err != nil {
		return r.err
	}
	return err
}

func (r *runner) printToConsole(rdr io.ReadCloser) error {
	buf := make([]byte, 1024*16)
	for {
		n, err := rdr.Read(buf[:])
		if n > 0 {
			//fmt.Printf("rdr.Read n = %d\n", n)
			//fmt.Printf("%s", buf[:n])
			for _, line := range strings.Split(string(buf[:n]), "\n") {
				if len(line) == 0 || line == "\n" {
					continue
				}
				// find error in line
				if r.err == nil && len(r.errorsMap) > 0 {
					for k, v := range r.errorsMap {
						if strings.Contains(line, k) {
							r.err = v
						}
					}
				}
				r.output("%s", line)
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return 127
}
