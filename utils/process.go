package utils

// https://github.com/yuwf/gobase

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"

	"github.com/rs/zerolog/log"
)

var (
	processWG       sync.WaitGroup
	processQuit     = make(chan struct{})
	processExitCall []func(os.Signal)
	exiting         bool
)

func Daemon() {
	envName := "__DAEMON_K_"  //环境变量名称
	envValue := "__DAEMON_V_" //环境变量值

	val := os.Getenv(envName) //读取环境变量的值,若未设置则为空字符串
	if val == envValue {      //监测到特殊标识, 判断为子进程,不再执行后续代码
		return
	}

	/*以下是父进程执行的代码*/

	//因为要设置更多的属性, 这里不使用`exec.Command`方法, 直接初始化`exec.Cmd`结构体
	cmd := &exec.Cmd{
		Path: os.Args[0],
		Args: os.Args,      //注意,此处是包含程序名的
		Env:  os.Environ(), //父进程中的所有环境变量
	}
	//为子进程设置特殊的环境变量标识
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", envName, envValue))

	//异步启动子进程
	err := cmd.Start()
	if err != nil {
		fmt.Println("Start error:", err)
		os.Exit(1)
	}

	fmt.Println("Process pid:", cmd.Process.Pid)
	os.Exit(0)
}

// 进程ID写入pid文件
func CreatePid() bool {
	f, err := os.Create("pid")
	if err != nil {
		log.Error().Err(err).Msg("CreatePid")
		return false
	}
	pid := fmt.Sprintf("%v", os.Getpid())
	_, err = f.Write([]byte(pid))
	f.Close()
	if err != nil {
		log.Error().Err(err).Msg("CreatePid")
		return false
	}
	return true
}

// 注册退出函数
func RegExit(fn func(os.Signal)) {
	processExitCall = append(processExitCall, fn)
}

// Exit 退出
func Exit(code int) {
	close(processQuit)
	processWG.Wait()
	os.Exit(code)
}

// 等待退出，一般放到main函数的最下面调用
func ExitWait() {
	processWG.Add(1)
	go func() {
		defer processWG.Done()
		for {
			sigchan := make(chan os.Signal, 1)
			signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

			var sig os.Signal
			select {
			case sig = <-sigchan:
				switch sig {
				case syscall.SIGHUP:
					log.Warn().Msg("Process Caught SIGHUP. Ignoring") // 终端关闭
					continue
				case os.Interrupt:
					log.Warn().Msg("Process Caught SIGINT. Exiting")
				case syscall.SIGTERM:
					log.Warn().Msg("Process Caught SIGTERM. Exiting")
				default:
					log.Warn().Msgf("Process Caught signal %v. Exiting", sig)
				}
			case <-processQuit:
				log.Info().Msg("Process Exiting")
			}
			exiting = true
			// 处理注册的退出函数
			for _, fn := range processExitCall {
				if fn != nil {
					func() {
						defer func() { recover() }()
						fn(sig)
					}()
				}
			}
			break
		}
	}()

	processWG.Wait()

	log.Info().Msg("Process Exited")
	// 移除pid文件
	os.Remove("pid")
}

func Exiting() bool {
	return exiting
}
