package main

import (
    "flag"
    "fmt"
    "github.com/colinmarc/hdfs"
    "net/http"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "time"
)

type Flag struct {
    f bool
    h bool
    a bool
    put bool
    get bool
    mkdir bool
    ls bool
    rm bool
    fs string
    progress string
}

var ff = Flag {}

func init() {
    flag.BoolVar(&ff.h, "h", false, "print this help message")
    flag.BoolVar(&ff.f, "f", false, "force")
    flag.BoolVar(&ff.a, "a", false, "do not ignore hidden files")
    flag.BoolVar(&ff.put, "put", false, "put <localsrc> <dst>")
    flag.BoolVar(&ff.get, "get", false, "get <src> <localdst>")
    flag.BoolVar(&ff.mkdir, "mkdir", false, "mkdir <path>")
    flag.BoolVar(&ff.ls, "ls", false, "ls <path>")
    flag.BoolVar(&ff.rm, "rm", false, "rm <path>")
    flag.StringVar(&ff.fs, "fs", "192.168.199.100:9000", "filesystem URL")
    flag.StringVar(&ff.progress, "progress", "", "progress URL")
}

func put(src string, dst string) error {
    client, err := hdfs.New(ff.fs)
    if err != nil {
       fmt.Printf("Connect to hdfs failed: %s", err.Error())
       return err
    }

    sfi, err := os.Stat(src)
    if err != nil {
        return err
    }

    if sfi.IsDir() {
        dfi, err := client.Stat(dst)
        if err == nil && !dfi.IsDir() {
            fmt.Printf("File already exist, path: %s\n", dst)
            return err
        }

        var files []string
        var dirs []string
        // walk
        _ = filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
            newPath := strings.Replace(path, src, dst, 1)
            if info.IsDir() {
                dirs = append(dirs, newPath)
            } else {
                files = append(files, newPath)
            }
            return nil
        })

        total := len(files) + 1
        count := 0
        httpClient := &http.Client{
            Timeout: time.Duration(1 * time.Second),
        }
        for _, d := range dirs {
            err = client.MkdirAll(d, 0644)
            if err != nil {
                fmt.Printf("Mkdir failed, path: %s, error: %s\n", d, err.Error())
                return err
            }
        }
        count += 1
        for _, f := range files {
            srcPath := strings.Replace(f, dst, src, 1)
            needSkip := false
            fi, err := client.Stat(f)
            if err == nil && fi != nil {
                // delete old file when size mismatch
                ssfi, err := os.Stat(srcPath)
                if err != nil {
                    fmt.Printf("Stat file failed, file: %s, error: %s\n", srcPath, err.Error())
                    return err
                }
                if ssfi.Size() == fi.Size() {
                    needSkip = true
                } else {
                    _ = client.RemoveAll(f)
                }
            }

            if !needSkip {
                if  ff.a || !strings.HasPrefix(filepath.Base(srcPath), ".") {
                    err = client.CopyToRemote(srcPath, f)
                    if err != nil {
                        fmt.Printf("Put file failed, file: %s, error: %s\n", srcPath, err.Error())
                        return err
                    }
                }
            }

            count += 1
            if ff.progress != "" && count % 5 == 0 {
                progress := strconv.FormatFloat(float64(count * 100.0 / total), 'f', 2, 64)
                payload := strings.NewReader("path=" + dst + "&progress=" + progress)
                req, _ := http.NewRequest("POST", ff.progress, payload)
                req.Header.Set("Content-Type", "application/x-www-form-urlencoded;param=value")
                _, _ = httpClient.Do(req)
            }
        }
    }

    return nil
}

func get(src string, dst string) error {
    client, err := hdfs.New(ff.fs)
    if err != nil {
        fmt.Printf("Connect failed: %s", err.Error())
        return err
    }

    sfi, err := client.Stat(src)
    if err != nil {
        fmt.Printf("File not exist, file: %s\n", src)
        return err
    }

    var files []string
    var dirs []string

    if sfi.IsDir() {
        // walk
        _ = client.Walk(src, func(path string, info os.FileInfo, err error) error {
            newPath := strings.Replace(path, src, dst, 1)
            if info.IsDir() {
                dirs = append(dirs, newPath)
            } else {
                files = append(files, newPath)
            }
            return nil
        })
    } else {
        files = append(files, strings.Replace(src, src, dst, 1))
    }

    for _, d := range dirs {
        err = os.MkdirAll(d, 0751)
        if err != nil {
            fmt.Printf("Mkdir failed, path: %s, error: %s\n", d, err.Error())
            return err
        }
    }

    for _, f := range files {
        srcPath := strings.Replace(f, dst, src, 1)
        content, err := client.ReadFile(srcPath)
        if err != nil {
            fmt.Printf("Open file failed, file: %s, error: %s\n", src, err.Error())
            return err
        }

        dfp, err := os.Create(f)
        if err != nil {
            fmt.Printf("Create file failed, file: %s, error: %s\n", f, err.Error())
            return err
        }

        _, err = dfp.Write(content)
        if err != nil {
            fmt.Printf("Write file failed, file: %s, error: %s\n", f, err.Error())
            return err
        }

        err = dfp.Sync()
        if err != nil {
            fmt.Printf("Sync file failed, file: %s, error: %s\n", f, err.Error())
            return err
        }

        err = dfp.Close()
        if err != nil {
            fmt.Printf("Close file failed, file: %s, error: %s\n", f, err.Error())
            return err
        }
    }

    return nil
}

func ls(path string) error {
    client, err := hdfs.New(ff.fs)
    if err != nil {
        fmt.Printf("Connect failed: %s", err.Error())
        return err
    }

    fi, err := client.Stat(path)
    if err != nil {
        fmt.Printf("Path not exist, file: %s\n", path)
        return err
    }

    if fi.IsDir() {
        files, err := client.ReadDir(path)
        if err != nil {
            return err
        }

        for _, f := range files {
            fmt.Printf("%-13s %-8d %-20s %-30s\n", f.Mode(), f.Size(), f.ModTime().String()[:19], f.Name())
        }
    } else {
        fmt.Printf("%-13s %-8d %-20s %-30s\n", fi.Mode(), fi.Size(), fi.ModTime().String()[:19], fi.Name())
    }
    fmt.Println()

    return nil
}

func rm(path string) error {
    client, err := hdfs.New(ff.fs)
    if err != nil {
        fmt.Printf("Connect failed: %s", err.Error())
        return err
    }

    err = client.RemoveAll(path)
    if err != nil {
        fmt.Printf("Rm path failed, path: %s, error: %s", path, err.Error())
        return err
    }

    return nil
}

func mkdir(path string) error {
    client, err := hdfs.New(ff.fs)
    if err != nil {
        fmt.Printf("Connect failed: %s", err.Error())
        return err
    }

    err = client.MkdirAll(path, os.ModeDir)
    if err != nil {
        fmt.Printf("Mkdir failed, path: %s, error: %s", path, err.Error())
        return err
    }

    return nil
}

func main() {
    flag.Parse()
    if ff.h {
        flag.Usage()
        return
    }

    if ff.put {
        args := flag.Args()
        if len(args) != 2 {
            fmt.Printf("Invalid args, command put require 2 args")
            os.Exit(-1)
        }
        err := put(args[0], args[1])
        if err != nil {
            os.Exit(-1)
        }
        return
    }

    if ff.get {
        args := flag.Args()
        if len(args) != 2 {
            fmt.Printf("Invalid args, command get require 2 args")
            os.Exit(-1)
        }
        _ = get(args[0], args[1])
        return
    }

    if ff.mkdir {
        args := flag.Args()
        if len(args) != 1 {
            fmt.Printf("Invalid args, command mkdir require 1 arg")
            os.Exit(-1)
        }
        _ = mkdir(args[0])
        return
    }

    if ff.rm {
        args := flag.Args()
        if len(args) != 1 {
            fmt.Printf("Invalid args, command rm require 1 arg")
            os.Exit(-1)
        }
        _ = rm(args[0])
        return
    }

    if ff.ls {
        args := flag.Args()
        if len(args) != 1 {
            fmt.Printf("Invalid args, command ls require 1 arg")
            os.Exit(-1)
        }
        _ = ls(args[0])
        return
    }

    flag.Usage()
}
