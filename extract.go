package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/tealeg/xlsx"
	"github.com/kjk/lzmadec"
)

const (
	TMPDIR  = "./tmp"
	DATADIR = "./result"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: ./extract file.xlsx")
		os.Exit(1)
	}

	// TODO: remove, for test only
	if err := os.RemoveAll(TMPDIR); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := os.Mkdir(TMPDIR, 0777); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// TODO: remove, for test only
	if err := os.RemoveAll(DATADIR); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := os.Mkdir(DATADIR, 0777); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	filename := os.Args[1]
	parseXlsx(filename)
}

func parseXlsx(filename string) {
	xlFile, err := xlsx.OpenFile(filename)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	numColumn := 0
	urlColumn := -1
	counter := 1

	for _, sheet := range xlFile.Sheets {
		for rowIndex, row := range sheet.Rows {

			// Extract index of column with url
			if rowIndex == 0 {
				for columnIndex, cell := range row.Cells {
					if strings.Contains(strings.ToLower(cell.String()), "lien") {
						urlColumn = columnIndex
					}
				}
				// skip header
				continue
			}

			if urlColumn < 0 {
				fmt.Println("Error: Column with url not found for sheet", sheet.Name)
				os.Exit(-1)
			}

			num := fmt.Sprintf("%d_%s", counter, row.Cells[numColumn].String())
			url := row.Cells[urlColumn].String()
			counter++

			download(num, url)
		}

		urlColumn = -1
	}
}

func download(name string, url string) {
	fmt.Println("Downloading", name, "...")

	filepath := path.Join(TMPDIR, name+".7z")

	response, err := http.Get(url)
	if err != nil {
		fmt.Println("Error while downloading", url, "-", err)
		return
	}
	defer response.Body.Close()

	output, err := os.Create(filepath)
	if err != nil {
		fmt.Println("Error while creating", filepath, "-", err)
		return
	}
	defer output.Close()

	n, err := io.Copy(output, response.Body)
	if err != nil {
		fmt.Println("Error while writing", url, "-", err)
		return
	}

	fmt.Println(n, "bytes downloaded.")

	extract(name, filepath)
}

func extract(name string, filepath string) {
	archive, err := lzmadec.NewArchive(filepath)
	if err != nil {
		fmt.Println(err)
		return
	}

	dir := path.Join(DATADIR, name)

	if err := os.Mkdir(dir, 0777); err != nil {
		fmt.Println(err)
		return
	}

	for _, e := range archive.Entries {
		if path.Ext(e.Path) == "" {
			// Kinda hacky, consider no extension as directories and ignore
			continue
		}

		// flatten the hierarchy
		filename := path.Base(e.Path)

		err = archive.ExtractToFile(path.Join(dir, filename), e.Path)
		if err != nil {
			fmt.Println("Failed to extract", e.Path, err)
			return
		}
	}

	fmt.Printf("Extracted %d files\n", len(archive.Entries))
}
