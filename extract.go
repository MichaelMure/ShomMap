package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/kjk/lzmadec"
	"github.com/tealeg/xlsx"
	"cartemaritime/common"
)


func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: ./extract file.xlsx")
		os.Exit(1)
	}

	if err := ensureMkdir(common.TMPDIR); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := ensureMkdir(common.DATADIR); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	filename := os.Args[1]

	items := parseXlsx(filename)

	// Run a pipeline parse --> download --> extract
	// to avoid blocking the download during the extraction
	for item := range items {
		if itemExist(item) {
			fmt.Printf("%s exist, skipping\n", item.name)
			continue
		}

		filepath, err := download(item)

		if err != nil {
			fmt.Printf("Error while downloading %s: %s\n", item.name, err)
			continue
		}

		go extract(item, filepath)
	}
}

func ensureMkdir(path string) error {
	_, err := os.Stat(path)

	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		if err := os.Mkdir(path, 0777); err != nil {
			return err
		}
	}

	return nil
}

type Item struct {
	name string
	url  string
}

func parseXlsx(filename string) <-chan Item {
	xlFile, err := xlsx.OpenFile(filename)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	out := make(chan Item)

	go func() {
		defer close(out)

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

				out <- Item{
					name: fmt.Sprintf("%d_%s", counter, row.Cells[numColumn].String()),
					url:  row.Cells[urlColumn].String(),
				}

				counter++
			}

			urlColumn = -1
		}
	}()

	return out
}

func itemExist(item Item) bool {
	dir := path.Join(common.DATADIR, item.name)

	_, err := os.Stat(dir)

	return err == nil
}

func download(item Item) (string, error) {
	fmt.Println("Downloading", item.name, "...")

	filepath := path.Join(common.TMPDIR, item.name+".7z")

	response, err := http.Get(item.url)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	output, err := os.Create(filepath)
	if err != nil {
		return "", err
	}
	defer output.Close()

	n, err := io.Copy(output, response.Body)
	if err != nil {
		return "", err
	}

	fmt.Println(n, "bytes downloaded.")

	return filepath, nil
}

func extract(item Item, filepath string) {
	archive, err := lzmadec.NewArchive(filepath)
	if err != nil {
		fmt.Println(err)
		return
	}

	dir := path.Join(common.DATADIR, item.name)

	if err := os.Mkdir(dir, 0777); err != nil {
		fmt.Println(err)
		return
	}

	extracted := 0

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

		extracted++
	}

	fmt.Printf("Extracted %d files\n", extracted)
}
