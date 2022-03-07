package surfstore

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Implement the logic for a client syncing with the server here.

type FileUpdateInfo struct {
	operation     int // 0: create, 1: update, 2: delete, 3: no change
	blockHashList []string
	fileName      string
	version       int32
}

func arraysEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func isValidFile(f fs.FileInfo) bool {
	if f.IsDir() {
		return false
	}
	if f.Name() == "index.txt" {
		return false
	}
	if strings.Contains(f.Name(), "/") {
		return false
	}
	if strings.Contains(f.Name(), ",") {
		return false
	}
	return true
}

func getHash(buf []byte, bytesRead int) string {
	hashBytes := sha256.Sum256(buf[:bytesRead])
	return hex.EncodeToString(hashBytes[:])
}

func getContentFromLine(text string) (string, int32, []string) {
	fName := strings.Split(text, ",")[0]
	version, _ := strconv.Atoi(strings.Split(text, ",")[1])
	hashString := strings.Split(text, ",")[2]
	hashList := strings.Split(hashString, " ")
	return fName, int32(version), hashList[:len(hashList)-1]
}

func updateFields(client RPCClient, fileInfoMap *(map[string]*FileUpdateInfo)) {
	f, err := os.Open(filepath.Clean(client.BaseDir + "/index.txt"))
	if err != nil {
		log.Fatal("failed to open index.txt")
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	delHashList := make([]string, 0)
	delHashList = append(delHashList, "0")

	// indexMap := make(map[string]*FileUpdateInfo)

	for scanner.Scan() {
		fName, version, hashList := getContentFromLine(scanner.Text())
		if _, ok := (*fileInfoMap)[fName]; ok {
			if arraysEqual((*fileInfoMap)[fName].blockHashList, hashList) {
				(*fileInfoMap)[fName].operation = 3 // no change
			} else {
				(*fileInfoMap)[fName].operation = 1 // update
				(*fileInfoMap)[fName].version = version + 1
			}
		} else {
			if arraysEqual(hashList, delHashList) { // file was already deleted no change
				(*fileInfoMap)[fName] = &FileUpdateInfo{
					fileName:      fName,
					version:       version,
					operation:     3, // no change
					blockHashList: delHashList,
				}
			} else { // file deleted now
				(*fileInfoMap)[fName] = &FileUpdateInfo{
					fileName:      fName,
					version:       version + 1,
					operation:     2, // delete
					blockHashList: delHashList,
				}
			}
		}
	}
}

func computeHashList(client RPCClient, fileName string) []string {
	f, err := os.Open(filepath.Clean(client.BaseDir + "/" + fileName))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, client.BlockSize)
	var mainbuf []byte

	var arr []string

	for {
		bytesRead, err := f.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}
		tempbuf := buf[:bytesRead]
		mainbuf = append(mainbuf, tempbuf...)
		if len(mainbuf) >= client.BlockSize {
			arr = append(arr, getHash(mainbuf[:client.BlockSize], client.BlockSize))
			mainbuf = mainbuf[client.BlockSize:]
		}
	}
	if len(mainbuf) > 0 {
		arr = append(arr, getHash(mainbuf, len(mainbuf)))
	}
	return arr
}

func getFileFromMetaStore(client RPCClient, remoteIndex *map[string]*FileMetaData,
	fileUpdateMap *map[string]*FileUpdateInfo, fileName string, blockStoreAddr string) {

	if checkDeletedFile((*remoteIndex)[fileName].BlockHashList) {
		return
	}

	f, err := os.Create(filepath.Clean(client.BaseDir + "/" + fileName))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	for _, hash := range (*remoteIndex)[fileName].BlockHashList {
		var block Block
		client.GetBlock(hash, blockStoreAddr, &block)
		_, err := f.Write(block.BlockData)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func getUpdatedFileFromMetaStore(client RPCClient, remoteIndex *map[string]*FileMetaData,
	fileUpdateMap *map[string]*FileUpdateInfo, fileName string, blockStoreAddr string) {
	if checkDeletedFile((*remoteIndex)[fileName].BlockHashList) {
		if !checkDeletedFile((*fileUpdateMap)[fileName].blockHashList) {
			os.Remove(filepath.Clean(client.BaseDir + "/" + fileName))
		}
		(*fileUpdateMap)[fileName].version = (*remoteIndex)[fileName].Version
		(*fileUpdateMap)[fileName].blockHashList = (*remoteIndex)[fileName].BlockHashList
		(*fileUpdateMap)[fileName].fileName = (*remoteIndex)[fileName].Filename
		(*fileUpdateMap)[fileName].operation = 3
		return
	}

	var f *os.File
	blocksMap := make(map[string]*Block)
	presentBlocks := make([]string, 0)
	blocksAlreadyPresent := make([]string, 0)

	// if checkDeletedFile((*fileUpdateMap)[fileName].blockHashList) { // create file if already deleted
	// 	f, err2 := os.Create(filepath.Clean(client.BaseDir + "/" + fileName))
	// 	if err2 != nil {
	// 		log.Fatal(err2)
	// 	}
	// 	f.Close()
	// }

	f, err2 := os.Create(filepath.Clean(client.BaseDir + "/" + fileName))
	if err2 != nil {
		log.Fatal(err2)
	}
	buf := make([]byte, client.BlockSize)
	var mainbuf []byte
	for {
		bytesRead, err := f.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}
		tempbuf := buf[:bytesRead]
		mainbuf = append(mainbuf, tempbuf...)

		if len(mainbuf) >= client.BlockSize {
			hashval := getHash(mainbuf[:client.BlockSize], client.BlockSize)
			if _, ok := blocksMap[hashval]; !ok {
				blocksMap[hashval] = &Block{BlockData: mainbuf[:client.BlockSize], BlockSize: int32(client.BlockSize)}
				presentBlocks = append(presentBlocks, hashval)
			}
			mainbuf = mainbuf[client.BlockSize:]
		}
		// hashval := getHash(buf, bytesRead)
		// if _, ok := blocksMap[hashval]; !ok {
		// 	blocksMap[hashval] = &Block{BlockData: tmp, BlockSize: int32(bytesRead)}
		// 	presentBlocks = append(presentBlocks, hashval)
		// }
	}
	if len(mainbuf) > 0 {
		hashval := getHash(mainbuf, len(mainbuf))
		if _, ok := blocksMap[hashval]; !ok {
			blocksMap[hashval] = &Block{BlockData: mainbuf, BlockSize: int32(len(mainbuf))}
			presentBlocks = append(presentBlocks, hashval)
		}
	}
	defer f.Close()

	client.HasBlocks(presentBlocks, blockStoreAddr, &blocksAlreadyPresent)
	blocksToBeFetched := differenceArrays((*remoteIndex)[fileName].BlockHashList, blocksAlreadyPresent)

	for _, hash := range blocksToBeFetched {
		var block Block
		err := client.GetBlock(hash, blockStoreAddr, &block)
		if err != nil {
			log.Fatal(err)
		}
		blocksMap[hash] = &Block{
			BlockData: block.BlockData,
			BlockSize: block.BlockSize,
		}
	}

	for _, hash := range (*remoteIndex)[fileName].BlockHashList {
		_, err2 := f.Write(blocksMap[hash].BlockData)
		if err2 != nil {
			log.Fatal(err2)
		}
	}

	(*fileUpdateMap)[fileName].version = (*remoteIndex)[fileName].Version
	(*fileUpdateMap)[fileName].blockHashList = (*remoteIndex)[fileName].BlockHashList
	(*fileUpdateMap)[fileName].fileName = (*remoteIndex)[fileName].Filename
	(*fileUpdateMap)[fileName].operation = 3
}

func sendFileToMetaStore(client RPCClient, remoteIndex *map[string]*FileMetaData,
	fileUpdateMap *map[string]*FileUpdateInfo, fileName string, blockStoreAddr string) error {

	if checkDeletedFile((*fileUpdateMap)[fileName].blockHashList) {
		return client.UpdateFile(&FileMetaData{
			Filename:      fileName,
			Version:       (*fileUpdateMap)[fileName].version,
			BlockHashList: (*fileUpdateMap)[fileName].blockHashList,
		}, &(*fileUpdateMap)[fileName].version)
	}
	f, err := os.Open(filepath.Clean(client.BaseDir + "/" + fileName))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, client.BlockSize)
	var mainbuf []byte
	blocksMap := make(map[string]*Block)
	for {
		bytesRead, err := f.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}
		tempbuf := buf[:bytesRead]
		mainbuf = append(mainbuf, tempbuf...)

		if len(mainbuf) >= client.BlockSize {
			blocksMap[getHash(buf, client.BlockSize)] = &Block{BlockData: mainbuf[:client.BlockSize], BlockSize: int32(client.BlockSize)}
			mainbuf = mainbuf[client.BlockSize:]
		}
	}

	if len(mainbuf) > 0 {
		blocksMap[getHash(buf, len(mainbuf))] = &Block{BlockData: mainbuf, BlockSize: int32(len(mainbuf))}
	}

	blocksAlreadyPresent := make([]string, 0)
	client.HasBlocks((*fileUpdateMap)[fileName].blockHashList, blockStoreAddr, &blocksAlreadyPresent)
	blocksToBeSent := differenceArrays((*fileUpdateMap)[fileName].blockHashList, blocksAlreadyPresent)

	for _, hash := range blocksToBeSent {
		var succ bool
		err := client.PutBlock(blocksMap[hash], blockStoreAddr, &succ)
		if err != nil {
			log.Fatal(err)
		}
	}

	return client.UpdateFile(&FileMetaData{
		Filename:      fileName,
		Version:       (*fileUpdateMap)[fileName].version,
		BlockHashList: (*fileUpdateMap)[fileName].blockHashList,
	}, &(*fileUpdateMap)[fileName].version)

}

func handleUpdateCase(client RPCClient, remoteIndex *map[string]*FileMetaData,
	fileUpdateMap *map[string]*FileUpdateInfo, fileName string, blockStoreAddr string) {
	if (*fileUpdateMap)[fileName].operation == 3 { // no change
		if (*remoteIndex)[fileName].Version > (*fileUpdateMap)[fileName].version {
			getUpdatedFileFromMetaStore(client, remoteIndex, fileUpdateMap, fileName, blockStoreAddr)
		}
	} else if (*fileUpdateMap)[fileName].operation == 0 { // new file already failed sync from meta store
		newRemoteIndex := make(map[string]*FileMetaData)
		client.GetFileInfoMap(&newRemoteIndex)
		getUpdatedFileFromMetaStore(client, &newRemoteIndex, fileUpdateMap, fileName, blockStoreAddr)
	} else if (*fileUpdateMap)[fileName].operation == 1 || (*fileUpdateMap)[fileName].operation == 2 { // try to update and if fails sync from meta store
		if (*fileUpdateMap)[fileName].version > (*remoteIndex)[fileName].Version {
			err := sendFileToMetaStore(client, remoteIndex, fileUpdateMap, fileName, blockStoreAddr)
			if err == nil {
				(*fileUpdateMap)[fileName].operation = 3
				return
			}
			//send blocks and update file meta data
			//if above operation succeeds return
		}
		newRemoteIndex := make(map[string]*FileMetaData)
		client.GetFileInfoMap(&newRemoteIndex)
		getUpdatedFileFromMetaStore(client, &newRemoteIndex, fileUpdateMap, fileName, blockStoreAddr)
	}
	// else if (*fileUpdateMap)[fileName].operation == 2 { // try to send update and if fails sync from meta store

	// }
}

func ClientSync(client RPCClient) {

	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}

	var doesIndexExist bool

	var fileList []string
	for _, f := range files {
		if isValidFile(f) {
			fileList = append(fileList, f.Name())
		}
		if f.Name() == "index.txt" {
			doesIndexExist = true
		}
	}

	fileUpdateMap := make(map[string]*FileUpdateInfo)

	for _, fName := range fileList {
		fileUpdateMap[fName] = &FileUpdateInfo{
			fileName:      fName,
			operation:     0, // default create
			blockHashList: computeHashList(client, fName),
			version:       1,
		}
	}

	if doesIndexExist {
		updateFields(client, &fileUpdateMap)
	}

	remoteIndex := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&remoteIndex)

	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		log.Fatal(err)
	}

	for fileName, _ := range remoteIndex {
		if _, ok := (fileUpdateMap)[fileName]; ok { // both have the files, check for updating
			handleUpdateCase(client, &remoteIndex, &fileUpdateMap, fileName, blockStoreAddr)
		} else { // only metastore has the file download it, check if its a delete file
			getFileFromMetaStore(client, &remoteIndex, &fileUpdateMap, fileName, blockStoreAddr)
			fileUpdateMap[fileName] = &FileUpdateInfo{
				fileName:      fileName,
				blockHashList: remoteIndex[fileName].BlockHashList,
				operation:     3,
				version:       remoteIndex[fileName].Version,
			}
		}
	}

	for fileName, _ := range fileUpdateMap {
		if _, ok := remoteIndex[fileName]; !ok { // only current folder has it send it to metaStore
			err := sendFileToMetaStore(client, &remoteIndex, &fileUpdateMap, fileName, blockStoreAddr)
			if err == nil {
				fileUpdateMap[fileName].operation = 3
			} else {
				// handle update
				handleUpdateCase(client, &remoteIndex, &fileUpdateMap, fileName, blockStoreAddr)
			}
		}
	}

	WriteMetaFileNew(fileUpdateMap, client.BaseDir)

	// log.Println("Got block address: " + blockStoreAddr)

	// blockHash := "1234"
	// var block Block
	// if err := client.GetBlock(blockHash, blockStoreAddr, &block); err != nil {
	// 	log.Fatal(err)
	// }

	// log.Print(block.String())
}
