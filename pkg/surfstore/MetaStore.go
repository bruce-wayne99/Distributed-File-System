package surfstore

import (
	context "context"
	"fmt"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	mlock          sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.mlock.Lock()
	if _, ok := (m.FileMetaMap)[fileMetaData.Filename]; ok {
		if m.FileMetaMap[fileMetaData.Filename].Version >= fileMetaData.Version {
			m.mlock.Unlock()
			return &Version{
				Version: m.FileMetaMap[fileMetaData.Filename].Version,
			}, fmt.Errorf("old version update")
		}
	}

	m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	m.mlock.Unlock()
	return &Version{
		Version: fileMetaData.Version,
	}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
