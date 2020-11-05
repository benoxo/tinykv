package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	txn    *badger.Txn
	it     engine_util.DBIterator
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	storage := &StandAloneStorage{}
	engine := engine_util.CreateDB(conf.DBPath, conf.Raft)
	storage.engine = engine_util.NewEngines(engine, engine, conf.DBPath, conf.DBPath)
	return storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Kv.Close()
}

func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	val, _ := engine_util.GetCF(s.engine.Kv, cf, key)
	return val, nil
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	s.txn = s.engine.Kv.NewTransaction(false)
	s.it = engine_util.NewCFIterator(cf, s.txn)
	return s.it
}

func (s *StandAloneStorage) Close() {
	s.txn.Discard()
	s.it.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			pm := m.Data.(storage.Put)
			wb.SetCF(pm.Cf, pm.Key, pm.Value)
			break
		case storage.Delete:
			dm := m.Data.(storage.Delete)
			wb.DeleteCF(dm.Cf, dm.Key)
			break
		}
	}
	return s.engine.WriteKV(wb)
}
