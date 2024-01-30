package gosvl

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"log"
	"reflect"
	"strings"
)

type BasicTransaction struct {
	Key   []byte
	Value []byte
}

type SVL struct {
	encodeElements bool
	set            func(t []BasicTransaction) error
	get            func(t []BasicTransaction) ([]BasicTransaction, error)
	iterate        func(selector func(key string) int) ([]BasicTransaction, error)
	increments     map[string]func(value any) any
	encoder        func(e any) ([]byte, error)
	decoder        func(e any, v []byte) error
	logger         func(v ...any)
}

type SvlBuilder struct {
	svl *SVL
}

func NewSVL() *SvlBuilder {
	builder := &SvlBuilder{}
	builder.svl = &SVL{}
	builder.svl.encodeElements = false
	builder.svl.increments = make(map[string]func(value any) any)
	return builder
}

func (s *SvlBuilder) SetBadger(db *badger.DB) *SvlBuilder {
	setF := func(transactions []BasicTransaction) error {
		return db.Update(func(txn *badger.Txn) error {
			for _, transaction := range transactions {
				if err := txn.Set(transaction.Key, transaction.Value); err != nil {
					return err
				}
			}
			return nil
		})
	}
	getF := func(transactions []BasicTransaction) ([]BasicTransaction, error) {
		var values []BasicTransaction
		err := db.View(func(txn *badger.Txn) error {
			for _, transaction := range transactions {
				item, err := txn.Get(transaction.Key)
				if err != nil {
					if errors.Is(err, badger.ErrKeyNotFound) {
						continue
					}
					return err
				}
				var value []byte
				value, err = item.ValueCopy(nil)
				if err != nil {
					return err
				}
				values = append(values, BasicTransaction{Value: value})
			}
			return nil
		})
		return values, err
	}
	iteratorF := func(selector func(key string) int) ([]BasicTransaction, error) {
		var values []BasicTransaction
		err := db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				k := string(item.Key())
				operation := selector(k)
				if operation >= 0 {
					var value []byte
					value, err := item.ValueCopy(nil)
					if err != nil {
						return err
					}
					values = append(values, BasicTransaction{Key: item.Key(), Value: value})
				}
				if operation == 1 {
					return nil
				}
			}
			return nil
		})
		return values, err
	}
	s.svl.set = setF
	s.svl.get = getF
	s.svl.iterate = iteratorF
	return s
}

func (s *SvlBuilder) SetEncodeElements(elements bool) *SvlBuilder {
	s.svl.encodeElements = elements
	return s
}

func (s *SvlBuilder) ImplementEncoder(f func(e any) ([]byte, error)) *SvlBuilder {
	s.svl.encoder = f
	return s
}

func (s *SvlBuilder) ImplementSet(f func(t []BasicTransaction) error) *SvlBuilder {
	s.svl.set = f
	return s
}

func (s *SvlBuilder) ImplementGet(f func(t []BasicTransaction) ([]BasicTransaction, error)) *SvlBuilder {
	s.svl.get = f
	return s
}

func (s *SvlBuilder) ImplementLog(f func(v ...any)) *SvlBuilder {
	s.svl.logger = f
	return s
}

func (s *SvlBuilder) UseLog() *SvlBuilder {
	s.svl.logger = log.Println
	return s
}

func (s *SvlBuilder) UseGobEncDec() *SvlBuilder {
	s.svl.encoder = func(e any) ([]byte, error) {
		var b bytes.Buffer
		if err := gob.NewEncoder(&b).Encode(e); err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	}
	s.svl.decoder = func(e any, v []byte) error {
		return gob.NewDecoder(bytes.NewReader(v)).Decode(e)
	}
	return s
}

func (s *SvlBuilder) UseJsonEncDec() *SvlBuilder {
	s.svl.encoder = func(e any) ([]byte, error) {
		var b bytes.Buffer
		if err := json.NewEncoder(&b).Encode(e); err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	}
	s.svl.decoder = func(e any, v []byte) error {
		return json.NewDecoder(bytes.NewReader(v)).Decode(e)
	}
	return s
}

func (s *SvlBuilder) Build() (*SVL, error) {
	switch {
	case s.svl.set == nil:
		return nil, errors.New("set function not set")
	case s.svl.logger == nil:
		return nil, errors.New("logger function not set")
	}
	return s.svl, nil
}

func (s *SVL) prefix(value reflect.Value) string {
	return strings.ToLower(value.Type().Name() + "s.")
}

func (s *SVL) setSingle(prefix string, value []byte) error {
	return s.set([]BasicTransaction{{Key: []byte(prefix), Value: value}})
}

func (s *SVL) isPrimaryKey(field reflect.StructField) bool {
	if field.Tag.Get("svl") == "primary" {
		return true
	}
	return false
}

func (s *SVL) SetIncrement(value any, increment func(value any) any) error {
	// Check increment function types
	//if reflect.TypeOf(increment(value)).Kind() != reflect.TypeOf(value).Kind() {
	//	return errors.New("increment function does not return the same type as the value")
	//}
	if increment == nil {
		increment = func(value any) any {
			return value.(uint) + 1
		}
	}
	prefix := s.prefix(reflect.ValueOf(value).Elem())
	s.logger("prefix:", prefix)
	gob.Register(reflect.ValueOf(value).Elem().Interface())
	s.increments[prefix] = increment
	return nil
}

func (s *SVL) DisableIncrement(value any) {
	s.increments[s.prefix(reflect.ValueOf(value))] = func(value any) any {
		return value
	}
}

func (s *SVL) Add(value any) error {
	element := reflect.ValueOf(value).Elem()
	var baseID any
	switch {
	case element.Kind() == reflect.Slice || element.Kind() == reflect.Array:
		s.logger("slice")
		if element.Len() == 0 {
			return errors.New("slice is empty")
		}
		if element.Index(0).Type().NumField() == 0 {
			return errors.New("structure is empty")
		}
		firstElement := element.Index(0)
		prefix := s.prefix(firstElement)
		field := element.Index(0).Type().Field(0)
		if s.isPrimaryKey(field) {
			get, err := s.get([]BasicTransaction{{Key: []byte(prefix + "id")}})
			if err != nil {
				return err
			}
			if len(get) != 0 {
				s.logger("get:", get[0])
				//val := reflect.New(element.Index(0).Type().Field(0).Type).Interface()
				val := firstElement.Field(0).Interface()
				s.logger("val:", val)
				err := s.decoder(&val, get[0].Value)
				baseID = val
				s.logger("val:", val, "type:", reflect.TypeOf(val))
				if err != nil {
					return err
				}
			} else {
				baseID = firstElement.Field(0).Interface()
				s.logger("baseID:", baseID, "type:", reflect.TypeOf(baseID))
			}
			s.logger("baseID:", baseID)
			s.logger("prefix:", prefix)
			log.Println("increment:", baseID)
		} else {
			return errors.New("primary key not found in first field")
		}

		s.logger("Prefix:", s.prefix(firstElement))
		s.logger("Type of elem:", element.Index(0).Type().Name())
		for i := 0; i < element.Len(); i++ {
			s.logger(element.Index(i).Type().Name())
			val, _ := s.encoder(&baseID)
			prefixID := prefix + string(val)
			s.logger("prefixID:", prefixID)
			var byteWrite []byte
			el := element.Index(i).Interface()
			element.Index(i).Field(0).Set(reflect.ValueOf(baseID))
			if b, err := s.encoder(el); err != nil {
				return err
			} else {
				byteWrite = b
			}
			s.logger("byteWrite:", byteWrite)
			if err := s.setSingle(prefixID, byteWrite); err != nil {
				return err
			}
			baseID = s.increments[s.prefix(firstElement)](baseID)
		}

		if val, err := s.encoder(&baseID); err != nil {
			return err
		} else {
			if err := s.setSingle(prefix+"id", val); err != nil {
				return err
			}
		}
		break
	case element.Kind() == reflect.Struct:
		s.logger("struct")
		if element.Type().NumField() == 0 {
			return errors.New("structure is empty")
		}
		firstElement := element
		prefix := s.prefix(firstElement)
		field := element.Type().Field(0)
		if s.isPrimaryKey(field) {
			get, err := s.get([]BasicTransaction{{Key: []byte(prefix + "id")}})
			if err != nil {
				return err
			}
			if len(get) != 0 {
				s.logger("get:", get[0])
				//val := reflect.New(element.Index(0).Type().Field(0).Type).Interface()
				val := firstElement.Field(0).Interface()
				s.logger("val:", val)
				err := s.decoder(&val, get[0].Value)
				baseID = val
				s.logger("val:", val, "type:", reflect.TypeOf(val))
				if err != nil {
					return err
				}
			} else {
				baseID = firstElement.Field(0).Interface()
				s.logger("baseID:", baseID, "type:", reflect.TypeOf(baseID))
			}
			s.logger("baseID:", baseID)
			s.logger("prefix:", prefix)
			log.Println("increment:", baseID)
		} else {
			return errors.New("primary key not found in first field")
		}

		s.logger("Prefix:", s.prefix(firstElement))
		s.logger("Type of elem:", element.Type().Name())

		val, _ := s.encoder(&baseID)
		prefixID := prefix + string(val)
		s.logger("prefixID:", prefixID)
		var byteWrite []byte
		element.Field(0).Set(reflect.ValueOf(baseID))
		el := element.Interface()
		if b, err := s.encoder(&el); err != nil {
			return err
		} else {
			byteWrite = b
		}
		s.logger("byteWrite:", byteWrite)
		if err := s.setSingle(prefixID, byteWrite); err != nil {
			return err
		}
		baseID = s.increments[s.prefix(firstElement)](baseID)
		if val, err := s.encoder(&baseID); err != nil {
			return err
		} else {
			if err := s.setSingle(prefix+"id", val); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *SVL) Get(value any) error {
	element := reflect.ValueOf(value).Elem()
	switch {
	case element.Kind() == reflect.Slice:
		s.logger("slice")
		s.logger("Type of elem:", reflect.TypeOf(value).Elem())
		prefix := reflect.TypeOf(value).Elem().String()
		prefixSplit := strings.Split(prefix, ".")
		prefix = strings.ToLower(prefixSplit[len(prefixSplit)-1]) + "s."
		s.logger("Prefix:", prefix)
		iterator := func(key string) int {
			if strings.HasPrefix(key, prefix) && !strings.HasSuffix(key, "id") {
				return 0
			}
			return -1
		}
		els, err := s.iterate(iterator)
		if err != nil {
			return err
		}
		if len(els) == 0 {
			return errors.New("no elements found")
		}
		nSlice := reflect.MakeSlice(element.Type(), len(els), len(els))
		for i := 0; i < len(els); i++ {
			el := nSlice.Index(i).Interface()
			if err := s.decoder(&el, els[i].Value); err != nil {
				return err
			}
			nSlice.Index(i).Set(reflect.ValueOf(el))
		}
		element.Set(nSlice)
		return nil
	case element.Kind() == reflect.Struct:
		s.logger("struct")
		if element.Type().NumField() == 0 {
			return errors.New("structure is empty")
		}
		prefix := s.prefix(element)
		s.logger("Prefix:", prefix)
		baseID := element.Field(0).Interface()
		s.logger("baseID:", baseID)
		val, _ := s.encoder(&baseID)
		prefixID := prefix + string(val)
		s.logger("prefixID:", prefixID)
		get, err := s.get([]BasicTransaction{{Key: []byte(prefixID)}})
		if err != nil {
			return err
		}
		if len(get) != 0 {
			el := reflect.ValueOf(value).Interface()
			if err := s.decoder(&el, get[0].Value); err != nil {
				return err
			}
			element.Set(reflect.ValueOf(el))
		}
		return nil
	case element.Kind() == reflect.Array:
		s.logger("array")
		return errors.New("not implemented")
	}
	return nil
}
