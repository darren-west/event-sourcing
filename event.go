package event

import (
	"errors"

	"fmt"

	"reflect"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	ErrNotFound             = errors.New("Event not found")
	ErrInterfaceNotSlicePtr = errors.New("Argument is not a ptr to a slice")
)

type rawEvent struct {
	Id         bson.ObjectId `json:"id"        bson:"_id,omitempty"`
	InstanceId bson.ObjectId
	Type       EventType
	Raw        *bson.Raw
}

type Event interface {
	GetId() bson.ObjectId
	GetInstanceId() bson.ObjectId
	GetType() EventType
}

func toBSONRaw(in interface{}) (raw *bson.Raw, err error) {
	var b []byte
	if b, err = bson.Marshal(in); err != nil {
		return
	}
	raw = new(bson.Raw)
	if err = bson.Unmarshal(b, raw); err != nil {
		return
	}
	return
}

func buildEvent(e Event) (re rawEvent, err error) {
	if re.Raw, err = toBSONRaw(e); err != nil {
		return
	}
	re.Id = e.GetId()
	re.Type = e.GetType()
	re.InstanceId = e.GetInstanceId()
	return
}

func fromEvent(out interface{}, raw rawEvent) (err error) {
	err = raw.Raw.Unmarshal(out)
	return
}

func validateEvent(ev Event) (err error) {
	if !ev.GetId().Valid() {
		err = fmt.Errorf("Event not valid Id = %s", ev.GetId())
		return
	}
	if !ev.GetInstanceId().Valid() {
		err = fmt.Errorf("Event not valid InstanceId = %s", ev.GetInstanceId())
		return
	}
	return
}

type EventType int8

type Options struct {
	Address        string
	DatabaseName   string
	CollectionName string
}

type Option func(opts *Options)

func Address(address string) Option {
	return func(opts *Options) {
		opts.Address = address
	}
}

func DatabaseName(name string) Option {
	return func(opts *Options) {
		opts.DatabaseName = name
	}
}

func CollectionName(name string) Option {
	return func(opts *Options) {
		opts.CollectionName = name
	}
}

func newOptions(opts ...Option) *Options {
	o := &Options{
		DatabaseName:   "event-sourcing",
		CollectionName: "event",
		Address:        "localhost:27017",
	}

	for _, opt := range opts {
		opt(o)
	}
	return o
}

type EventStore interface {
	Init(...Option) error
	Create(Event) error
	Update(Event) error
	Get(bson.ObjectId, interface{}) error
	List(out interface{}) error
	ListFiltered(interface{}, bson.M) error
}

type MongoEventStore struct {
	*mgo.Session
	options *Options
}

func (store *MongoEventStore) Init(options ...Option) (err error) {
	store.options = newOptions(options...)

	if store.Session, err = mgo.Dial(store.options.Address); err != nil {
		return
	}
	col := store.Session.DB(store.options.DatabaseName).C(store.options.CollectionName)

	index := mgo.Index{
		Key: []string{"instanceid"},
	}
	err = col.EnsureIndex(index)
	return
}

func (store *MongoEventStore) Create(event Event) (err error) {
	if err = validateEvent(event); err != nil {
		return
	}
	session := store.Session.Copy()
	defer session.Close()
	e, err := buildEvent(event)
	if err != nil {
		return
	}
	err = session.DB(store.options.DatabaseName).C(store.options.CollectionName).Insert(&e)
	return
}

func (store *MongoEventStore) Update(event Event) (err error) {
	if err = validateEvent(event); err != nil {
		return
	}
	session := store.Session.Copy()
	defer session.Close()

	e, err := buildEvent(event)

	if err != nil {
		return
	}
	err = session.DB(store.options.DatabaseName).C(store.options.CollectionName).Update(bson.M{"_id": e.Id}, e)
	return
}

func (store *MongoEventStore) Get(id bson.ObjectId, out interface{}) (err error) {
	session := store.Session.Copy()
	defer session.Close()
	e := rawEvent{}
	if err = session.DB(store.options.DatabaseName).C(store.options.CollectionName).Find(bson.M{"_id": id}).One(&e); err != nil {
		if err == mgo.ErrNotFound {
			err = ErrNotFound
		}
		return
	}
	err = fromEvent(out, e)
	return
}

func (store *MongoEventStore) List(out interface{}) (err error) {
	return store.list(out, nil)
}

func (store *MongoEventStore) list(out interface{}, f bson.M) (err error) {
	ot := reflect.TypeOf(out)
	ov := reflect.ValueOf(out)
	if out == nil || ot.Kind() != reflect.Ptr || ot.Elem().Kind() != reflect.Slice {
		err = ErrInterfaceNotSlicePtr
		return
	}

	session := store.Session.Copy()

	var rawEvents []rawEvent

	if err = session.DB(store.options.DatabaseName).C(store.options.CollectionName).Find(f).All(&rawEvents); err != nil {
		return
	}

	st := ot.Elem().Elem() //slice type

	for _, re := range rawEvents {
		event := reflect.New(st).Interface()
		fromEvent(event, re)
		ov.Elem().Set(reflect.Append(ov.Elem(), reflect.ValueOf(event).Elem()))
	}

	return
}

func (store *MongoEventStore) ListFiltered(out interface{}, f bson.M) error {
	return store.list(out, f)
}
