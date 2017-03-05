package event

import (
	"log"
	"testing"

	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const mongoAddress = "192.168.3.165:27017"

type testEvent struct {
	Id         bson.ObjectId
	InstanceId bson.ObjectId
	String     string
}

func (e *testEvent) GetId() bson.ObjectId {
	return e.Id
}
func (e *testEvent) GetInstanceId() bson.ObjectId {
	return e.InstanceId
}

func (e *testEvent) GetType() EventType {
	return EventType(101)
}

func teardownTestDatabase(dbName string, collectionName string) (err error) {
	session, err := mgo.Dial(mongoAddress)
	if err != nil {
		log.Printf("%s\n", err.Error())
		return
	}
	col := session.DB(dbName).C(collectionName)
	if err = col.DropCollection(); err != nil {
		return
	}
	return
}

func newTestEventStore() (EventStore, error) {
	teardownTestDatabase("event-sourcing-test", "tests")
	store := &MongoEventStore{}
	err := store.Init(Address(mongoAddress), DatabaseName("event-sourcing-test"), CollectionName("tests"))

	return store, err
}

func TestCreate(t *testing.T) {
	store, err := newTestEventStore()
	if err != nil {
		t.Fatal(err)
	}
	event := testEvent{bson.NewObjectId(), bson.NewObjectId(), "This is a test of some data"}
	if err := store.Create(&event); err != nil {
		t.Fatal(err)
	}

	event1 := testEvent{}
	store.Get(event.GetId(), &event1)

	if event1 != event {
		t.Errorf("Actual %v, Expected %v", event1, event)
	}
	t.Log(event, event1)
}

func TestCreateInvalid(t *testing.T) {
	store, err := newTestEventStore()
	if err != nil {
		t.Fatal(err)
	}

	event := testEvent{}
	if err = store.Create(&event); err == nil {
		t.Fatal("Expected error")
	}
	t.Log(err)
}

func TestGetInvalid(t *testing.T) {
	store, err := newTestEventStore()
	if err != nil {
		t.Fatal(err)
	}

	event := testEvent{}
	if err = store.Get(bson.NewObjectId(), &event); err == nil {
		t.Fatal("Expected error")
	}
	if err != ErrNotFound {
		t.Error("Expected to have not found error")
	}
	t.Log(err)
}

func TestList(t *testing.T) {
	store, err := newTestEventStore()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		e := &testEvent{Id: bson.NewObjectId(), InstanceId: bson.NewObjectId(), String: fmt.Sprintf("This test %d", i)}
		if err = store.Create(e); err != nil {
			t.Fatal(err)
		}
	}

	var list []testEvent

	if err = store.List(&list); err != nil {
		t.Error(err)
	}

	if len(list) != 100 {
		t.Error("Expected 100 test events")
	}

	for i, te := range list {
		if te.String != fmt.Sprintf("This test %d", i) {
			t.Errorf("Expected event to contain text %s %d", "This test", i)
		}
	}

	t.Log(err)

}

func TestListInvalidArg(t *testing.T) {
	store, err := newTestEventStore()
	if err != nil {
		t.Fatal(err)
	}
	if err = store.List(nil); err != ErrInterfaceNotSlicePtr {
		t.Errorf("Expected error = %s, Actual = %s", ErrInterfaceNotSlicePtr, err)
	}
	sl := make([]testEvent, 0)

	if err = store.List(sl); err != ErrInterfaceNotSlicePtr {
		t.Errorf("Expected error = %s, Actual = %s", ErrInterfaceNotSlicePtr, err)
	}

	if err = store.List(&sl); err != nil {
		t.Errorf("Expected error = %s, Actual = %s", nil, err)
	}
}

func TestListFiltered(t *testing.T) {
	store, err := newTestEventStore()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		e := &testEvent{Id: bson.NewObjectId(), InstanceId: bson.NewObjectId(), String: fmt.Sprintf("This test %d", i)}
		if err = store.Create(e); err != nil {
			t.Fatal(err)
		}
	}

	var list []testEvent

	if err = store.ListFiltered(&list, bson.M{"raw.string": "This test 1"}); err != nil {
		t.Error(err)
	}

	if len(list) != 1 {
		t.Error("Expected 1 test events")
		return
	}

	if list[0].String != "This test 1" {
		t.Errorf("Expect 'This test 1', actual %s", list[0].String)
	}

	t.Log(err)
}

func TestUpdate(t *testing.T) {
	store, err := newTestEventStore()
	if err != nil {
		t.Fatal(err)
	}
	event := testEvent{bson.NewObjectId(), bson.NewObjectId(), "This is a test of some data"}
	if err := store.Create(&event); err != nil {
		t.Fatal(err)
	}

	event1 := testEvent{}
	store.Get(event.GetId(), &event1)

	event1.String = "UPDATED"

	if err = store.Update(&event1); err != nil {
		t.Fatal(err)
	}

	store.Get(event.GetId(), &event)

	if event1 != event {
		t.Errorf("Actual %v, Expected %v", event1, event)
	}
	t.Log(event, event1)
}

func BenchmarkCreate(b *testing.B) {
	store, err := newTestEventStore()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer() //dont include init and setup
	for i := 0; i < b.N; i++ {
		event := testEvent{bson.NewObjectId(), bson.NewObjectId(), "This is a test of some data"}
		if err := store.Create(&event); err != nil {
			b.Fatal(err)
		}
	}

}
