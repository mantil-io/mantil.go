package mantil

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const usersPartition = "user"

// test structure
type User struct {
	Key       string
	Email     string
	FirstName string
	LastName  string
	//LockVersion int
}

type Todo struct {
	ID          string
	Description string
	CreatedAt   time.Time
	CompletedAt time.Time
}

func TestKVTodos(t *testing.T) {
	partition := "TODOS"
	kv, err := NewKV(partition)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		d := Todo{
			ID:          fmt.Sprintf("%d", i),
			Description: fmt.Sprintf("todo item no %d", i),
			CreatedAt:   time.Now(),
		}
		err := kv.Put(d.ID, d)
		require.NoError(t, err)
	}
	var todos []Todo
	err = kv.GetAll(&todos)
	require.NoError(t, err)
	require.Len(t, todos, 10)
	for i, d := range todos {
		require.Equal(t, fmt.Sprintf("%d", i), d.ID)
		require.Equal(t, fmt.Sprintf("todo item no %d", i), d.Description)
	}

	todos = make([]Todo, 0)
	//err = kv.GetMany(&todos, GetKeyOptions{BeginsWith: "7"})
	err = kv.Find(&todos, FindBeginsWith, "7")
	require.NoError(t, err)
	require.Len(t, todos, 1)

	todos = make([]Todo, 0)
	var opt GetKeyOptions
	opt.Between.Start = "2"
	opt.Between.End = "6"
	err = kv.GetMany(&todos, opt)
	require.NoError(t, err)
	require.Len(t, todos, 5)

	todos = make([]Todo, 0)
	//err = kv.GetMany(&todos, GetKeyOptions{GreaterThan: "7"})
	err = kv.Find(&todos, FindGreaterThan, "7")
	require.NoError(t, err)
	require.Len(t, todos, 2)

	todos = make([]Todo, 0)
	//err = kv.GetMany(&todos, GetKeyOptions{GreaterThanOrEqual: "7"})
	err = kv.Find(&todos, FindGreaterThanOrEqual, "7")
	require.NoError(t, err)
	require.Len(t, todos, 3)

	todos = make([]Todo, 0)
	//err = kv.GetMany(&todos, GetKeyOptions{LessThan: "4"})
	err = kv.Find(&todos, FindLessThan, "4")
	require.NoError(t, err)
	require.Len(t, todos, 4)

	todos = make([]Todo, 0)
	//err = kv.GetMany(&todos, GetKeyOptions{LessThanOrEqual: "4"})
	err = kv.Find(&todos, FindLessThanOrEqual, "4")
	require.NoError(t, err)
	require.Len(t, todos, 5)

	err = kv.DeleteAll()
	require.NoError(t, err)

	todos = make([]Todo, 0)
	err = kv.GetAll(&todos)
	require.NoError(t, err)
	require.Len(t, todos, 0)
}

func TestKV(t *testing.T) {
	partition := "USERS"
	kv, err := NewKV(partition)
	require.NoError(t, err)

	u1 := User{
		Key:       "ivan",
		Email:     "ivan@mantil.com",
		FirstName: "Ivan",
		LastName:  "Vlašić",
	}
	err = kv.Put(u1.Key, u1)
	require.NoError(t, err)

	u2 := User{
		Key:       "daniel",
		Email:     "daniel@mantil.com",
		FirstName: "Daniel",
		LastName:  "Jelušić",
	}
	err = kv.Put(u2.Key, u2)
	require.NoError(t, err)

	// var u1r User
	// err = kv.Get(u1.Key, &u1r)
	// require.NoError(t, err)
	// require.Equal(t, u1, u1r)

	var users []User
	err = kv.GetAll(&users)
	require.NoError(t, err)
	require.Len(t, users, 2)
	//fmt.Printf("users:\n%#v\n", users)

	err = kv.deleteMany(u1.Key, u2.Key)
	require.NoError(t, err)

	users = make([]User, 0)
	err = kv.GetAll(&users)
	require.NoError(t, err)
	require.Len(t, users, 0)

	// // connect
	// kv := kv{tableName: "mantil-project-try-first-try"}
	// err := kv.connect()
	// require.NoError(t, err)

	// u := User{
	// 	Email:     "ianic@mantil.com",
	// 	FirstName: "Igor",
	// 	LastName:  "Anic",
	// }
	// // put example, version 0 is insert without version checking
	// err = kv.Put(usersPartition, u.Email, u, 0)
	// require.NoError(t, err)
	// // after this version is 1

	// // get example
	// var u2 User
	// ver, err := kv.Get(usersPartition, u.Email, &u2)
	// require.NoError(t, err)
	// require.Equal(t, ver, 1)
	// require.Equal(t, u, u2)
	// require.Equal(t, u.Email, u2.Email)
	// require.Equal(t, u.FirstName, u2.FirstName)
	// require.Equal(t, u.LastName, u2.LastName)

	// u.FirstName = u.FirstName + " 1"
	// err = kv.Put(usersPartition, u.Email, u, ver)
	// require.NoError(t, err)
	// // after this put version in store is 2

	// // example of stale item put
	// // after preious put version is 2 in store
	// // cant' update with the ver 1 any more
	// u.FirstName = u.FirstName + " 2"
	// err = kv.Put(usersPartition, u.Email, u, ver)
	// require.Error(t, err)

	// // chack that the error is of required type
	// var staleErr *ErrStaleItemVersion
	// require.ErrorAs(t, err, &staleErr)
	// require.True(t, errors.As(err, &staleErr))
	// require.Equal(t, 1, staleErr.Version)
}
