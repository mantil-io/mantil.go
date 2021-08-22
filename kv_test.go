package mantil

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	usersPartition = "USERS"
	todosPartition = "TODOS"
)

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

func TestKVFindOperations(t *testing.T) {
	kv, err := NewKV(todosPartition)
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
	err = kv.FindAll(&todos)
	require.NoError(t, err)
	require.Len(t, todos, 10)
	for i, d := range todos {
		require.Equal(t, fmt.Sprintf("%d", i), d.ID)
		require.Equal(t, fmt.Sprintf("todo item no %d", i), d.Description)
	}

	cases := []struct {
		op          FindOperator
		args        []string
		requiredLen int
	}{
		{FindBeginsWith, []string{"7"}, 1},
		{FindBetween, []string{"2", "6"}, 5},
		{FindGreaterThan, []string{"7"}, 2},
		{FindGreaterThanOrEqual, []string{"7"}, 3},
		{FindLessThan, []string{"4"}, 4},
		{FindLessThanOrEqual, []string{"4"}, 5},
	}
	for _, c := range cases {
		todos = make([]Todo, 0)
		err = kv.Find(&todos, c.op, c.args...)
		require.NoError(t, err)
		require.Len(t, todos, c.requiredLen)
	}

	err = kv.DeleteAll()
	require.NoError(t, err)

	todos = make([]Todo, 0)
	err = kv.FindAll(&todos)
	require.NoError(t, err)
	require.Len(t, todos, 0)
}

func TestKVPutGet(t *testing.T) {
	kv, err := NewKV(usersPartition)
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

	var u2r User
	err = kv.Get(u2.Key, &u2r)
	require.NoError(t, err)
	require.Equal(t, u2, u2r)

	var users []User
	err = kv.FindAll(&users)
	require.NoError(t, err)
	require.Len(t, users, 2)
	require.Equal(t, u2, users[0])
	require.Equal(t, u1, users[1])

	err = kv.Delete(u1.Key, u2.Key)
	require.NoError(t, err)

	users = make([]User, 0)
	err = kv.FindAll(&users)
	require.NoError(t, err)
	require.Len(t, users, 0)
}
