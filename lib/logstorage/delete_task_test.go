package logstorage

import (
	"testing"
	"time"
)

func TestDeleteTaskMarshalUnmarshalAsJSON(t *testing.T) {
	dts := []*DeleteTask{
		{
			TaskID: "task1",
			TenantIDs: []TenantID{
				{
					AccountID: 0,
					ProjectID: 0,
				},
				{
					AccountID: 12,
					ProjectID: 456,
				},
			},
			Filter:    "app:=foo",
			StartTime: time.Now(),
		},
		{
			TaskID: "task_2",
			TenantIDs: []TenantID{
				{
					AccountID: 0,
					ProjectID: 0,
				},
			},
			Filter:    "app:=x",
			StartTime: time.Now(),
		},
	}

	data := MarshalDeleteTasksToJSON(dts)

	dtsUnmarshaled, err := UnmarshalDeleteTasksFromJSON(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	data2 := MarshalDeleteTasksToJSON(dtsUnmarshaled)
	if string(data) != string(data2) {
		t.Fatalf("unexpected delete_task unmarshaled\ngot %s\nwant %s", data2, data)
	}
}
