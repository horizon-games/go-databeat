package databeat

import "time"

func TimeNow() *time.Time {
	t := time.Now().UTC()
	return &t
}

func String(s string) *string {
	return &s
}

func ServerDevice() *Device {
	return &Device{Platform: "server"}
}

func updateEventDevice(events []*Event, device *Device) {
	for _, ev := range events {
		if ev.Device == nil {
			ev.Device = device
		}
	}
}
