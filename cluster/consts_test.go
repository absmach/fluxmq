// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

const (
	testTopicSensorTemp      = "sensor/temp"
	testTopicSensorRoom1Temp = "sensor/room1/temp"
	testTopicSensorRoom1High = "sensor/room1/temp/high"
	testTopicSensorEmptyTemp = "sensor//temp"
	testFilterSensorPlus     = "sensor/+"
	testFilterSensorPlusTemp = "sensor/+/temp"
	testFilterSensorHash     = "sensor/#"
	testFilterSensorPlusHash = "sensor/+/#"
	testTopicSensor          = "sensor"

	testNodeLocal = "node-local"
	testNodeA     = "node-a"
	testNodeB     = "node-b"

	testTopicAB  = "a/b"
	testTopicCD  = "c/d"
	testTopicBC  = "b/c"
	testTopicBad = "bad/topic"

	testValue = "value"

	testOrders      = "orders"
	testAlwaysFails = "always fails"
	testPersistent  = "persistent"
	testPartial     = "partial"
	testFail        = "fail"
)
