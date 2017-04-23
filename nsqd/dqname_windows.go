// +build windows

package nsqd

// On Windows, file names cannot contain colons.
// 在windows平台上面,根据topicName和channelName组成BackendName(<topic>:<channel>)
func getBackendName(topicName, channelName string) string {
	// backend names, for uniqueness, automatically include the topic... <topic>;<channel>
	backendName := topicName + ";" + channelName
	return backendName
}
