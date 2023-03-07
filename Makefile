
install:
	go build -o ~/.steampipe/plugins/hub.steampipe.io/plugins/turbot/googleworkspace@latest/steampipe-plugin-googleworkspace.plugin *.go

local:
	go build -o  ~/.steampipe/plugins/local/googleworkspace-zb/googleworkspace-zb.plugin *.go
