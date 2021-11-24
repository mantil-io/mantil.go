package mantil

import "fmt"

type ResourceInfo struct {
	// Full name of the AWS resource
	Name string
	// Tags that are automatically added to the resource on creation
	Tags map[string]string
}

// Resource takes a user-defined resource name and returns a ResourceInfo struct
// containing the actual name and tags that are used to create the resource.
func Resource(name string) ResourceInfo {
	return ResourceInfo{
		Name: fmt.Sprintf(config().NamingTemplate, name),
		Tags: config().ResourceTags,
	}
}
