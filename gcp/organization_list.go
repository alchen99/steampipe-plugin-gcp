package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"google.golang.org/api/cloudresourcemanager/v1"
)

const matrixKeyOrg = "organization"

func BuildOrganizationList(ctx context.Context, d *plugin.QueryData) []map[string]interface{} {

	// have we already created and cached the Organizations?
	orgCacheKey := "Organizations"
	if cachedData, ok := d.ConnectionManager.Cache.Get(orgCacheKey); ok {
		plugin.Logger(ctx).Trace("listOrgDetails:", cachedData.([]map[string]interface{}))
		return cachedData.([]map[string]interface{})
	}

	// Create Service Connection
	service, err := CloudResourceManagerService(ctx, d)
	if err != nil {
		return nil
	}

	// Max limit isn't mentioned in the documentation
	// Default limit is set as 1000
	rb := &cloudresourcemanager.SearchOrganizationsRequest{
		PageSize: 1000,
	}

	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil {
		if *limit < rb.PageSize {
			rb.PageSize = *limit
		}
	}

	resp, err := service.Organizations.Search(rb).Do()
	if err != nil {
		return nil
	}

	// validate org list
	matrix := make([]map[string]interface{}, len(resp.Organizations))
	for i, org := range resp.Organizations {
		matrix[i] = map[string]interface{}{matrixKeyOrg: getLastPathElement(org.Name)}
	}
	d.ConnectionManager.Cache.Set(orgCacheKey, matrix)
	return matrix
}
