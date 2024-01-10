package gcp

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/cloudresourcemanager/v1"
)

//// LIST FUNCTION

func listGCPOrganizationProjectFolder(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("listGCPOrganizationProjectFolder")

	// Create Service Connection
	service, err := CloudResourceManagerService(ctx, d)
	if err != nil {
		return nil, err
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

	resp := service.Organizations.Search(rb)
	if err := resp.Pages(ctx, func(page *cloudresourcemanager.SearchOrganizationsResponse) error {
		for _, organization := range page.Organizations {
			d.StreamListItem(ctx, organization)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return nil, nil
}
