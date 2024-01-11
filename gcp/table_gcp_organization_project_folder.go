package gcp

import (
	"context"
	"strconv"
	"strings"

	"cloud.google.com/go/asset/apiv1/assetpb"
	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/iterator"
)

//// TABLE DEFINITION

func tableGcpOrganizationProjectFolder(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_organization_project_folder",
		Description: "GCP Organization, Projects, and Folders",
		List: &plugin.ListConfig{
			Hydrate: listGCPOrganizationProjectFolder,
		},
		GetMatrixItemFunc: BuildOrganizationList,
		Columns: []*plugin.Column{
			{
				Name:        "resource_id",
				Description: "The id of an organization, project, or folder.",
				Type:        proto.ColumnType_INT,
				Transform:   transform.FromP(gcpOrgProjectFolderData, "Resource_Id"),
			},
			{
				Name:        "resource_type",
				Description: "Type of resource. One of organization, project, or folder.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(gcpOrgProjectFolderData, "Resource_Type"),
			},
			{
				Name:        "name",
				Description: "The resource name.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(gcpOrgProjectFolderData, "Name"),
			},
			{
				Name:        "display_name",
				Description: "Human-readable display name of the resource.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "lifecycle_state",
				Description: "The resource's current lifecycle state.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("State"),
			},
			{
				Name:        "create_time",
				Description: "Timestamp when the resource was created.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("CreateTime").Transform(convertTimestamppbAsTime),
			},
			{
				Name:        "update_time",
				Description: "Timestamp when the resource was last updated.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("UpdateTime").Transform(convertTimestamppbAsTime).NullIfZero(),
			},
			{
				Name:        "organization",
				Description: "Oragnization resource belongs to.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "parent",
				Description: "Parent resource name.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(gcpOrgProjectFolderData, "Parent").NullIfZero(),
			},
			{
				Name:        "parent_asset_type",
				Description: "Parent asset type. One of organization, project, or folder.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(gcpOrgProjectFolderData, "Parent_Asset_Type").NullIfZero(),
			},

			// Steampipe standard column
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("DisplayName"),
			},

			{
				Name:        "labels",
				Description: "A set of labels associated with this resource.",
				Type:        proto.ColumnType_JSON,
			},

			// Steampipe standard columns
			{
				Name:        "tags",
				Description: ColumnDescriptionTags,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Tags"),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(gcpOrgProjectFolderData, "Akas"),
			},

			// Standard gcp columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(gcpOrgProjectFolderData, "Location"),
			},
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(gcpOrgProjectFolderData, "Project").NullIfZero(),
			},
		},
	}
}

//// LIST FUNCTION

func listGCPOrganizationProjectFolder(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("listGCPOrganizationProjectFolder")

	var org string
	matrixOrg := d.EqualsQualString(matrixKeyOrg)
	// Since, when the service API is disabled, matrixLocation value will be nil
	if matrixOrg != "" {
		org = matrixOrg
	}

	plugin.Logger(ctx).Trace("listGCPOrganizationProjectFolder", "org", org)

	// Page size should be in range of [0, 500]
	pageSize := types.Int64(500)
	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil {
		if *limit < *pageSize {
			pageSize = limit
		}
	}

	// Create Service Connection
	service, err := CloudAssetInventoryService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("listGCPOrganizationProjectFolder", "service_error", err)
		return nil, err
	}

	req := &assetpb.SearchAllResourcesRequest{
		Scope:      "organizations/" + org,
		AssetTypes: []string{"cloudresourcemanager.googleapis.com/Organization", "cloudresourcemanager.googleapis.com/Project", "cloudresourcemanager.googleapis.com/Folder"},
		OrderBy:    "assetType",
		PageSize:   int32(*pageSize),
	}

	it := service.SearchAllResources(ctx, req)

	for {
		resp, err := it.Next()
		if err != nil {
			if strings.Contains(err.Error(), "404") {
				return nil, nil
			}
			if err == iterator.Done {
				break
			}
			plugin.Logger(ctx).Error("listGCPOrganizationProjectFolder", "api_error", err)
			return nil, err
		}

		plugin.Logger(ctx).Trace("listGCPOrganizationProjectFolder", "response", resp)

		d.StreamListItem(ctx, resp)

		// Check if context has been cancelled or if the limit has been hit (if specified)
		// if there is a limit, it will return the number of rows required to reach this limit
		if d.RowsRemaining(ctx) == 0 {
			return nil, nil
		}
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func gcpOrgProjectFolderData(ctx context.Context, h *transform.TransformData) (interface{}, error) {
	plugin.Logger(ctx).Trace("gcpOrgProjectFolderData")

	param := h.Param.(string)
	data := h.HydrateItem.(*assetpb.ResourceSearchResult)
	akas := []string{"gcp:" + data.Name}

	var parent string
	var parentAssetType string
	var project string
	var resourceName string
	var resourceIdStr string
	var resourceType string
	if getLastPathElement(data.AssetType) == "Organization" {
		resourceIdStr = getLastPathElement(data.Organization)
		resourceName = data.Organization
		resourceType = "Organization"
	} else if getLastPathElement(data.AssetType) == "Folder" {
		resourceIdStr = getLastPathElement(data.Name)
		resourceName = "folders/" + resourceIdStr
		resourceType = "Folder"
		parentAssetType = getLastPathElement(data.ParentAssetType)
		parentParts := strings.Split(data.ParentFullResourceName, "/")
		parentLastTwoParts := parentParts[len(parentParts)-2:]
		parent = parentLastTwoParts[0] + "/" + parentLastTwoParts[1]
	} else if getLastPathElement(data.AssetType) == "Project" {
		resourceIdStr = getLastPathElement(data.Project)
		resourceName = data.Project
		resourceType = "Project"
		project = getLastPathElement(data.Name)
		parentAssetType = getLastPathElement(data.ParentAssetType)
		parentParts := strings.Split(data.ParentFullResourceName, "/")
		parentLastTwoParts := parentParts[len(parentParts)-2:]
		parent = parentLastTwoParts[0] + "/" + parentLastTwoParts[1]
	}

	resourceId, err := strconv.Atoi(resourceIdStr)
	if err != nil {
		plugin.Logger(ctx).Error("gcpOrgProjectFolderData - Could not convert resource id to integer!")
		return nil, err
	}

	plugin.Logger(ctx).Trace("gcpOrgProjectFolderData", "Resource_Id", resourceId)
	plugin.Logger(ctx).Trace("gcpOrgProjectFolderData", "Resource_Type", resourceType)
	plugin.Logger(ctx).Trace("gcpOrgProjectFolderData", "Name", resourceName)
	plugin.Logger(ctx).Trace("gcpOrgProjectFolderData", "Parent", parent)
	plugin.Logger(ctx).Trace("gcpOrgProjectFolderData", "Parent_Asset_Type", parentAssetType)
	plugin.Logger(ctx).Trace("gcpOrgProjectFolderData", "Akas", akas)
	plugin.Logger(ctx).Trace("gcpOrgProjectFolderData", "Location", data.Location)
	plugin.Logger(ctx).Trace("gcpOrgProjectFolderData", "Project", project)

	turbotData := map[string]interface{}{
		"Resource_Id":       resourceId,
		"Resource_Type":     resourceType,
		"Name":              resourceName,
		"Parent":            parent,
		"Parent_Asset_Type": parentAssetType,
		"Akas":              akas,
		"Location":          data.Location,
		"Project":           project,
	}

	return turbotData[param], nil
}
