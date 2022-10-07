package googleworkspace

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin/transform"
)

//// TABLE DEFINITION

func tableGoogleWorkspaceAdminReportsEntityUsage(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "googleworkspace_admin_reports_entity_usage",
		Description: "Retrieves usage reports including statistics",
		List: &plugin.ListConfig{
			Hydrate: listAdminReportsEntityUsage,
			KeyColumns: []*plugin.KeyColumn{
				{
					Name:    "date",
					Require: plugin.Required,
				},
				{
					Name:    "entity_type",
					Require: plugin.Required,
				},
				{
					Name:    "entity_key",
					Require: plugin.Optional,
				},
				{
					Name:    "customer_id",
					Require: plugin.Optional,
				},
				{
					Name:    "filters",
					Require: plugin.Optional,
				},
				{
					Name:    "parameters",
					Require: plugin.Optional,
				},
			},
		},
		Columns: []*plugin.Column{
			{
				Name:        "date",
				Description: "Represents the date the usage occurred. The timestamp is in the ISO 8601 format, yyyy-mm-dd",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "customer_id",
				Description: "The unique ID of the customer to retrieve data for",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Entity.CustomerId"),
			},
			{
				Name:        "parameters",
				Description: "Comma-separated list of event parameters that refine a report's results",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "user_email",
				Description: "The user's email address",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Entity.UserEmail"),
			},
			{
				Name:        "profile_id",
				Description: "The user's immutable Google Workspace profile identifier",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Entity.ProfileId"),
			},
			{
				Name:        "entity_id",
				Description: "Object key",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Entity.EntityId"),
			},
			{
				Name:        "type",
				Description: "The type of item",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Entity.Type"),
			},
			{
				Name:        "filters",
				Description: "comma-separated list of an application's event parameters where the parameter's value is manipulated by a relational operator",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromQual("filters"),
			},
			{
				Name:        "entity_key",
				Description: "Represents the key of the object to filter the data with",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromQual("entity_key"),
			},
			{
				Name:        "entity_type",
				Description: "Represents the type of entity for the report",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromQual("entity_type"),
			},
		},
	}
}

//// LIST FUNCTION

func listAdminReportsEntityUsage(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	// Create service
	service, err := AdminReportsService(ctx, d)
	if err != nil {
		return nil, err
	}

	var date string
	if d.KeyColumnQuals["date"] != nil {
		date = d.KeyColumnQuals["date"].GetStringValue()
	} else {
		return nil, nil
	}

	var entity_type string
	if d.KeyColumnQuals["entity_type"] != nil {
		entity_type = d.KeyColumnQuals["entity_type"].GetStringValue()
	} else {
		return nil, nil
	}

	var entity_key string
	if d.KeyColumnQuals["entity_key"] != nil {
		entity_key = d.KeyColumnQuals["entity_key"].GetStringValue()
	} else {
		entity_key = "all"
	}

	// Setting the maximum number of messages, API can return in a single page
	maxResults := int64(1000)

	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil {
		if *limit < maxResults {
			maxResults = *limit
		}
	}
	resp := service.EntityUsageReports.Get(entity_type, entity_key, date).MaxResults(maxResults)

	var customer_id string
	if d.KeyColumnQuals["customer_id"] != nil {
		customer_id = d.KeyColumnQuals["customer_id"].GetStringValue()
		resp = resp.CustomerId(customer_id)
	}

	var parameters string
	if d.KeyColumnQuals["parameters"] != nil {
		parameters = d.KeyColumnQuals["parameters"].GetStringValue()
		resp = resp.Parameters(parameters)
	}

	var filters string
	if d.KeyColumnQuals["filters"] != nil {
		filters = d.KeyColumnQuals["filters"].GetStringValue()
		resp = resp.Filters(filters)
	}

	if err := resp.Pages(ctx, func(page *UsageReports) error {
		for _, item := range page.UsageReports {
			d.StreamListItem(ctx, item)

			// Context can be cancelled due to manual cancellation or the limit has been hit
			if plugin.IsCancelled(ctx) {
				page.NextPageToken = ""
				break
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return nil, nil
}
