package googleworkspace

import (
	"context"
	"fmt"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin/transform"

	admin "google.golang.org/api/admin/reports/v1"
)

//// TABLE DEFINITION

func tableGoogleWorkspaceAdminReportsActivities(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "googleworkspace_admin_reports_activities",
		Description: "Retrieves activity reports for one application",
		List: &plugin.ListConfig{
			Hydrate: listAdminReportsActivities,
			KeyColumns: []*plugin.KeyColumn{
				{
					Name:    "application_name",
					Require: plugin.Required,
				},
				{
					Name:    "user_key",
					Require: plugin.Optional,
				},
				{
					Name:    "actor_ip_address",
					Require: plugin.Optional,
				},
				{
					Name:    "customer_id",
					Require: plugin.Optional,
				},
				{
					Name:      "time",
					Require:   plugin.Optional,
					Operators: []string{">", ">=", "=", "<", "<="},
				},
				{
					Name:    "event_name",
					Require: plugin.Optional,
				},
				{
					Name:    "filters",
					Require: plugin.Optional,
				},
				{
					Name:    "org_unit_id",
					Require: plugin.Optional,
				},
				{
					Name:    "group_id_filter",
					Require: plugin.Optional,
				},
			},
		},
		Columns: []*plugin.Column{
			{
				Name:        "application_name",
				Description: "The application name for query",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Id.ApplicationName"),
			},
			{
				Name:        "user_key",
				Description: "The user id or email to retrieve",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Actor.Key"),
			},
			{
				Name:        "actor_ip_address",
				Description: "An actor's ip adress to retrieve",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("IpAddress"),
			},
			{
				Name:        "customer_id",
				Description: "The customer id for each activity record",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Id.CustomerId"),
			},
			{
				Name:        "owner_domain",
				Description: "The immutable ID of the message.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "ip_address",
				Description: "The IP of the actor",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "events",
				Description: "Activity events in the report",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "time",
				Description: "Unique identifier for each activity record",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Id.Time"),
			},
			{
				Name:        "unique_qualifier",
				Description: "Unique identifier for each activity record",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Id.UniqueQualifier"),
			},
			{
				Name:        "profile_id",
				Description: "The Profile id of actor",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Actor.ProfileId"),
			},
			{
				Name:        "email",
				Description: "An email of actor",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Actor.Email"),
			},
			{
				Name:        "caller_type",
				Description: "A caller type of actor",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Actor.CallerType"),
			},
			{
				Name:        "event_name",
				Description: "The name of the event being queried by the API",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromQual("event_name"),
			},
			{
				Name:        "filters",
				Description: "A query string to filter for specific eventName",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromQual("filters"),
			},
			{
				Name:        "org_unit_id",
				Description: "ID of the organizational unit to report on",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromQual("org_unit_id"),
			},
			{
				Name:        "group_id_filter",
				Description: "Group ids on which user activities are filtered",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromQual("group_id_filter"),
			},
		},
	}
}

//// LIST FUNCTION

func listAdminReportsActivities(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	// Create service
	service, err := AdminReportsService(ctx, d)
	if err != nil {
		return nil, err
	}

	var applicationName string
	if d.KeyColumnQuals["application_name"] != nil {
		applicationName = d.KeyColumnQuals["application_name"].GetStringValue()
	} else {
		return nil, nil
	}

	var user_key string
	if d.KeyColumnQuals["user_key"] != nil {
		user_key = d.KeyColumnQuals["user_key"].GetStringValue()
	} else {
		user_key = "all"
	}

	// Setting the maximum number of messages, API can return in a single page
	maxResults := int64(1000)

	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil {
		if *limit < maxResults {
			maxResults = *limit
		}
	}
	resp := service.Activities.List(user_key, applicationName).MaxResults(maxResults)

	var actor_ip_address string
	if d.KeyColumnQuals["actor_ip_address"] != nil {
		actor_ip_address = d.KeyColumnQuals["actor_ip_address"].GetStringValue()
		resp = resp.ActorIpAddress(actor_ip_address)
	}

	var customer_id string
	if d.KeyColumnQuals["customer_id"] != nil {
		customer_id = d.KeyColumnQuals["customer_id"].GetStringValue()
		resp = resp.CustomerId(customer_id)
	}

	if d.Quals["time"] != nil {
		for _, q := range d.Quals["time"].Quals {
			givenTime, err := time.Parse("2006-01-02T15:04:05.000Z", q.Value.GetStringValue())
			if err != nil {
				return nil, err
			}
			beforeTime := givenTime.Add(time.Duration(-1) * time.Second).Format("2006-01-02T15:04:05.000Z")
			afterTime := givenTime.Add(time.Second * 1).Format("2006-01-02T15:04:05.000Z")

			switch q.Operator {
			case ">":
				resp.StartTime(afterTime)
			case ">=":
				resp.StartTime(givenTime.Format("2006-01-02T15:04:05.000Z"))
			case "=":
				resp.StartTime(givenTime.Format("2006-01-02T15:04:05.000Z")).EndTime(givenTime.Format("2006-01-02T15:04:05.000Z"))
			case "<=":
				resp.EndTime(givenTime.Format("2006-01-02T15:04:05.000Z"))
			case "<":
				resp.EndTime(beforeTime)
			}
		}
	} else {
		resp.StartTime(time.Now().Add(time.Duration(-24) * time.Hour).Format("2006-01-02T15:04:05.000Z"))
	}

	var event_name string
	if d.KeyColumnQuals["event_name"] != nil {
		event_name = d.KeyColumnQuals["event_name"].GetStringValue()
		resp = resp.EventName(event_name)
	}

	var filters string
	if d.KeyColumnQuals["filters"] != nil {
		filters = d.KeyColumnQuals["filters"].GetStringValue()
		resp = resp.Filters(filters)
	}

	var org_unit_id string
	if d.KeyColumnQuals["org_unit_id"] != nil {
		org_unit_id = d.KeyColumnQuals["org_unit_id"].GetStringValue()
		resp = resp.OrgUnitID(org_unit_id)
	}

	var group_id_filter string
	if d.KeyColumnQuals["group_id_filter"] != nil {
		group_id_filter = d.KeyColumnQuals["group_id_filter"].GetStringValue()
		resp = resp.GroupIdFilter(group_id_filter)
	}

	// DBG START

	// r, err := service.Activities.List("all", "drive").
	// 	MaxResults(10).
	// 	StartTime("2022-09-25T08:26:32.245Z").
	// 	EndTime("2022-09-26T08:26:32.245Z").
	// 	Do()
	// if err != nil {
	// 	log.Fatalf("Unable to retrieve logins to domain. %v", err)
	// }

	// DBG END

	fmt.Println(actor_ip_address, customer_id, event_name, filters, org_unit_id, group_id_filter)

	if err := resp.Pages(ctx, func(page *admin.Activities) error {
		for _, item := range page.Items {
			d.StreamListItem(ctx, item)

			// //DBG
			// plugin.Logger(ctx).Error("@DBG1:")
			// res2B, _ := json.Marshal(item)
			// plugin.Logger(ctx).Error(string(res2B))
			// plugin.Logger(ctx).Error("@DBG1-9:")
			// // END

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
