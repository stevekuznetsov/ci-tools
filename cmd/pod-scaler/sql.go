package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/common/model"

	prowv1 "k8s.io/test-infra/prow/apis/prowjobs/v1"
	"k8s.io/test-infra/prow/interrupts"

	"github.com/openshift/ci-tools/pkg/api"
	"github.com/openshift/ci-tools/pkg/steps"
)

type poolWithCache struct {
	*pgxpool.Pool
	// columnTypesByTable caches the data types for columns in our tables
	columnTypesByTable map[string]map[string]string
}

// rowQuerier knows how to query rows in Postgres
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

// identifier knows how to identify objects in our schema
type identifier struct {
	delegate           rowQuerier
	columnTypesByTable map[string]map[string]string
}

// identifierQueryMetadata fully qualifies a query for an identifier in our schema
type identifierQueryMetadata struct {
	table   string
	columns []columnValue
}

// columnValue is a mapping from column to value - we don't use a map as order is important
type columnValue struct {
	column string
	value  interface{}
}

// upsertReturningId creates a query for cases where we want to insert a new row or use a previous one,
// in an atomic manner. Postgres does not allow `INSERT ... ON CONFLICT DO NOTHING RETURNING ...` to work,
// so we need this monstrosity in order to be performant, as we expect most of these calls to be duplicates
// of what's already in the DB: https://stackoverflow.com/a/42217872
func (i *identifier) upsertReturningId(into interface{}, metadata *identifierQueryMetadata) error {
	var columnNames []string
	var parameters []string
	var values []interface{}
	for j, column := range metadata.columns {
		columnNames = append(columnNames, column.column)
		parameters = append(parameters, fmt.Sprintf("$%d::%s", j+1, i.columnTypesByTable[metadata.table][column.column]))
		values = append(values, column.value)
	}
	delimitedColumns := strings.Join(columnNames, ", ")
	delimitedParameters := strings.Join(parameters, ", ")
	query := fmt.Sprintf(`WITH input_rows(%[1]s) AS (
    (SELECT %[1]s FROM podscaler.%[2]s LIMIT 0)	
    UNION ALL
    VALUES (%[3]s)
), inserted_rows AS (INSERT INTO podscaler.%[2]s(%[1]s)
    SELECT * FROM input_rows
    ON CONFLICT DO NOTHING
    RETURNING id
)
SELECT id FROM inserted_rows
UNION ALL
SELECT c.id FROM input_rows JOIN podscaler.%[2]s c USING(%[1]s)`, delimitedColumns, metadata.table, delimitedParameters)
	return i.delegate.QueryRow(interrupts.Context(), query, values...).Scan(into)
}

func (i *identifier) forQuery(cluster, query string) (int64, error) {
	var id int64
	err := i.upsertReturningId(&id, infoForQuery(cluster, query))
	return id, err
}

func infoForQuery(cluster, query string) *identifierQueryMetadata {
	return &identifierQueryMetadata{
		table:   "queries",
		columns: []columnValue{{column: "cluster", value: cluster}, {column: "query", value: query}},
	}
}

func (i *identifier) forMetric(metric model.Metric) (string, error) {
	if fromConfiguration(metric) {
		var confId int
		conf, info := infoForMetric(metric)
		if conf != nil && info != nil {
			if err := i.upsertReturningId(&confId, conf); err != nil {
				return "", fmt.Errorf("could not retrieve identifier for config: %w", err)
			}
			var id string
			err := i.upsertReturningId(&id, info(confId))
			return id, err
		}
	} else if info := simpleInfoForMetric(metric); info != nil {
		var id string
		err := i.upsertReturningId(&id, info)
		return id, err
	}
	return "", fmt.Errorf("could not determine identifier for metric: %v", metric)
}

// fromConfiguration determines if a metric is for a container created from ci-operator configuration
func fromConfiguration(metric model.Metric) bool {
	return oneOf(metric, LabelNameOrg, ProwLabelNameOrg) != ""
}

// simpleInfoForMetric returns metadata for a single query needed to identify this metric
func simpleInfoForMetric(metric model.Metric) *identifierQueryMetadata {
	if _, prowJob := metric[ProwLabelNameCreated]; !prowJob {
		// this should not happen, but if it does, we can't ingest this metric
		// (we know of no other containers that have no org/repo association but raw periodics)
		return nil
	}
	job, jobLabeled := metric[ProwLabelNameJob]
	if !jobLabeled {
		// this should not happen, but if it does, we can't ingest this metric
		// (Prow should always label this)
		return nil
	}
	jobType, typeLabeled := metric[ProwLabelNameType]
	if !typeLabeled {
		// this should not happen, but if it does, we can't ingest this metric
		// (Prow should always label this)
		return nil
	}
	if prowv1.ProwJobType(jobType) != prowv1.PeriodicJob {
		// this should not happen, but if it does, we can't ingest this metric
		// (only periodic jobs should exist without org/repo association)
		return nil
	}
	return &identifierQueryMetadata{
		table: "raw_prowjobs",
		columns: []columnValue{
			{column: "job", value: string(job)},
			{column: "container", value: string(metric[LabelNameContainer])},
		},
	}
}

// infoForMetric returns the query metadata necessary for a two-tier identification of a metric
func infoForMetric(metric model.Metric) (*identifierQueryMetadata, func(confId int) *identifierQueryMetadata) {
	metadata := api.Metadata{
		Org:     oneOf(metric, LabelNameOrg, ProwLabelNameOrg),
		Repo:    oneOf(metric, LabelNameRepo, ProwLabelNameRepo),
		Branch:  oneOf(metric, LabelNameBranch, ProwLabelNameBranch),
		Variant: string(metric[LabelNameVariant]),
	}
	pod, container := string(metric[LabelNamePod]), string(metric[LabelNameContainer])
	conf := &identifierQueryMetadata{
		table: "conf",
		columns: []columnValue{
			{column: "org", value: metadata.Org},
			{column: "repo", value: metadata.Repo},
			{column: "branch", value: metadata.Branch},
			{column: "variant", value: metadata.Variant},
		},
	}
	app, set := metric[LabelNameApp]
	if build, buildPod := metric[LabelNameBuild]; buildPod {
		return conf, func(confId int) *identifierQueryMetadata {
			return &identifierQueryMetadata{
				table: "builds",
				columns: []columnValue{
					{column: "conf_id", value: confId},
					{column: "build", value: string(build)},
					{column: "container", value: container},
				},
			}
		}
	} else if _, releasePod := metric[LabelNameRelease]; releasePod {
		return conf, func(confId int) *identifierQueryMetadata {
			return &identifierQueryMetadata{
				table: "releases",
				columns: []columnValue{
					{column: "conf_id", value: confId},
					{column: "pod", value: pod},
					{column: "container", value: container},
				},
			}
		}
	} else if rpmRepoPod := set && app == model.LabelValue(steps.RPMRepoName); rpmRepoPod {
		return conf, func(confId int) *identifierQueryMetadata {
			return &identifierQueryMetadata{
				table: "rpms",
				columns: []columnValue{
					{column: "conf_id", value: confId},
				},
			}
		}
	} else if step, stepJob := metric[LabelNameStep]; stepJob {
		return conf, func(confId int) *identifierQueryMetadata {
			return &identifierQueryMetadata{
				table: "steps",
				columns: []columnValue{
					{column: "conf_id", value: confId},
					{column: "target", value: string(metric[LabelNameTarget])},
					{column: "step", value: string(step)},
					{column: "container", value: container},
				},
			}
		}
	} else if _, prowJob := metric[ProwLabelNameCreated]; prowJob {
		prowContext := string(metric[ProwLabelNameContext])
		if prowContext == "" {
			// periodic and postsubmit jobs do not have a context, but we can try to
			// extract a useful name for the job by processing the full name, with the
			// caveat that labels have a finite length limit and the most specific data
			// is in the suffix of the job name, so we will alias jobs here whose names
			// are too long
			prowContext = syntheticContextFromJob(metadata, metric)
		}
		if prowContext == "" {
			// this should not happen, but if it does we can't ingest this metric
			return nil, nil
		}
		return conf, func(confId int) *identifierQueryMetadata {
			return &identifierQueryMetadata{
				table: "prowjobs",
				columns: []columnValue{
					{column: "conf_id", value: confId},
					{column: "context", value: prowContext},
					{column: "container", value: container},
				},
			}
		}
	} else {
		// this should not happen, but if it does we can't ingest this metric
		return nil, nil
	}
}
